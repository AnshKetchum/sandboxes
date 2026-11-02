"""A recorded trajectory as an ATIF-v1.7 document (harbor's Agent Trajectory Interchange Format), for training.

The trajectory's last successful call holds the whole conversation (its prompt and answer); each of its messages becomes a
step: system and user messages as `system` / `user` steps, an assistant message as an `agent` step with its reasoning (only
when the model returned some), its tool calls, the tool results that follow it as its observation, and the metrics of the
call that produced it (tokens, cached tokens, cost; reasoning tokens, provider, latency and the cost's source in `extra`).
An assistant message no call of this trajectory produced (context carried over from an earlier session, e.g. a resumed
review point) is marked `is_copied_context`, which ATIF says to leave out of training data. Plain dicts: no harbor import.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from .session import LLMSession, get_message_hash, get_text

SCHEMA_VERSION = "ATIF-v1.7"


def get_iso(t: Optional[float]) -> Optional[str]:
    return datetime.fromtimestamp(t, tz=timezone.utc).isoformat() if t else None


def get_arguments(arguments) -> dict:
    """A tool call's arguments as the object ATIF wants (a string that is not a JSON object is kept under `_raw`)."""
    if isinstance(arguments, dict):
        return arguments
    try:
        value = json.loads(arguments or "{}")
    except (TypeError, ValueError):
        return {"_raw": arguments}
    return value if isinstance(value, dict) else {"_raw": arguments}


def get_message_value(content):
    """ATIF's `message`: a string, or the text parts of a multimodal list (images are not recorded)."""
    if isinstance(content, list):
        parts = [{"type": "text", "text": str(p.get("text", ""))} for p in content if isinstance(p, dict) and p.get("text")]
        return parts or ""
    return content if isinstance(content, str) else get_text(content)


def get_metrics(call: dict) -> dict:
    tokens = call.get("tokens") or {}
    extra = {"call_id": call.get("call_id"), "reasoning_tokens": tokens.get("reasoning"), "upstream": call.get("upstream"),
             "usd_source": call.get("usd_source"), "list_usd": call.get("list_usd"), "latency_s": call.get("latency_s"),
             "attempts": call.get("attempts")}
    return {"prompt_tokens": tokens.get("prompt"), "completion_tokens": tokens.get("completion"),
            "cached_tokens": tokens.get("cached"), "cost_usd": call.get("usd"),
            "extra": {k: v for k, v in extra.items() if v is not None} or None}


def to_atif(session: LLMSession, trajectory_id: str, *, agent_name: str = "", agent_version: str = "") -> dict:
    """The trajectory as an ATIF-v1.7 dict (raises KeyError when it has no successful call)."""
    trajectories = session.get_trajectories()
    data = session.get_trajectory_messages(trajectory_id, trajectories)
    trajectory = trajectories[trajectory_id]
    calls = [c for c in trajectory["calls"] if c.get("status") == "ok"]
    producer = {(c.get("path") or {}).get("response"): c for c in calls}
    # A loop may edit an answer before resending it (dropping repeated tool calls, replacing unparsable arguments): such
    # a message is still the answer of the call whose prompt ended just before it.
    by_position = {len((c.get("path") or {}).get("prompt") or []): c for c in calls}
    steps: list[dict] = []
    messages = data["messages"]
    i = 0
    while i < len(messages):
        m = messages[i]
        role = m.get("role")
        if role in ("system", "user"):
            steps.append({"step_id": len(steps) + 1, "source": role, "message": get_message_value(m.get("content"))})
        elif role == "assistant":
            call = producer.get(get_message_hash(m))
            edited = call is None and by_position.get(i) is not None
            call = call or by_position.get(i)
            step = {"step_id": len(steps) + 1, "source": "agent", "message": get_message_value(m.get("content"))}
            if m.get("reasoning"):
                step["reasoning_content"] = m["reasoning"] if isinstance(m["reasoning"], str) else get_text(m["reasoning"])
            calls_made = m.get("tool_calls") or []
            if calls_made:
                step["tool_calls"] = [{"tool_call_id": c.get("id") or "", "function_name": (c.get("function") or {}).get("name", ""),
                                       "arguments": get_arguments((c.get("function") or {}).get("arguments"))}
                                      for c in calls_made]
            ids = {c.get("id") for c in calls_made}
            results = []
            while i + 1 < len(messages) and messages[i + 1].get("role") == "tool":
                i += 1
                t = messages[i]
                results.append({"source_call_id": t.get("tool_call_id") if t.get("tool_call_id") in ids else None,
                                "content": get_message_value(t.get("content"))})
            if results:
                step["observation"] = {"results": results}
            if call is not None:
                step.update({"timestamp": get_iso(call.get("t_end")), "model_name": call.get("model_served"),
                             "metrics": get_metrics(call), "llm_call_count": 1})
                if edited:      # the conversation holds the loop's edit; the answer as given keeps its reasoning
                    original = session.get_messages({(call.get("path") or {}).get("response")})
                    reasoning = next(iter(original.values()), {}).get("reasoning")
                    if reasoning and "reasoning_content" not in step:
                        step["reasoning_content"] = reasoning if isinstance(reasoning, str) else get_text(reasoning)
                    step["extra"] = {"edited_by_loop": True}
                if (call.get("params") or {}).get("reasoning_effort"):
                    step["reasoning_effort"] = call["params"]["reasoning_effort"]
            else:
                step.update({"is_copied_context": True, "llm_call_count": 0})
            steps.append(step)
        else:   # a tool result with no assistant step before it
            steps.append({"step_id": len(steps) + 1, "source": "user", "message": get_message_value(m.get("content")),
                          "extra": {"role": role, "tool_call_id": m.get("tool_call_id")}})
        i += 1
    total = lambda key: sum((c.get("tokens") or {}).get(key) or 0 for c in calls)     # noqa: E731 — a short accessor
    return {
        "schema_version": SCHEMA_VERSION, "session_id": session.id, "trajectory_id": trajectory_id,
        "agent": {"name": agent_name or session.client, "version": agent_version or str(session.tags.get("commit") or "unknown"),
                  "model_name": data.get("model"), "tool_definitions": data.get("tools") or None,
                  "extra": {"kind": trajectory.get("kind") or None, "tags": trajectory.get("tags") or None}},
        "steps": steps,
        "final_metrics": {"total_prompt_tokens": total("prompt"), "total_completion_tokens": total("completion"),
                          "total_cached_tokens": total("cached"),
                          "total_cost_usd": round(sum(c.get("usd") or 0.0 for c in calls), 8), "total_steps": len(steps),
                          "extra": {"calls": trajectory["steps"], "errors": trajectory["errors"],
                                    "retries": trajectory["branches"], "rewrites": trajectory["rewrites"],
                                    "total_reasoning_tokens": total("reasoning")}},
        "extra": {"parent_trajectory": trajectory.get("parent"), "llm_session_dir": str(session.dir)},
    }
