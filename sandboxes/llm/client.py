"""One client for every LLM call, over OpenRouter or any OpenAI-compatible server (vLLM, SGLang), that records each call in
the active LLM session (session.py).

    cfg = load_llm_config("sandboxes:openrouter")
    client = LLMClient(cfg)                                  # records into current_llm_session(), or pass session=
    r = client.chat(messages, model="openai/gpt-6-luna", tools=tools, reasoning_effort="medium",
                    tags={"stage": "evolve", "task_id": "t-0034"}, trajectory={"id": "u1x3-review2", "kind": "fidelity"})
    r.message, r.usage, r.usd, r.record

The provider is the one whose `models` glob matching the model is the most specific (`local/*` over `*`). Per type:

    openrouter         `provider` routing and `reasoning: {effort}` are sent; the upstream provider, the billed cost
                       (usage.cost) and reasoning (`reasoning`, `reasoning_details`) are read from the answer; prices come
                       from OpenRouter (pricing.py), and a share of calls fetch OpenRouter's generation record afterwards
    openai_compatible  `reasoning_effort` only where the provider declares the capability, `extra_body` as given;
                       reasoning is read from `reasoning_content`; cost from the config's static rates, or none

A request that is rate limited, times out, gets a 5xx, an OpenRouter in-body error or an upstream hiccup reported as
"Provider returned error" is retried with exponential backoff (`retries` attempts); each retry prints one stderr line. A
call that fails for good is recorded with its error, then raised as LLMRequestError.
"""

from __future__ import annotations

import json
import os
import random
import re
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Union

from .config import LLMConfig, LLMProviderConfig
from .pricing import OpenRouterPrices, Price, get_openrouter_prices, get_static_price
from .session import (LLMSession, current_llm_session, get_chain, get_message_chars, get_source, get_text, get_tool_names)

READ_TIMEOUT_S = 300.0
RETRYABLE_STATUS = {408, 429}
BACKOFF_INITIAL_S, BACKOFF_MAX_S = 2.0, 60.0
GENERATION_DELAYS_S = (2.0, 6.0, 20.0)       # OpenRouter's generation record appears a few seconds after the answer
USAGE_FIELDS = {"prompt_tokens", "completion_tokens", "total_tokens", "prompt_tokens_details", "completion_tokens_details",
                "input_tokens", "output_tokens", "input_tokens_details", "output_tokens_details", "cost"}


class LLMError(Exception):
    """A call that could not be made or answered."""


class LLMRequestError(LLMError):
    """The provider refused or failed the request (after retries, when `retryable`)."""

    def __init__(self, message: str, *, status: Optional[int] = None, body: str = "", retryable: bool = False,
                 attempts: int = 1):
        super().__init__(message)
        self.status, self.body, self.retryable, self.attempts = status, body, retryable, attempts


class _Retry(Exception):
    def __init__(self, reason: str, status: Optional[int], body: str):
        super().__init__(f"{reason}: {body[:500]}")
        self.reason, self.status, self.body = reason, status, body


@dataclass
class ChatResult:
    message: dict
    usage: dict            # prompt_tokens, completion_tokens, cached_tokens, cache_write_tokens, reasoning_tokens (0 if unreported)
    usd: float
    raw: dict
    record: dict = field(default_factory=dict)


def is_transient_provider_error(status: int, text: str) -> bool:
    """OpenRouter wraps upstream hiccups as HTTP 400/404/502/503 "Provider returned error" (a provider answering
    model_not_found for a model it serves, an overloaded upstream): retried like a 5xx."""
    if status not in (400, 404, 502, 503):
        return False
    t = (text or "")[:2000]
    return "Provider returned error" in t and ("model_not_found" in t or "The requested model was not found" in t
                                              or "overloaded" in t.lower() or "temporarily" in t.lower())


def load_env_file(path: Optional[str]) -> None:
    """KEY=VALUE lines ('#' comments) into the environment, never overriding a variable already set."""
    if not path or not Path(path).is_file():
        return
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


class KeyPool:
    """A provider's API keys, used round-robin: the first of its key env vars that is set (comma or space separated), else
    its keys_file. An openai_compatible server without keys gets "EMPTY"."""

    def __init__(self, provider: LLMProviderConfig):
        self.provider = provider
        self.keys: Optional[list[str]] = None
        self.index = 0
        self.lock = threading.Lock()

    def load(self) -> list[str]:
        load_env_file(self.provider.env_file)
        for env in self.provider.get_key_envs():
            keys = [k for k in re.split(r"[\s,]+", os.environ.get(env, "").strip()) if k]
            if keys:
                return keys
        if self.provider.keys_file and Path(self.provider.keys_file).is_file():
            keys = [ln.strip() for ln in Path(self.provider.keys_file).read_text().splitlines()
                    if ln.strip() and not ln.strip().startswith("#")]
            if keys:
                return keys
        if self.provider.provider_type == "openai_compatible":
            return ["EMPTY"]
        raise LLMError(f"llm provider {self.provider.name}: no API key (set one of {self.provider.get_key_envs()} or "
                       "keys_file)")

    def next(self) -> tuple[str, int]:
        with self.lock:
            if self.keys is None:
                self.keys = self.load()
            i = self.index % len(self.keys)
            self.index += 1
            return self.keys[i], i


def get_tokens(usage: dict) -> tuple[dict, list[str]]:
    """Token counts from a usage object (chat or responses style); None where the provider did not report one."""
    prompt_details = usage.get("prompt_tokens_details") or usage.get("input_tokens_details") or {}
    completion_details = usage.get("completion_tokens_details") or usage.get("output_tokens_details") or {}
    raw = {"prompt": usage.get("prompt_tokens", usage.get("input_tokens")),
           "completion": usage.get("completion_tokens", usage.get("output_tokens")),
           "cached": prompt_details.get("cached_tokens"),
           "cache_write": prompt_details.get("cache_write_tokens", prompt_details.get("cache_creation_tokens")),
           "reasoning": completion_details.get("reasoning_tokens")}
    reported = [k for k, v in raw.items() if v is not None]
    tokens = {k: (int(v) if v is not None else None) for k, v in raw.items()}
    prompt = tokens["prompt"] or 0
    if tokens["cached"] is not None:
        tokens["cached"] = max(0, min(tokens["cached"], prompt))     # a stray value never exceeds the prompt
    tokens["uncached"] = max(0, prompt - (tokens["cached"] or 0) - (tokens["cache_write"] or 0))
    tokens["total"] = prompt + (tokens["completion"] or 0)
    return tokens, reported


def get_billed_usd(usage: dict) -> Optional[float]:
    cost = usage.get("cost")
    return float(cost) if isinstance(cost, (int, float)) else None


def get_result_usage(tokens: dict) -> dict:
    """A ChatResult's usage: every count an int (0 where the provider did not report it)."""
    return {"prompt_tokens": tokens.get("prompt") or 0, "completion_tokens": tokens.get("completion") or 0,
            "cached_tokens": tokens.get("cached") or 0, "cache_write_tokens": tokens.get("cache_write") or 0,
            "reasoning_tokens": tokens.get("reasoning") or 0}


def get_plain_result(raw: dict, message: dict) -> ChatResult:
    """A call's result without its record (what a call returns when recording it failed): its usage and billed cost."""
    usage = raw.get("usage") if isinstance(raw.get("usage"), dict) else {}
    try:
        tokens, _ = get_tokens(usage)
    except (TypeError, ValueError):
        tokens = {}
    return ChatResult(message=message, usage=get_result_usage(tokens), usd=get_billed_usd(usage) or 0.0, raw=raw)


def get_backoff_s(attempt: int) -> float:
    return min(BACKOFF_INITIAL_S * 2 ** (attempt - 1) + random.uniform(0, 1), BACKOFF_MAX_S)


class LLMClient:
    """Calls the providers of `config`, recording each call in `session` (default: the active session, if any).
    `transport` replaces the HTTP transport (an httpx.MockTransport in tests), `prices` the OpenRouter price cache (one
    per base URL is shared in the process by default), `sleep` the backoff wait."""

    def __init__(self, config: LLMConfig, *, session: Optional[LLMSession] = None, transport=None,
                 max_connections: int = 100, max_keepalive: int = 20, sleep: Callable[[float], None] = time.sleep,
                 log_retries: bool = True, prices: Optional[OpenRouterPrices] = None):
        import httpx
        self.httpx = httpx
        self.config = config
        self.session = session
        self.prices = prices
        self.sleep = sleep
        self.log_retries = log_retries
        self.http = httpx.Client(transport=transport, limits=httpx.Limits(max_connections=max_connections,
                                                                          max_keepalive_connections=max_keepalive))
        self.keys: dict[str, KeyPool] = {}
        self.keys_lock = threading.Lock()
        self.warned: set[tuple[str, str]] = set()
        self.recorded_config = False
        self.recording_errors: dict[str, int] = {}      # what failed to record -> how often (never a call's failure)
        self.recording_lock = threading.Lock()

    # -- plumbing ---------------------------------------------------------------------------------------------------------
    def record_safely(self, what: str, fn: Callable, *args, default=None, **kwargs):
        """fn(*args, **kwargs) for the session's records. Recording never fails a call: an error (a full disk, no file
        descriptors left, an answer of an unexpected shape) is counted in `recording_errors`, printed once per `what`,
        and `default` returned, so a call is never lost, raised over or sent again because of its record."""
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001 — see the docstring
            with self.recording_lock:
                self.recording_errors[what] = self.recording_errors.get(what, 0) + 1
                first = self.recording_errors[what] == 1
            if first:
                print(f"[sandboxes.llm] recording {what} failed (the call itself is unaffected): {type(e).__name__}: "
                      f"{str(e)[:200]}", file=sys.stderr, flush=True)
            return default

    def get_session(self) -> Optional[LLMSession]:
        s = self.session or current_llm_session()
        if s is None:
            return None
        with self.recording_lock:
            first, self.recorded_config = not self.recorded_config, True
        if first:
            self.record_safely("session config", s.write_meta,
                               {"llm_providers": [p.to_public() for p in self.config.llm_providers],
                                "recording": self.config.recording.model_dump()})
        return s

    def get_keys(self, provider: LLMProviderConfig) -> KeyPool:
        with self.keys_lock:
            if provider.name not in self.keys:
                self.keys[provider.name] = KeyPool(provider)
            return self.keys[provider.name]

    def warn_dropped(self, provider: LLMProviderConfig, params: list[str]) -> None:
        for p in params:
            if (provider.name, p) not in self.warned:
                self.warned.add((provider.name, p))
                print(f"[sandboxes.llm] {provider.name} ({provider.provider_type}) does not take `{p}`: not sent",
                      file=sys.stderr, flush=True)

    def post_once(self, provider: LLMProviderConfig, url: str, payload: dict, key: str, timeout_s: float) -> dict:
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json", **provider.headers}
        deadline = time.monotonic() + timeout_s
        chunks: list[bytes] = []
        timeout = self.httpx.Timeout(min(READ_TIMEOUT_S, timeout_s), pool=None)
        with self.http.stream("POST", url, json=payload, headers=headers, timeout=timeout) as resp:
            status = resp.status_code
            for chunk in resp.iter_bytes():
                chunks.append(chunk)
                if time.monotonic() > deadline:
                    raise _Retry("timeout", 408, f"request exceeded the {timeout_s:.0f}s total deadline "
                                                 f"({sum(map(len, chunks))} bytes received)")
        text = b"".join(chunks).decode("utf-8", errors="replace")
        if status in RETRYABLE_STATUS or status >= 500 or is_transient_provider_error(status, text):
            raise _Retry(str(status), status, text)
        if status >= 400:
            raise LLMRequestError(f"{provider.name} request to {url} failed: HTTP {status}: {text[:500]}", status=status,
                                  body=text)
        try:
            data = json.loads(text)
        except ValueError:
            raise _Retry("non-json", 502, f"non-JSON HTTP {status} body: {text.strip()[:200]!r}")
        if isinstance(data, dict) and isinstance(data.get("error"), dict):    # OpenRouter: HTTP 200 with an error object
            err = data["error"]
            try:
                code = int(err.get("code"))
            except (TypeError, ValueError):
                code = 0
            body = str(err.get("message") or err)
            if code in RETRYABLE_STATUS or code >= 500 or is_transient_provider_error(code, text):
                raise _Retry(str(code or "in-body"), code, body)
            raise LLMRequestError(f"{provider.name} request to {url} failed: {code or 400}: {body[:500]}", status=code or 400,
                                  body=body)
        return data

    def send(self, provider: LLMProviderConfig, url: str, payload: dict, timeout_s: float) -> tuple[dict, dict]:
        """The answer, and how it went: {"attempts", "retry_reasons", "key_index"}. Raises LLMRequestError (with
        `attempts` and `retry_reasons` on it) when the call fails for good."""
        reasons: list[str] = []
        pool = self.get_keys(provider)
        for attempt in range(1, max(1, provider.retries) + 1):
            key, index = pool.next()
            try:
                data = self.post_once(provider, url, payload, key, timeout_s)
                return data, {"attempts": attempt, "retry_reasons": reasons, "key_index": index}
            except (_Retry, self.httpx.TransportError) as e:
                reason = e.reason if isinstance(e, _Retry) else type(e).__name__
                reasons.append(reason)
                if attempt >= max(1, provider.retries):
                    err = LLMRequestError(f"{provider.name} request to {url} failed after {attempt} attempts: {e}",
                                          status=getattr(e, "status", None), body=getattr(e, "body", str(e)),
                                          retryable=True, attempts=attempt)
                    err.retry_reasons = reasons
                    raise err from e
                if self.log_retries:
                    print(f"[{time.strftime('%H:%M:%S')}]      ! LLM request retry {attempt}: {type(e).__name__}: "
                          f"{str(e)[:200]}", file=sys.stderr, flush=True)
                self.sleep(get_backoff_s(attempt))
            except LLMRequestError as e:
                e.attempts, e.retry_reasons = attempt, reasons
                raise
        raise AssertionError("unreachable")

    # -- chat -------------------------------------------------------------------------------------------------------------
    def build_chat_payload(self, provider: LLMProviderConfig, model: str, messages: list[dict], *, tools, tool_choice,
                           response_format, reasoning_effort, max_tokens, temperature, routing, extra_body) -> tuple[dict, dict]:
        """The request body for `provider`, and the params it records (with what the provider could not take)."""
        payload: dict = {"model": provider.model_map.get(model, model), "messages": messages}
        dropped = []
        if tools is not None:
            if provider.get_capability("tools"):
                payload["tools"] = tools
                if tool_choice is not None:
                    payload["tool_choice"] = tool_choice
            else:
                dropped.append("tools")
        if response_format is not None:
            if response_format.get("type") != "json_schema" or provider.get_capability("json_schema"):
                payload["response_format"] = response_format
            else:
                dropped.append("response_format")
        if reasoning_effort is not None:
            if provider.provider_type == "openrouter":
                payload["reasoning"] = {"effort": reasoning_effort}
            elif provider.get_capability("reasoning_effort"):
                payload["reasoning_effort"] = reasoning_effort
            else:
                dropped.append("reasoning_effort")
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if temperature is not None:
            payload["temperature"] = temperature
        route = {**provider.routing.model_dump(), **(routing or {})}
        if provider.provider_type == "openrouter" and route.get("order"):
            payload["provider"] = {"order": list(route["order"]), "allow_fallbacks": bool(route.get("allow_fallbacks", True))}
        elif routing and routing.get("order"):
            dropped.append("routing")
        payload.update(provider.extra_body)
        payload.update(extra_body or {})
        self.warn_dropped(provider, dropped)
        choice = tool_choice if isinstance(tool_choice, str) or tool_choice is None else \
            (tool_choice.get("function") or {}).get("name", "function")
        rf = None if response_format is None else ((response_format.get("json_schema") or {}).get("name")
                                                   or response_format.get("type"))
        params = {"max_tokens": max_tokens, "temperature": temperature, "reasoning_effort": reasoning_effort,
                  "tool_choice": choice, "response_format": rf, "routing": payload.get("provider"),
                  "extra_body_keys": sorted(set(provider.extra_body) | set(extra_body or {}))}
        if dropped:
            params["dropped"] = dropped
        return payload, {k: v for k, v in params.items() if v not in (None, [], {})}

    def chat(self, messages: list[dict], *, model: str, tools: Optional[list] = None,
             tool_choice: Union[dict, str, None] = None, response_format: Optional[dict] = None,
             reasoning_effort: Optional[str] = None, max_tokens: Optional[int] = None, temperature: Optional[float] = None,
             routing: Optional[dict] = None, timeout_s: Optional[float] = None, tags: Optional[dict] = None,
             trajectory: Optional[dict] = None, extra_body: Optional[dict] = None) -> ChatResult:
        """One chat completion, recorded. `routing` ({order, allow_fallbacks}) and `timeout_s` override the provider's
        for this call; `trajectory` is {"id", "parent", "kind"} (its step is counted by the session)."""
        provider = self.config.get_provider(model)
        payload, params = self.build_chat_payload(provider, model, messages, tools=tools, tool_choice=tool_choice,
                                                  response_format=response_format, reasoning_effort=reasoning_effort,
                                                  max_tokens=max_tokens, temperature=temperature, routing=routing,
                                                  extra_body=extra_body)
        session = self.get_session()
        content = self.config.recording.content
        call_id, t_start = uuid.uuid4().hex[:16], time.time()
        trajectory = dict(trajectory or {})
        hashes, links, tools_hash = [], {}, None
        if session is not None:
            hashes = self.record_safely("messages", session.add_messages, messages, call_id=call_id, t=t_start,
                                        content=content, default=[])
            links = self.record_safely("links", session.get_links, hashes, trajectory.get("id"), default={})
            tools_hash = self.record_safely("tools", session.add_tools, payload.get("tools"), content=content)
        if trajectory:
            trajectory["step"] = links.get("step", 0)
        new_from = links.get("new_from", 0)
        record = {"v": 1, "call_id": call_id, "kind": "chat", "pid": os.getpid(), "t_start": round(t_start, 3),
                  "tags": dict(tags or {}), "trajectory": trajectory or None,
                  "provider": {"name": provider.name, "type": provider.provider_type,
                               "host": self.httpx.URL(provider.base_url).host},
                  "model_requested": model, "model_sent": payload["model"], "params": params, "tools": tools_hash,
                  "n_tools": len(payload.get("tools") or []),
                  "chars": self.record_safely("characters", self.get_prompt_chars, messages, new_from, default={}),
                  "n_messages": len(messages), "n_new_messages": len(messages) - new_from}
        try:
            raw, sent = self.send(provider, provider.base_url.rstrip("/") + "/chat/completions", payload,
                                  timeout_s or provider.timeout_s)
            try:
                message = raw["choices"][0]["message"]
            except (KeyError, IndexError, TypeError) as e:
                raise LLMRequestError(f"malformed chat completion from {provider.name}: {raw!r:.500}", status=200,
                                      body=json.dumps(raw)[:2000], attempts=sent["attempts"]) from e
        except LLMRequestError as e:
            if session is not None:
                self.record_safely("failed call", self.record_failure, session, record, links, hashes, e)
            raise
        result = self.record_safely("call", self.record_chat, session, provider, record, links, hashes, raw, message,
                                    sent, content)
        return result if result is not None else get_plain_result(raw, message)

    @staticmethod
    def get_prompt_chars(messages: list[dict], new_from: int) -> dict:
        names = get_tool_names(messages)
        by_role: dict[str, int] = {}
        new_by_source: dict[str, int] = {}
        for i, m in enumerate(messages):
            n = get_message_chars(m)
            by_role[str(m.get("role"))] = by_role.get(str(m.get("role")), 0) + n
            if i >= new_from:
                source = get_source(m, names)
                new_by_source[source] = new_by_source.get(source, 0) + n
        return {"prompt": sum(by_role.values()), "prompt_new": sum(new_by_source.values()), "by_role": by_role,
                "new_by_source": new_by_source}

    def record_failure(self, session, record: dict, links: dict, hashes: list, e: LLMRequestError) -> None:
        record.update({"t_end": round(time.time(), 3), "status": "error", "attempts": e.attempts,
                       "retry_reasons": getattr(e, "retry_reasons", []),
                       "error": {"kind": "http" if e.status else "transport", "http_status": e.status,
                                 "message": str(e)[:1000]},
                       "path": {**links, "prompt": hashes}})
        record["latency_s"] = round(record["t_end"] - record["t_start"], 3)
        session.add_call(record)

    def get_price(self, provider: LLMProviderConfig, model: str, upstream: str, tokens: dict,
                  billed: Optional[float]) -> tuple[Optional[Price], str]:
        if provider.pricing.source == "static":
            return get_static_price(model, provider.name, provider.pricing), "static"
        if provider.pricing.source == "openrouter" and provider.provider_type == "openrouter":
            prices = self.prices or get_openrouter_prices(provider.base_url.rstrip("/"), provider.pricing.refresh_s)
            return prices.choose(model, upstream, tokens, billed)
        return None, "none"

    def record_chat(self, session, provider: LLMProviderConfig, record: dict, links: dict, hashes: list, raw: dict,
                    message: dict, sent: dict, content: bool) -> ChatResult:
        t_end = time.time()
        usage = raw.get("usage") or {}
        tokens, reported = get_tokens(usage)
        billed = get_billed_usd(usage)
        upstream = raw.get("provider") if provider.provider_type == "openrouter" else None
        price, match = self.get_price(provider, record["model_sent"], upstream or "", tokens, billed)
        list_usd = round(price.usd(tokens), 8) if price else None
        if billed is not None:
            usd, source = billed, "api"
        elif price is not None:
            usd, source = list_usd, "static" if price.source == "static" else "list"
        else:
            usd, source = 0.0, "none"
        reasoning = message.get("reasoning") or message.get("reasoning_content")
        tool_calls = message.get("tool_calls") or []
        choice = (raw.get("choices") or [{}])[0]
        record.update({
            "t_end": round(t_end, 3), "latency_s": round(t_end - record["t_start"], 3), "status": "ok", **sent,
            "upstream": upstream, "model_served": raw.get("model"), "generation_id": raw.get("id"),
            "finish_reason": choice.get("finish_reason"), "native_finish_reason": choice.get("native_finish_reason"),
            "tool_calls": [(c.get("function") or {}).get("name") for c in tool_calls],
            "tokens": tokens, "tokens_reported": reported, "usd": usd, "usd_source": source, "list_usd": list_usd,
            "price": price.to_record(tokens, now=t_end, max_age_s=provider.pricing.max_age_s, match=match) if price else None,
            "usage_extra": {k: v for k, v in usage.items() if k not in USAGE_FIELDS} or None,
        })
        record["chars"].update({"content": len(get_text(message.get("content"))), "reasoning": len(get_text(reasoning)),
                                "tool_args": sum(len(str((c.get("function") or {}).get("arguments") or "")) for c in tool_calls)})
        cost_details = usage.get("cost_details") or {}
        if cost_details.get("upstream_inference_cost") is not None:
            record["upstream_usd"] = cost_details["upstream_inference_cost"]
        if session is not None:
            self.record_safely("call", self.write_chat_record, session, provider, record, links, hashes, message, price,
                               content)
        return ChatResult(message=message, usage=get_result_usage(tokens), usd=usd, raw=raw, record=record)

    def write_chat_record(self, session: LLMSession, provider: LLMProviderConfig, record: dict, links: dict,
                          hashes: list, message: dict, price: Optional[Price], content: bool) -> None:
        response = session.add_response(message, call_id=record["call_id"], t=record["t_end"], content=content)
        record["path"] = {**links, "prompt": hashes, "response": response,
                          "chain": get_chain(links.get("prefix", ""), response)}
        if price is not None:
            session.add_price(price.to_history())
        session.add_call(record)
        if provider.provider_type == "openrouter" and record.get("generation_id") \
                and random.random() < provider.pricing.verify_generations:
            threading.Thread(target=self.verify_generation, args=(session, provider, record["call_id"],
                                                                  record["generation_id"]), daemon=True).start()

    def verify_generation(self, session: LLMSession, provider: LLMProviderConfig, call_id: str, generation_id: str) -> None:
        """OpenRouter's own record of a generation (exact provider, native token counts, total cost), into
        generations.jsonl; given up quietly after a few tries."""
        url = provider.base_url.rstrip("/") + "/generation"
        for delay in GENERATION_DELAYS_S:
            time.sleep(delay)
            try:
                key, _ = self.get_keys(provider).next()
                r = self.http.get(url, params={"id": generation_id}, headers={"Authorization": f"Bearer {key}"}, timeout=30)
                if r.status_code == 200:
                    session.add_generation({"t": round(time.time(), 3), "call_id": call_id, "generation_id": generation_id,
                                            "data": r.json().get("data")})
                    return
            except Exception:  # noqa: BLE001 — a verification that cannot be fetched is skipped, never an error
                pass

    # -- other endpoints --------------------------------------------------------------------------------------------------
    def request(self, path_or_url: str, payload: dict, *, model: str, kind: str = "request", tags: Optional[dict] = None,
                timeout_s: Optional[float] = None, input_chars: int = 0) -> dict:
        """Any other JSON endpoint of `model`'s provider (a path under its base URL, or a full URL), recorded with its
        usage and billed cost when the answer has them."""
        provider = self.config.get_provider(model)
        url = path_or_url if path_or_url.startswith("http") else provider.base_url.rstrip("/") + "/" + path_or_url.lstrip("/")
        payload = {**payload, "model": provider.model_map.get(model, model)}
        session = self.get_session()
        record = {"v": 1, "call_id": uuid.uuid4().hex[:16], "kind": kind, "pid": os.getpid(), "t_start": round(time.time(), 3),
                  "tags": dict(tags or {}), "trajectory": None,
                  "provider": {"name": provider.name, "type": provider.provider_type,
                               "host": self.httpx.URL(provider.base_url).host},
                  "model_requested": model, "model_sent": payload["model"], "chars": {"prompt": input_chars}}
        try:
            raw, sent = self.send(provider, url, payload, timeout_s or provider.timeout_s)
        except LLMRequestError as e:
            if session is not None:
                self.record_safely("failed call", self.record_failure, session, record, {}, [], e)
            raise
        if session is not None:
            self.record_safely("call", self.write_request_record, session, record, raw, sent)
        return raw

    @staticmethod
    def write_request_record(session: LLMSession, record: dict, raw, sent: dict) -> None:
        usage = raw.get("usage") if isinstance(raw, dict) else None
        tokens, reported = get_tokens(usage if isinstance(usage, dict) else {})
        billed = get_billed_usd(usage if isinstance(usage, dict) else {})
        t_end = time.time()
        record.update({"t_end": round(t_end, 3), "latency_s": round(t_end - record["t_start"], 3), "status": "ok",
                       **sent, "model_served": raw.get("model") if isinstance(raw, dict) else None,
                       "tokens": tokens, "tokens_reported": reported, "usd": billed or 0.0,
                       "usd_source": "api" if billed is not None else "none", "list_usd": None, "price": None})
        session.add_call(record)

    def embed(self, texts: list[str], *, model: str, tags: Optional[dict] = None,
              timeout_s: Optional[float] = None) -> list[list[float]]:
        """Embeddings of `texts`, in order (recorded like any request)."""
        raw = self.request("/embeddings", {"input": texts}, model=model, kind="embedding", tags=tags, timeout_s=timeout_s,
                           input_chars=sum(len(t) for t in texts))
        try:
            return [row["embedding"] for row in sorted(raw["data"], key=lambda d: d.get("index", 0))]
        except (KeyError, TypeError) as e:
            raise LLMRequestError(f"malformed embeddings response: {raw!r:.300}", status=200) from e
