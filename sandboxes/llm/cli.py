"""`sandboxes llm`: LLM sessions, their cost, calls, trajectories and where their prompts grow.

    sandboxes llm sessions [--client greenhouse]                    every session: calls, tokens, $
    sandboxes llm cost REF [--by model,stage]                       calls, tokens, cache hit, $ by source, grouped
    sandboxes llm calls REF [--stage S] [--trajectory T] [--limit N] one line per call
    sandboxes llm trajectories REF [--kind K]                       one line per trajectory: steps, retries, rewrites, $
    sandboxes llm trajectory REF ID [--format openai|sharegpt|atif] [--out F] [--tree]   one trajectory rebuilt (or its
                                                                    call tree); atif: an ATIF-v1.7 document (atif.py)
    sandboxes llm growth REF [--kind K] [--stage S]                 new prompt characters by source (role or tool)
    sandboxes llm pricing MODEL [--base-url URL]                    OpenRouter's prices for MODEL, per provider endpoint

REF is a session id, name, unique id prefix or directory. Add --json for machine-readable output.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from .session import get_llm_session, list_llm_sessions


def _filter(calls: list[dict], a) -> list[dict]:
    out = calls
    if getattr(a, "stage", None):
        out = [c for c in out if (c.get("tags") or {}).get("stage") == a.stage]
    if getattr(a, "kind", None):
        out = [c for c in out if ((c.get("trajectory") or {}).get("kind") or c.get("kind")) == a.kind]
    if getattr(a, "trajectory", None):
        out = [c for c in out if (c.get("trajectory") or {}).get("id") == a.trajectory]
    return out


def _print(obj, as_json: bool, text: str) -> None:
    print(json.dumps(obj, default=str) if as_json else text)


def cmd_sessions(a) -> int:
    rows = []
    for s in list_llm_sessions(a.client):
        g = next(iter(s.summary(by=()).values()), {})
        rows.append({**s.to_dict(), "calls": int(g.get("calls", 0)), "usd": g.get("usd", 0.0),
                     "prompt_tokens": int(g.get("prompt_tokens", 0))})
    lines = [f"{'client':<14}{'session':<48}{'started (UTC)':<16}{'calls':>8}{'prompt tok':>13}{'$':>10}"]
    lines += [f"{r['client']:<14}{r['id']:<48}{time.strftime('%m-%d %H:%M', time.gmtime(r['started'])):<16}"
              f"{r['calls']:>8}{r['prompt_tokens']:>13,}{r['usd']:>10.3f}" for r in rows]
    _print(rows, a.json, "\n".join(lines) if rows else "no llm sessions")
    return 0


def cmd_cost(a) -> int:
    s = get_llm_session(a.ref)
    by = tuple(x for x in a.by.split(",") if x)
    groups = s.summary(by=by, calls=_filter(s.get_calls(), a))
    lines = [f"{'/'.join(by) or 'all':<44}{'calls':>7}{'err':>5}{'prompt tok':>13}{'cache':>7}{'compl tok':>11}"
             f"{'$':>10}{'$ api':>9}{'$ list':>9}"]
    for key, g in sorted(groups.items(), key=lambda kv: -kv[1]["usd"]):
        lines.append(f"{key[:43]:<44}{int(g['calls']):>7}{int(g['errors']):>5}{int(g['prompt_tokens']):>13,}"
                     f"{g['cache_hit']:>7.0%}{int(g['completion_tokens']):>11,}{g['usd']:>10.4f}{g.get('usd_api', 0):>9.4f}"
                     f"{g['list_usd']:>9.4f}")
    _print(groups, a.json, "\n".join(lines))
    return 0


def cmd_calls(a) -> int:
    calls = _filter(get_llm_session(a.ref).get_calls(), a)[-a.limit:]
    lines = [f"{'time':<9}{'stage':<18}{'trajectory':<22}{'step':>5} {'model':<28}{'status':<7}{'prompt':>9}{'cached':>9}"
             f"{'compl':>7}{'new chars':>10}{'$':>9}{'s':>6}"]
    for c in calls:
        tok, t = c.get("tokens") or {}, c.get("trajectory") or {}
        lines.append(f"{time.strftime('%H:%M:%S', time.gmtime(c['t_start'])):<9}{str((c.get('tags') or {}).get('stage', ''))[:17]:<18}"
                     f"{str(t.get('id', ''))[:21]:<22}{t.get('step', ''):>5} {str(c.get('model_served') or c['model_requested'])[:27]:<28}"
                     f"{c.get('status', ''):<7}{tok.get('prompt') or 0:>9,}{tok.get('cached') or 0:>9,}{tok.get('completion') or 0:>7,}"
                     f"{(c.get('chars') or {}).get('prompt_new', 0):>10,}{c.get('usd') or 0:>9.5f}{c.get('latency_s') or 0:>6.1f}")
    _print(calls, a.json, "\n".join(lines))
    return 0


def cmd_trajectories(a) -> int:
    s = get_llm_session(a.ref)
    trajectories = s.get_trajectories(_filter(s.get_calls(), a))
    rows = [{k: v for k, v in t.items() if k != "calls"} for t in trajectories.values()]
    lines = [f"{'trajectory':<34}{'kind':<20}{'steps':>6}{'err':>5}{'retry':>6}{'rewr':>5}{'max prompt':>12}{'$':>10}"]
    lines += [f"{r['id'][:33]:<34}{str(r['kind'])[:19]:<20}{r['steps']:>6}{r['errors']:>5}{r['branches']:>6}{r['rewrites']:>5}"
              f"{r['max_prompt_tokens']:>12,}{r['usd']:>10.4f}" for r in rows]
    _print(rows, a.json, "\n".join(lines))
    return 0


def cmd_trajectory(a) -> int:
    s = get_llm_session(a.ref)
    if a.tree:
        t = s.get_trajectories()[a.id]
        lines = []
        for c in t["calls"]:
            p = c.get("path") or {}
            note = (f" branch of {p['branch_of']}" if p.get("branch_of") else "") + \
                   (f" rewritten at {p['rewritten_at']}" if p.get("rewritten_at") is not None else "")
            lines.append(f"step {(c.get('trajectory') or {}).get('step', '?'):>3}  {c['call_id']}  parent {p.get('parent_call')}"
                         f"  shares {p.get('shared_len', 0)}/{len(p.get('prompt') or [])}  {c.get('status')}{note}")
        _print(t["calls"], a.json, "\n".join(lines))
        return 0
    data = s.get_trajectory_messages(a.id)
    if a.format == "atif":
        from .atif import to_atif
        data = to_atif(s, a.id)
    elif a.format == "sharegpt":
        names = {"system": "system", "user": "human", "assistant": "gpt", "tool": "tool"}
        data = {"id": data["id"], "tools": data["tools"],
                "conversations": [{"from": names.get(m["role"], m["role"]),
                                   "value": m.get("content") if isinstance(m.get("content"), str) else json.dumps(m)}
                                  for m in data["messages"]]}
    text = json.dumps(data, ensure_ascii=False)
    if a.out:
        with open(a.out, "a", encoding="utf-8") as f:
            f.write(text + "\n")
        print(f"wrote {a.id} to {a.out}")
    else:
        print(text)
    return 0


def cmd_growth(a) -> int:
    s = get_llm_session(a.ref)
    growth = s.get_growth(_filter(s.get_calls(), a))
    total = sum(growth.values()) or 1
    lines = [f"{'source':<36}{'new chars':>14}{'share':>8}"] + [f"{k[:35]:<36}{v:>14,}{v / total:>8.1%}"
                                                               for k, v in growth.items()]
    _print(growth, a.json, "\n".join(lines))
    return 0


def cmd_pricing(a) -> int:
    from .pricing import get_openrouter_prices
    prices = get_openrouter_prices(a.base_url, refresh_s=0)
    rows = [p.to_history() for p in prices.get(a.model)]
    lines = [f"{'provider':<16}{'tag':<26}{'prompt/M':>10}{'cached/M':>10}{'compl/M':>10}  tiers"]
    for r in rows:
        b = r["base"]
        lines.append(f"{r['upstream'][:15]:<16}{r['endpoint_tag'][:25]:<26}{b['prompt_per_m']:>10.4f}"
                     f"{(b['cached_per_m'] if b['cached_per_m'] is not None else b['prompt_per_m']):>10.4f}"
                     f"{b['completion_per_m']:>10.4f}  " + ", ".join(f">={m}: {t['prompt_per_m']:.4f}/{t['completion_per_m']:.4f}"
                                                               for m, t in r["tiers"]))
    _print(rows, a.json, "\n".join(lines) if rows else f"no prices for {a.model}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="sandboxes llm", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("sessions"); p.add_argument("--client")
    p = sub.add_parser("cost"); p.add_argument("ref"); p.add_argument("--by", default="model")
    p.add_argument("--stage"); p.add_argument("--kind")
    p = sub.add_parser("calls"); p.add_argument("ref"); p.add_argument("--stage"); p.add_argument("--kind")
    p.add_argument("--trajectory"); p.add_argument("--limit", type=int, default=200)
    p = sub.add_parser("trajectories"); p.add_argument("ref"); p.add_argument("--stage"); p.add_argument("--kind")
    p = sub.add_parser("trajectory"); p.add_argument("ref"); p.add_argument("id")
    p.add_argument("--format", choices=("openai", "sharegpt", "atif"), default="openai"); p.add_argument("--out")
    p.add_argument("--tree", action="store_true")
    p = sub.add_parser("growth"); p.add_argument("ref"); p.add_argument("--stage"); p.add_argument("--kind")
    p = sub.add_parser("pricing"); p.add_argument("model"); p.add_argument("--base-url", default="https://openrouter.ai/api/v1")
    for p in sub.choices.values():
        p.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    return {"sessions": cmd_sessions, "cost": cmd_cost, "calls": cmd_calls, "trajectories": cmd_trajectories,
            "trajectory": cmd_trajectory, "growth": cmd_growth, "pricing": cmd_pricing}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
