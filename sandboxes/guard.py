"""Sandbox spend caps, enforced by the library itself and read from the sandboxes cache dir (~/.cache/sandboxes).

Every sandbox open path asks `check_admission()` first (RawSandbox.start, SandboxRouter.open, the harbor Daytona provider,
euphoria's make_env), so a cap holds in every process on the machine without any project configuring it. The spend it
compares against is the cost ledger (costs.jsonl in the same directory): closed sandboxes at their real cost plus every
still-running sandbox at its cost so far.

Caps, tightest wins (a USD amount; 0 or unset means "no cap at this level"):

    total      SANDBOX_CAP_USD env, else caps.json "total_usd", else DEFAULT_TOTAL_CAP_USD ($500)
    per run    SANDBOX_RUN_CAP_USD env (applies to the current SANDBOX_RUN_ID), or caps.json "runs": {"scale-200": 50}
    per provider   caps.json "providers": {"e2b": 150}, or spend_cap_usd in the provider's config entry (router)

caps.json (edit by hand, takes effect within 30 s):  {"total_usd": 800, "runs": {"final-e2b": 40}, "providers": {"daytona": 300}}

A cap only stops NEW sandboxes. To also stop the ones running, keep a guard process alive:

    python -m sandboxes.guard --watch 60 --kill       # when a cap trips, delete this stack's running sandboxes
    python -m sandboxes.guard                         # print caps, spend and headroom once
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional

from .cost import RUN_ENV, SpendCapExceeded, summary, summary_all

CACHE_DIR = Path(os.environ.get("SANDBOX_CACHE_DIR", Path.home() / ".cache" / "sandboxes"))
CAPS_FILE = CACHE_DIR / "caps.json"
DEFAULT_TOTAL_CAP_USD = float(os.environ.get("SANDBOX_DEFAULT_CAP_USD", 500))
WARN_AT = 0.8
_CACHE_S = 30.0
_cache: dict = {"t": 0.0, "key": None, "sum": None, "run_sum": None}
_warned: set = set()


def load_caps() -> dict:
    """{"total_usd": float, "runs": {run: usd}, "providers": {name: usd}} merged from env, caps.json and the default."""
    caps = {"total_usd": DEFAULT_TOTAL_CAP_USD, "runs": {}, "providers": {}}
    try:
        f = json.loads(CAPS_FILE.read_text())
        caps["total_usd"] = float(f.get("total_usd", caps["total_usd"]))
        caps["runs"].update({k: float(v) for k, v in (f.get("runs") or {}).items()})
        caps["providers"].update({k: float(v) for k, v in (f.get("providers") or {}).items()})
    except (OSError, ValueError):
        pass
    if os.environ.get("SANDBOX_CAP_USD"):
        caps["total_usd"] = float(os.environ["SANDBOX_CAP_USD"])
    run = os.environ.get(RUN_ENV, "")
    if run and os.environ.get("SANDBOX_RUN_CAP_USD"):
        caps["runs"][run] = float(os.environ["SANDBOX_RUN_CAP_USD"])
    return caps


def _spend(ledger_path, run: Optional[str]) -> tuple[dict, dict]:
    """Machine-wide spend: the legacy shared ledger plus every session ledger (sessions/<client>/<id>/costs.jsonl), added together."""
    key = (str(ledger_path), run)
    if _cache["key"] != key or time.time() - _cache["t"] > _CACHE_S:
        _cache.update({"t": time.time(), "key": key, "sum": summary_all(),
                       "run_sum": summary_all(run=run) if run else None})
    return _cache["sum"], _cache["run_sum"]


def check_admission(provider: Optional[str] = None, run: Optional[str] = None, provider_cap_usd: Optional[float] = None) -> float:
    """Raise SpendCapExceeded when opening one more sandbox would breach a cap; returns the total spend otherwise."""
    from .cost import get_ledger
    caps = load_caps()
    run = run if run is not None else os.environ.get(RUN_ENV, "")
    total_s, run_s = _spend(get_ledger().path, run or None)
    total = total_s["total_usd"]

    def trip(what, spent, cap):
        raise SpendCapExceeded(f"sandbox spend cap reached: {what} ${spent:.2f} >= ${cap:.2f} (caps in {CAPS_FILE}, spend in the cost ledger)")

    if caps["total_usd"] and total >= caps["total_usd"]:
        trip("total", total, caps["total_usd"])
    if run and caps["runs"].get(run) and run_s and run_s["total_usd"] >= caps["runs"][run]:
        trip(f"run {run}", run_s["total_usd"], caps["runs"][run])
    pcap = caps["providers"].get(provider or "") or provider_cap_usd
    if provider and pcap:
        spent = total_s["providers"].get(provider, {}).get("total_usd", 0.0)
        if spent >= pcap:
            trip(f"provider {provider}", spent, pcap)
    if caps["total_usd"] and total >= WARN_AT * caps["total_usd"] and "total" not in _warned:
        _warned.add("total")
        print(f"[sandboxes] WARNING: sandbox spend ${total:.2f} is {100 * total / caps['total_usd']:.0f}% of the ${caps['total_usd']:.0f} cap", file=sys.stderr, flush=True)
    return total


def status() -> dict:
    from .cost import get_ledger
    caps = load_caps(); s = summary_all()
    return {"caps": caps, "ledger": str(get_ledger().path), "total_usd": s["total_usd"], "running_usd": s["running_usd"],
            "burn_usd_per_hour": s["burn_usd_per_hour"], "headroom_usd": (caps["total_usd"] - s["total_usd"]) if caps["total_usd"] else None,
            "hours_to_cap_at_current_burn": ((caps["total_usd"] - s["total_usd"]) / s["burn_usd_per_hour"]) if caps["total_usd"] and s["burn_usd_per_hour"] else None,
            "by_run": {r: summary_all(run=r)["total_usd"] for r in _runs()}}


def _runs(path=None) -> list[str]:
    from .cost import all_ledger_paths
    seen = []
    for p in ([path] if path else all_ledger_paths()):
        try:
            for line in open(p):
                try:
                    r = json.loads(line).get("run", "")
                except ValueError:
                    continue
                if r and r not in seen:
                    seen.append(r)
        except OSError:
            pass
    return seen


def reconcile(path=None, live=None, now=None) -> int:
    """See fleet.reconcile: close ledger entries whose sandbox no longer exists on its provider."""
    from .fleet import reconcile as _r
    return _r(path, live, now)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--watch", type=float, default=0, help="check every N seconds")
    ap.add_argument("--kill", action="store_true", help="when a cap trips, delete this stack's running sandboxes (janitor, any age)")
    a = ap.parse_args(argv)
    last = time.time()
    while True:
        n = reconcile()
        import asyncio
        from .fleet import accrue_storage, meter_all
        now_t = time.time()
        added = asyncio.run(accrue_storage(now_t - last)) if a.watch else 0.0       # stored images/snapshots/volumes, per provider
        added += asyncio.run(meter_all(now_t - last))                              # machine-billed costs (GKE nodes), before caps are read
        last = now_t
        if added:
            print(f"  guard: stored artifacts accrued ${added:.4f}", flush=True)
        if n:
            print(f"  guard: closed {n} ledger entr{'y' if n == 1 else 'ies'} whose sandbox no longer exists", flush=True)
        st = status(); c = st["caps"]
        tripped = bool(c["total_usd"] and st["total_usd"] >= c["total_usd"])
        print(f"{time.strftime('%FT%TZ', time.gmtime())} total ${st['total_usd']:.2f} / cap ${c['total_usd']:.0f}  running ${st['running_usd']:.2f}  burn ${st['burn_usd_per_hour']:.2f}/h"
              + (f"  ~{st['hours_to_cap_at_current_burn']:.1f} h to cap" if st["hours_to_cap_at_current_burn"] else "") + ("  CAP TRIPPED" if tripped else ""), flush=True)
        for r, usd in st["by_run"].items():
            print(f"    run {r}: ${usd:.2f}" + (f" / ${c['runs'][r]:.0f}" if r in c["runs"] else ""), flush=True)
        if tripped and a.kill:
            from .fleet import sweep
            gone = asyncio.run(sweep(0.0))
            print(f"  guard: cap tripped, deleted {len(gone)} running sandbox(es)", flush=True)
        if not a.watch:
            return 1 if tripped else 0
        time.sleep(a.watch)


if __name__ == "__main__":
    sys.exit(main())
