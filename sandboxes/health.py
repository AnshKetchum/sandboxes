"""A circuit breaker per provider, shared by every process on the host.

A provider that fails every start looks the emptiest, so a scheduler that picks "the least loaded" sends it the whole queue, and each
failed attempt burns one of a task's tries (Fly did this, then Blaxel when its image quota filled: 1,700 attempts lost in two
minutes). Starts are recorded here as they succeed or fail; `healthy(provider)` turns False while most recent starts are failing.
The breaker is half-open: a tripped provider still gets one probe attempt every PROBE_EVERY_S, so it comes back by itself once a start
succeeds. A caller that picks providers asks `healthy()` and prefers the healthy ones; it needs to know nothing else about a provider.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
from pathlib import Path
from typing import Optional

WINDOW_S = 180            # outcomes older than this are forgotten
MIN_SAMPLES = 12          # fewer outcomes than this prove nothing
TRIP_FAIL_RATE = 0.75     # this share of recent starts failing trips the breaker
PROBE_EVERY_S = 15        # while tripped, let one start through this often
_KEEP = 400


def _path(provider: str) -> Path:
    d = Path(os.environ.get("SANDBOX_HEALTH_DIR") or Path.home() / ".cache" / "sandboxes" / "health")
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{provider}.json"


def _update(provider: str, fn):
    with open(_path(provider), "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.seek(0)
            try:
                st = json.loads(f.read() or "{}")
            except ValueError:
                st = {}
            st.setdefault("events", [])
            before = json.dumps(st)
            result = fn(st)
            after = json.dumps(st)
            if after != before:                 # healthy() is called per scheduled attempt: only write when something changed
                f.seek(0)
                f.truncate()
                f.write(after)
                f.flush()
            return result
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def record(provider: str, ok: bool, now: Optional[float] = None) -> None:
    """Note that a sandbox start on `provider` succeeded or failed."""
    now = time.time() if now is None else now

    def add(st):
        st["events"] = [e for e in st["events"] if now - e[0] < WINDOW_S][-_KEEP:] + [[now, 1 if ok else 0]]
    _update(provider, add)


def healthy(provider: str, now: Optional[float] = None) -> bool:
    """False while most recent starts on `provider` failed (apart from one probe every PROBE_EVERY_S)."""
    now = time.time() if now is None else now

    def check(st):
        ev = [e for e in st["events"] if now - e[0] < WINDOW_S]
        if len(ev) < MIN_SAMPLES or sum(1 for e in ev if not e[1]) / len(ev) < TRIP_FAIL_RATE:
            return True
        if now - float(st.get("probed", 0)) >= PROBE_EVERY_S:
            st["probed"] = now
            return True                       # the probe: its outcome decides whether the breaker closes
        return False
    return bool(_update(provider, check))
