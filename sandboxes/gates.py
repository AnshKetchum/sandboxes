"""Cross-process gates for provider limits that are not request rates: how many builds may run at once, and one build per template.

Both are built on flock, so they work across every worker process on the host and a process that dies releases what it held.

    async with build_slot("e2b", limit=15):        # at most 15 holders at once, host-wide
        ...
    async with single_flight("e2b", template):     # one holder per key; the others wait, then find the work done
        ...
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path


def _dir() -> Path:
    d = Path(os.environ.get("SANDBOX_GATE_DIR") or Path.home() / ".cache" / "sandboxes" / "gates")
    d.mkdir(parents=True, exist_ok=True)
    return d


def _try_lock(path: Path):
    """An open file holding an exclusive flock on `path`, or None when someone else holds it."""
    f = open(path, "a+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return f
    except OSError:
        f.close()
        return None


def _unlock(f) -> None:
    try:
        fcntl.flock(f, fcntl.LOCK_UN)
    finally:
        f.close()


@asynccontextmanager
async def build_slot(provider: str, limit: int, poll_s: float = 0.5):
    """One of `limit` host-wide slots for `provider`'s builds (e.g. E2B allows 20 concurrent template builds)."""
    held = None
    while held is None:
        for i in range(max(1, int(limit))):
            held = _try_lock(_dir() / f"slot-{provider}-{i}.lock")
            if held:
                break
        else:
            await asyncio.sleep(poll_s)
    try:
        yield
    finally:
        _unlock(held)


@asynccontextmanager
async def single_flight(provider: str, key: str, poll_s: float = 0.3):
    """Exclusive host-wide lock on (provider, key): the first caller does the work, the rest wait and re-check afterwards."""
    name = hashlib.sha1(key.encode()).hexdigest()[:16]
    held = None
    while held is None:
        held = _try_lock(_dir() / f"flight-{provider}-{name}.lock")
        if held is None:
            await asyncio.sleep(poll_s)
    try:
        yield
    finally:
        _unlock(held)


def remember_failure(provider: str, key: str, message: str) -> None:
    """Note that the work for (provider, key) just failed, so callers that were waiting on it fail at once instead of redoing it."""
    (_dir() / f"failed-{provider}-{hashlib.sha1(key.encode()).hexdigest()[:16]}.txt").write_text(message)


def recent_failure(provider: str, key: str, ttl_s: float = 600) -> str:
    """The message of a failure remembered for (provider, key) within `ttl_s`, else ''."""
    p = _dir() / f"failed-{provider}-{hashlib.sha1(key.encode()).hexdigest()[:16]}.txt"
    try:
        return p.read_text() if time.time() - p.stat().st_mtime < ttl_s else ""
    except OSError:
        return ""


def log_build_failure(provider: str, key: str, error: str) -> None:
    """Append one line to ~/.cache/sandboxes/build-failures.jsonl so a run's build failures can be counted and read afterwards."""
    import json
    path = _dir().parent / "build-failures.jsonl"
    with open(path, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.write(json.dumps({"t": time.time(), "provider": provider, "key": key, "error": error[:600]}) + "\n")
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
