"""A per-provider sandbox-creation rate limit shared by EVERY process on the machine.

Providers cap how fast sandboxes may be created (modal 5/s, e2b Pro 5/s, fly 1/s with a burst of 3, daytona 300-600/min by tier), and
a project that starts hundreds of sandboxes at once, from several worker processes, trips it: the rejected attempts fail and burn
a retry. `await acquire_create("modal")` waits for a token from a bucket kept in a lock file, so ten euphoria workers and a
greenhouse run on the same host draw from one budget. The rates are `create_rate_per_s` / `create_burst` in registry.DEFAULT_CAPABILITIES
(from each provider's published limits); `SANDBOX_CREATE_RATE_<PROVIDER>=<per second>` overrides one, `0` turns it off. A provider with no
declared rate is not limited.

The limit is per host: two hosts sharing one account each need a share of it.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import time
from pathlib import Path
from typing import Optional


def _dir() -> Path:
    d = Path(os.environ.get("SANDBOX_RATELIMIT_DIR") or Path.home() / ".cache" / "sandboxes")
    d.mkdir(parents=True, exist_ok=True)
    return d


def rate_for(provider: str) -> tuple[float, float]:
    """(tokens per second, burst) for creating sandboxes on `provider`; (0, 0) when unlimited."""
    from .registry import PROVIDERS
    caps = PROVIDERS[provider].capabilities if provider in PROVIDERS else {}
    rate = float(caps.get("create_rate_per_s") or 0)
    env = os.environ.get(f"SANDBOX_CREATE_RATE_{provider.upper()}")
    if env is not None:
        rate = float(env)
    if rate <= 0:
        return 0.0, 0.0
    return rate, float(caps.get("create_burst") or max(1.0, rate))


def take(provider: str, rate: float, burst: float, now: Optional[float] = None) -> float:
    """Take one token from `provider`'s shared bucket. Returns 0 when taken, else the seconds until one is available."""
    now = time.time() if now is None else now
    with open(_dir() / f"ratelimit-{provider}.json", "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.seek(0)
            try:
                st = json.loads(f.read() or "{}")
            except ValueError:
                st = {}
            tokens = min(burst, float(st.get("tokens", burst)) + max(0.0, now - float(st.get("t", now))) * rate)
            wait = 0.0
            if tokens >= 1:
                tokens -= 1
            else:
                wait = (1 - tokens) / rate
            f.seek(0)
            f.truncate()
            f.write(json.dumps({"tokens": tokens, "t": now}))
            f.flush()
            return wait
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


async def acquire_create(provider: str) -> None:
    rate, burst = rate_for(provider)
    if not rate:
        return
    while True:
        wait = take(provider, rate, burst)
        if not wait:
            return
        await asyncio.sleep(wait + 0.01)
