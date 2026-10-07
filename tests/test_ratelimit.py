"""Sandbox creation is rate limited per provider, across processes, from the provider's published limits."""
import asyncio
import multiprocessing as mp
import time

import pytest

from sandboxes import ratelimit
from sandboxes.registry import PROVIDERS


@pytest.fixture(autouse=True)
def _dir(monkeypatch, tmp_path):
    monkeypatch.setenv("SANDBOX_RATELIMIT_DIR", str(tmp_path))
    for p in PROVIDERS:
        monkeypatch.delenv(f"SANDBOX_CREATE_RATE_{p.upper()}", raising=False)


def test_published_rates_are_declared():
    assert ratelimit.rate_for("modal") == (5, 5)
    assert ratelimit.rate_for("e2b") == (5, 5)
    assert ratelimit.rate_for("fly") == (1, 3)
    assert ratelimit.rate_for("daytona") == (5, 5)
    assert ratelimit.rate_for("blaxel") == (0, 0)         # no per-second limit published


def test_env_overrides_and_zero_disables(monkeypatch):
    monkeypatch.setenv("SANDBOX_CREATE_RATE_MODAL", "2")
    assert ratelimit.rate_for("modal")[0] == 2
    monkeypatch.setenv("SANDBOX_CREATE_RATE_MODAL", "0")
    assert ratelimit.rate_for("modal") == (0, 0)


def test_bucket_allows_a_burst_then_paces():
    t = 1000.0
    waits = [ratelimit.take("x", 5, 5, now=t) for _ in range(5)]
    assert waits == [0] * 5                                # the burst
    w = ratelimit.take("x", 5, 5, now=t)
    assert w == pytest.approx(0.2)                         # then one per 1/rate
    assert ratelimit.take("x", 5, 5, now=t + 0.2) == 0     # and refills with time


def _worker(dirname, n, out):
    import os
    os.environ["SANDBOX_RATELIMIT_DIR"] = dirname
    os.environ["SANDBOX_CREATE_RATE_MODAL"] = "20"
    async def go():
        for _ in range(n):
            await ratelimit.acquire_create("modal")
    asyncio.run(go())
    out.put(time.time())


def test_the_limit_is_shared_across_processes(tmp_path):
    out = mp.get_context("fork").Queue()
    procs = [mp.get_context("fork").Process(target=_worker, args=(str(tmp_path), 10, out)) for _ in range(4)]
    t0 = time.time()
    [p.start() for p in procs]
    [p.join() for p in procs]
    elapsed = max(out.get() for _ in procs) - t0
    # 40 creations at 20/s with a burst of 20: the last 20 take >= ~1 s however many processes ask
    assert elapsed >= 0.9


def test_start_waits_for_the_limiter_and_retries_a_throttled_create(monkeypatch):
    import os
    from sandboxes.backends import modal_raw
    from sandboxes.errors import CapacityError
    from sandboxes.raw import SandboxSpec
    os.environ["SANDBOX_COST_LEDGER"] = os.devnull
    calls, took = [], []

    async def fake_acquire(provider):
        took.append(provider)

    async def fake_sleep(s):
        pass

    async def create(self):
        calls.append(1)
        if len(calls) < 3:
            raise CapacityError("modal", "Sandbox creation rate limit (5/s) exceeded")
        self.sandbox_id = "sb-1"

    monkeypatch.setattr("sandboxes.ratelimit.acquire_create", fake_acquire)
    monkeypatch.setattr("sandboxes.raw.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(modal_raw.ModalRaw, "_create", create)
    asyncio.run(modal_raw.ModalRaw(SandboxSpec(image="x")).start())
    assert len(calls) == 3 and took == ["modal"] * 3          # throttled twice, then created; every try waited for its token
