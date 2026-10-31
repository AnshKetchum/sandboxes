"""Cross-process gates (build slots, single flight) and the per-provider circuit breaker."""
import asyncio
import multiprocessing as mp
import os
import time
from pathlib import Path

import pytest

from sandboxes import gates, health


@pytest.fixture(autouse=True)
def _dirs(monkeypatch, tmp_path):
    monkeypatch.setenv("SANDBOX_GATE_DIR", str(tmp_path / "gates"))
    monkeypatch.setenv("SANDBOX_HEALTH_DIR", str(tmp_path / "health"))


def _hold(gate_dir, n, limit, out):
    os.environ["SANDBOX_GATE_DIR"] = gate_dir
    peak = []

    async def one():
        async with gates.build_slot("x", limit, poll_s=0.01):
            out.put(("in", time.time()))
            await asyncio.sleep(0.3)
            out.put(("out", time.time()))

    async def go():
        await asyncio.gather(*(one() for _ in range(n)))
    asyncio.run(go())


def test_build_slots_cap_concurrency_across_processes(tmp_path):
    ctx = mp.get_context("fork")
    out = ctx.Queue()
    procs = [ctx.Process(target=_hold, args=(str(tmp_path / "gates"), 3, 2, out)) for _ in range(3)]    # 9 holders, 2 slots
    [p.start() for p in procs]
    [p.join() for p in procs]
    events = sorted([out.get() for _ in range(18)], key=lambda e: e[1])
    live = peak = 0
    for kind, _ in events:
        live += 1 if kind == "in" else -1
        peak = max(peak, live)
    assert peak <= 2


def test_single_flight_runs_one_at_a_time_per_key():
    order = []

    async def worker(key, tag):
        async with gates.single_flight("e2b", key, poll_s=0.01):
            order.append(("start", tag))
            await asyncio.sleep(0.05)
            order.append(("end", tag))

    async def go():
        await asyncio.gather(worker("t1", "a"), worker("t1", "b"), worker("t2", "c"))
    asyncio.run(go())
    t1 = [e for e in order if e[1] in ("a", "b")]
    assert [e[0] for e in t1] == ["start", "end", "start", "end"]          # same key: strictly one after the other


def test_the_breaker_trips_on_mostly_failing_starts_probes_and_recovers():
    t = 1000.0
    assert health.healthy("p", now=t)                                       # nothing recorded
    for i in range(14):
        health.record("p", False, now=t + i)
    assert health.healthy("p", now=t + 15)                                  # the first call after tripping is the probe
    assert not health.healthy("p", now=t + 16)                              # then blocked
    assert health.healthy("p", now=t + 31)                                  # and one probe every PROBE_EVERY_S
    for i in range(30):
        health.record("p", True, now=t + 32 + i * 0.1)                      # it recovers
    assert health.healthy("p", now=t + 36) and health.healthy("p", now=t + 36.1)


def test_a_few_failures_do_not_trip_it():
    t = 2000.0
    for i in range(20):
        health.record("q", i % 5 != 0, now=t + i)                           # 20% failing
    assert all(health.healthy("q", now=t + 21 + i) for i in range(5))


def test_the_e2b_environment_builds_once_per_template_and_under_the_build_cap(monkeypatch, tmp_path):
    pytest.importorskip("e2b")
    from sandboxes import harbor_e2b
    built, live, peak = [], [0], [0]
    exists = {"v": False}

    async def fake_build(self):
        live[0] += 1
        peak[0] = max(peak[0], live[0])
        await asyncio.sleep(0.05)
        built.append(self._template_name)
        exists["v"] = True
        live[0] -= 1

    async def fake_exists(self):
        return exists["v"]

    parent = harbor_e2b.LifetimeE2BEnvironment.__mro__[1]
    monkeypatch.setattr(parent, "_create_template", fake_build)
    monkeypatch.setattr(harbor_e2b.LifetimeE2BEnvironment, "_does_template_exist", fake_exists)

    env = object.__new__(harbor_e2b.LifetimeE2BEnvironment)
    env._template_name = "t__abc"
    env.environment_dir, env.task_env_config = Path("/nonexistent"), type("C", (), {"docker_image": None})()

    async def go():
        await asyncio.gather(*(env._create_template() for _ in range(3)))
    asyncio.run(go())
    assert built == ["t__abc"]                                              # three tries of one task: one build
    assert peak[0] == 1


def test_a_failed_build_is_not_repeated_by_its_siblings_and_a_tagless_alias_is_not_built(monkeypatch):
    pytest.importorskip("e2b")
    from sandboxes import harbor_e2b
    parent = harbor_e2b.LifetimeE2BEnvironment.__mro__[1]
    attempts = []

    async def failing_build(self):
        attempts.append(1)
        await asyncio.sleep(0.02)
        raise RuntimeError("setup_runtime.sh exit 2")

    async def alias_only(self):
        return False                                   # the alias exists but nothing was built: treated as not built
    monkeypatch.setattr(parent, "_create_template", failing_build)
    monkeypatch.setattr(harbor_e2b.LifetimeE2BEnvironment, "_does_template_exist", alias_only)
    env = object.__new__(harbor_e2b.LifetimeE2BEnvironment)
    env._template_name = "t__bad"
    env.environment_dir, env.task_env_config = Path("/nonexistent"), type("C", (), {"docker_image": None})()

    async def go():
        return await asyncio.gather(*(env._create_template() for _ in range(3)), return_exceptions=True)
    res = asyncio.run(go())
    assert len(attempts) == 1 and all(isinstance(r, RuntimeError) for r in res)       # one build, three failures, no 404 from a half-made alias


def test_multi_variable_env_lines_are_split_for_e2b():
    pytest.importorskip("e2b")
    from sandboxes.harbor_e2b import split_env_instructions
    src = 'FROM debian:12\nENV DEBIAN_FRONTEND=noninteractive \\\n    PIP_NO_CACHE_DIR=1 \\\n    MSG="a b"\nENV SINGLE=1\nRUN echo ENV A=1 B=2\nENV LEGACY value with spaces\n'
    out = split_env_instructions(src).split("\n")
    assert "ENV DEBIAN_FRONTEND=noninteractive" in out and "ENV PIP_NO_CACHE_DIR=1" in out and "ENV MSG='a b'" in out
    assert "ENV SINGLE=1" in out                                  # one variable: untouched
    assert "RUN echo ENV A=1 B=2" in out                          # only ENV instructions are rewritten
    assert "ENV LEGACY value with spaces" in out                  # the legacy form is left to the converter


def test_a_hung_e2b_build_is_cancelled_and_logged(monkeypatch, tmp_path):
    pytest.importorskip("e2b")
    import json
    from sandboxes import harbor_e2b
    from sandboxes.registry import PROVIDERS
    monkeypatch.setitem(PROVIDERS["e2b"].capabilities, "build_timeout_s", 0.2)
    parent = harbor_e2b.LifetimeE2BEnvironment.__mro__[1]

    async def hang(self):
        await asyncio.sleep(30)

    async def never_built(self):
        return False
    monkeypatch.setattr(parent, "_create_template", hang)
    monkeypatch.setattr(harbor_e2b.LifetimeE2BEnvironment, "_does_template_exist", never_built)
    env = object.__new__(harbor_e2b.LifetimeE2BEnvironment)
    env._template_name = "t__hang"
    env.environment_dir, env.task_env_config = Path("/nonexistent"), type("C", (), {"docker_image": None})()
    t = time.time()
    with pytest.raises(RuntimeError, match="exceeded"):
        asyncio.run(env._create_template())
    assert time.time() - t < 5
    lines = [json.loads(l) for l in (tmp_path / "build-failures.jsonl").read_text().splitlines()]
    assert lines[0]["key"] == "t__hang" and "exceeded" in lines[0]["error"]
