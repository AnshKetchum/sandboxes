"""Offline tests: cost models, the cost ledger, multi-provider config, and router failover."""
import asyncio
import json
import time

import pytest
import yaml

from sandboxes import CapacityError, CostLedger, SandboxRouter, cost_model_for
from sandboxes.config import get_sandbox_provider, load_provider_entries, load_sandbox_providers
from sandboxes.cost import SpendCapExceeded, check_cap, summary
from sandboxes.errors import ConfigError
from sandboxes.models import SandboxProviderConfig
from sandboxes.raw import ExecResult, RawSandbox
from sandboxes.registry import PROVIDERS, ProviderInfo, register_provider, usable


# -- pricing ---------------------------------------------------------------------------------------
def test_price_per_hour_2cpu_4gib():
    # list prices: daytona/e2b ~ $0.166/h, blaxel (RAM only) ~ $0.166/h for 4 GB; docker free
    assert cost_model_for("daytona").hourly_usd(2, 4, 10) == pytest.approx(2 * 0.0504 + 4 * 0.0162 + 5 * 0.000108)
    assert cost_model_for("e2b").hourly_usd(2, 4, 20) == pytest.approx(2 * 0.0504 + 4 * 0.0162)
    assert cost_model_for("blaxel").hourly_usd(2, 4, 0) == pytest.approx(4 * 0.0000115 * 3600)
    assert cost_model_for("docker").hourly_usd(2, 4, 10) == 0


def test_storage_and_overrides():
    m = cost_model_for("blaxel")
    assert m.storage_usd("snapshot", 10, 730 * 3600) == pytest.approx(2.0)      # $0.20/GB-month
    assert m.storage_usd("volume", 8, 730 * 3600) == pytest.approx(0.96)
    assert cost_model_for("e2b").storage_usd("snapshot", 100, 3600) == 0        # E2B storage is free
    assert cost_model_for("e2b", {"vcpu_hour": 1.0}).vcpu_hour == 1.0
    with pytest.raises(ValueError):
        m.storage_usd("bogus", 1, 1)


# -- ledger ----------------------------------------------------------------------------------------
def test_ledger_open_close_running_and_cap(tmp_path):
    led = CostLedger(tmp_path / "l.jsonl", run="r1")
    m = cost_model_for("e2b")
    h1 = led.open("e2b", cpu=2, ram_gib=4, disk_gib=20)
    led.open("e2b", cpu=2, ram_gib=4, disk_gib=20)            # never closed: counted as running
    led._open[h1]["t"] -= 3600                                  # pretend h1 ran an hour
    usd = led.close(h1, m)
    assert usd == pytest.approx(m.hourly_usd(2, 4, 20), rel=1e-3)
    led.storage("blaxel", "snapshot", 10, 730 * 3600)
    s = summary(tmp_path / "l.jsonl")
    assert s["providers"]["e2b"]["opened"] == 2 and s["providers"]["e2b"]["running"] == 1
    assert s["providers"]["blaxel"]["storage_usd"] == pytest.approx(2.0)
    assert s["burn_usd_per_hour"] == pytest.approx(m.hourly_usd(2, 4, 20), rel=1e-3)
    assert summary(tmp_path / "l.jsonl", run="other")["total_usd"] == 0
    with pytest.raises(SpendCapExceeded):
        check_cap(0.01, tmp_path / "l.jsonl")


# -- config ----------------------------------------------------------------------------------------
def _multi(tmp_path):
    f = tmp_path / "multi.yaml"
    f.write_text(yaml.safe_dump({"total_spend_cap_usd": 50, "sandbox_providers": [
        {"name": "daytona", "peak_concurrent_sandboxes": 10},
        {"name": "e2b", "peak_concurrent_sandboxes": 5, "priority": -1},
        {"name": "blaxel", "peak_concurrent_sandboxes": 7, "enabled": False},
    ]}))
    return f


def test_provider_used_only_when_key_present(tmp_path, monkeypatch):
    monkeypatch.delenv("E2B_API_KEY", raising=False); monkeypatch.delenv("DAYTONA_API_KEY", raising=False)
    f = _multi(tmp_path)
    assert get_sandbox_provider(f).name == "e2b"        # none usable: first enabled entry; open() reports the key
    monkeypatch.setenv("DAYTONA_API_KEY", "x")
    assert get_sandbox_provider(f).name == "daytona"
    monkeypatch.setenv("E2B_API_KEY", "x")
    assert get_sandbox_provider(f).name == "e2b"                  # lower priority number wins
    ok, why = usable(SandboxProviderConfig(name="blaxel", enabled=False))
    assert not ok and "disabled" in why
    assert usable(SandboxProviderConfig(name="blaxel"), env={"BL_API_KEY": "k"})[1] == "BL_WORKSPACE not set"
    assert usable(SandboxProviderConfig(name="blaxel", api_key_env=["FOO"]), env={"FOO": "1"})[0]


def test_base_overrides_flat_and_per_provider(tmp_path):
    f = _multi(tmp_path)
    flat = load_sandbox_providers(f, {"peak_concurrent_sandboxes": 99})
    assert [e.get("peak_concurrent_sandboxes") for e in flat] == [99, 5, 7]
    per = load_sandbox_providers(f, {"e2b": {"peak_concurrent_sandboxes": 1}, "daytona": {"cpu": 8}})
    assert per[0]["cpu"] == 8 and per[1]["peak_concurrent_sandboxes"] == 1 and per[2]["peak_concurrent_sandboxes"] == 7
    outer = tmp_path / "outer.yaml"
    outer.write_text(yaml.safe_dump({"sandbox_providers": [{"base": str(f), "overrides": {"e2b": {"cpu": 3}}},
                                                           {"name": "docker"}]}))
    names = [e["name"] for e in load_provider_entries(outer)["providers"]]
    assert names == ["daytona", "e2b", "blaxel", "docker"]
    assert load_provider_entries(f)["total_spend_cap_usd"] == 50


def test_shipped_configs_parse():
    for name in ("daytona", "docker", "e2b", "blaxel", "multi", "multi-dind"):
        entries = load_provider_entries(f"/ansh/sandboxes/configs/{name}.yaml")["providers"]
        for e in entries:
            SandboxProviderConfig.model_validate(e)


# -- router ----------------------------------------------------------------------------------------
class FakeRaw(RawSandbox):
    provider = "fake"
    hard_lifetime = True
    full = False
    started = 0

    @classmethod
    async def list_live(cls): return []

    @classmethod
    async def kill(cls, sandbox_id): ...

    async def _create(self):
        if type(self).full:
            raise CapacityError(self.provider, "429")
        type(self).started += 1
        self.sandbox_id = f"{self.provider}-{type(self).started}"

    async def _exec(self, command, cwd, env, timeout):
        return ExecResult(0, "ok")

    async def _upload(self, data, remote_path): ...
    async def _download(self, remote_path): return b""
    async def _delete(self): ...


class FakeA(FakeRaw):
    provider = "fa"; hard_lifetime = True; full = False; started = 0


class FakeB(FakeRaw):
    provider = "fb"; hard_lifetime = False; full = False; started = 0


@pytest.fixture
def fakes(monkeypatch, tmp_path):
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(tmp_path / "led.jsonl"))
    register_provider(ProviderInfo("fa", (), f"{__name__}:FakeA", None, {"dind": True}))
    register_provider(ProviderInfo("fb", (), f"{__name__}:FakeB", None, {"dind": False}))
    FakeA.full = FakeB.full = False; FakeA.started = FakeB.started = 0
    yield
    PROVIDERS.pop("fa", None); PROVIDERS.pop("fb", None)


def _router(**kw):
    return SandboxRouter([SandboxProviderConfig(name="fa", peak_concurrent_sandboxes=3),
                          SandboxProviderConfig(name="fb", peak_concurrent_sandboxes=2)], **kw)


def test_router_spreads_by_free_capacity_and_respects_caps(fakes):
    async def go():
        r = _router()
        boxes = [await r.open("img") for _ in range(5)]
        assert r.status()["fa"]["in_use"] == 3 and r.status()["fb"]["in_use"] == 2
        with pytest.raises(Exception) as ei:
            await r.open("img", wait_s=0.1)
        assert "no provider" in str(ei.value)
        await r.close(boxes[0])
        assert sum(v["in_use"] for v in r.status().values()) == 4
    asyncio.run(go())


def test_router_requires_capability_and_fails_over(fakes):
    async def go():
        r = _router()
        assert r.total_capacity(("dind",)) == 3 and r.total_capacity() == 5
        a = await r.open("img", require=("dind",))
        assert a.provider == "fa"
        FakeA.full = True                                       # fa now answers 429
        b = await r.open("img")                                 # no capability needed: goes to fb
        assert b.provider == "fb" and r.status()["fa"]["capacity_errors"] >= 1
    asyncio.run(go())


def test_router_stops_at_spend_cap(fakes, tmp_path):
    async def go():
        r = _router(total_spend_cap_usd=0.0001)
        led = CostLedger(tmp_path / "led.jsonl")
        h = led.open("fa", cpu=2, ram_gib=4, disk_gib=10); led._open[h]["t"] -= 7200; led.close(h, cost_model_for("daytona"))
        with pytest.raises(SpendCapExceeded):
            await r.open("img")
    asyncio.run(go())
