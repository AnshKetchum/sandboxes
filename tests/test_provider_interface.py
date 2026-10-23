"""The provider contract: cost tracking and cost cutting are first class, and generic logic needs nothing else."""
import asyncio
import json
import time

import pytest

from sandboxes import CostLedger, CostModel, fleet
from sandboxes.cost import summary
from sandboxes.raw import ExecResult, LiveSandbox, RawSandbox, StoredArtifact
from sandboxes.registry import PROVIDERS, ProviderInfo, load_class, register_provider


@pytest.mark.parametrize("name", ["daytona", "e2b", "blaxel", "modal", "fly", "gke"])
def test_every_real_provider_satisfies_the_contract(name):
    cls = load_class(PROVIDERS[name].raw)
    assert issubclass(cls, RawSandbox) and cls.provider == name
    assert not cls.__abstractmethods__, f"{name} leaves {set(cls.__abstractmethods__)} unimplemented"
    assert isinstance(cls.hard_lifetime, bool)
    m = cls.cost_model()
    assert isinstance(m, CostModel) and m.provider == name and (m.vcpu_hour or m.ram_gib_hour)


def test_incomplete_subclasses_are_rejected():
    with pytest.raises(TypeError, match="hard_lifetime"):
        class NoLifetime(RawSandbox):             # forgot to say whether the provider enforces lifetimes itself
            provider = "x1"
    class NoFleet(RawSandbox):                    # can be defined, but cannot be used: no way to list or kill
        provider = "x2"; hard_lifetime = True
        async def _create(self): ...
        async def _exec(self, c, cwd, env, t): return ExecResult(0)
        async def _upload(self, d, p): ...
        async def _download(self, p): return b""
        async def _delete(self): ...
    with pytest.raises(TypeError):
        NoFleet(None)


class FakeFx(RawSandbox):
    provider = "fx"
    hard_lifetime = False
    alive: list = []
    killed: list = []

    @classmethod
    async def list_live(cls): return list(cls.alive)

    @classmethod
    async def kill(cls, sandbox_id): cls.killed.append(sandbox_id)

    @classmethod
    async def stored_artifacts(cls): return [StoredArtifact("fx", "image", "img-1", 10.0)]

    @classmethod
    def cost_model(cls, overrides=None): return CostModel("fx", vcpu_hour=1.0, ram_gib_hour=0.0, image_gib_month=730.0)   # $1 per GiB-hour stored

    async def _create(self): self.sandbox_id = "x"
    async def _exec(self, c, cwd, env, t): return ExecResult(0)
    async def _upload(self, d, p): ...
    async def _download(self, p): return b""
    async def _delete(self): ...


def test_a_new_provider_gets_tracking_reconcile_janitor_and_storage_for_free(tmp_path, monkeypatch):
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(tmp_path / "led.jsonl"))
    register_provider(ProviderInfo("fx", (), f"{__name__}:FakeFx", None, {}))
    try:
        FakeFx.alive, FakeFx.killed = [], []
        led = CostLedger(tmp_path / "led.jsonl", run="r")
        for sid in ("keep", "gone", "old"):
            led._append({"t": time.time() - 7200, "ev": "open", "id": sid, "provider": "fx", "run": "r", "sid": sid,
                         "cpu": 2, "ram_gib": 4, "disk_gib": 10, "labels": {}})
        # 1. reconcile: "gone" no longer exists on the provider, so it is closed (2 vCPU x 2 h x $1)
        FakeFx.alive = [LiveSandbox("fx", "keep", time.time() - 7200), LiveSandbox("fx", "old", time.time() - 9 * 3600)]
        assert fleet.reconcile(tmp_path / "led.jsonl") == 1
        s = summary(tmp_path / "led.jsonl")["providers"]["fx"]
        assert s["closed"] == 1 and s["running"] == 2 and abs(s["closed_usd"] - 4.0) < 0.05
        # 2. janitor: only the sandbox older than 6 h is killed, through the provider's own kill()
        found = asyncio.run(fleet.sweep(6.0, ["fx"]))
        assert [n for n, _ in found] == ["fx:old"] and FakeFx.killed == ["old"]
        assert asyncio.run(fleet.sweep(6.0, ["fx"], dry_run=True)) and FakeFx.killed == ["old"]      # dry run kills nothing more
        # 3. storage: 10 GiB for an hour at $1/GiB-hour lands in the ledger as a storage event
        usd = asyncio.run(fleet.accrue_storage(3600, ["fx"]))
        assert abs(usd - 10.0) < 0.01
        assert any(json.loads(l)["ev"] == "storage" for l in open(tmp_path / "led.jsonl"))
        assert summary(tmp_path / "led.jsonl")["providers"]["fx"]["storage_usd"] > 9.9
    finally:
        PROVIDERS.pop("fx", None)
