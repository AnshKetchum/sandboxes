"""Sandbox spend caps: total, per run, per provider; read from caps.json / env; spend from the cost ledger."""
import json

import pytest

from sandboxes import CostLedger, cost_model_for, guard
from sandboxes.cost import SpendCapExceeded


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(tmp_path / "led.jsonl"))
    monkeypatch.setattr(guard, "CAPS_FILE", tmp_path / "caps.json")
    monkeypatch.delenv("SANDBOX_CAP_USD", raising=False); monkeypatch.delenv("SANDBOX_RUN_CAP_USD", raising=False)
    monkeypatch.delenv("SANDBOX_RUN_ID", raising=False)
    monkeypatch.setattr(guard, "_CACHE_S", 0.0); guard._warned.clear()
    return tmp_path


def spend(tmp, provider="e2b", hours=10, run="r1", running=False):
    import time
    led = CostLedger(tmp / "led.jsonl", run=run)
    if running:                                   # an open event that started `hours` ago and never closed
        rec = {"t": time.time() - hours * 3600, "ev": "open", "id": "r" + str(hours) + provider, "provider": provider, "run": run,
               "sid": "", "cpu": 2, "ram_gib": 8, "disk_gib": 20, "labels": {}}
        led._append(rec)
        return
    h = led.open(provider, cpu=2, ram_gib=8, disk_gib=20); led._open[h]["t"] -= hours * 3600
    led.close(h, cost_model_for(provider))


def test_default_total_cap_and_caps_file(env):
    assert guard.load_caps()["total_usd"] == guard.DEFAULT_TOTAL_CAP_USD
    spend(env)                                                   # ~$2.3
    assert guard.check_admission("e2b") > 2
    (env / "caps.json").write_text(json.dumps({"total_usd": 2.0}))
    with pytest.raises(SpendCapExceeded):
        guard.check_admission("e2b")
    (env / "caps.json").write_text(json.dumps({"total_usd": 100}))
    guard.check_admission("e2b")                                 # raising the cap reopens admission without a restart


def test_running_sandboxes_count_toward_the_cap(env, monkeypatch):
    monkeypatch.setenv("SANDBOX_CAP_USD", "1.0")
    spend(env, running=True, hours=8)                            # still open, ~$1.8 so far
    with pytest.raises(SpendCapExceeded):
        guard.check_admission()


def test_run_and_provider_caps(env, monkeypatch):
    spend(env, provider="e2b", run="a"); spend(env, provider="daytona", run="b", hours=1)
    (env / "caps.json").write_text(json.dumps({"total_usd": 1000, "runs": {"a": 1.0}, "providers": {"daytona": 0.05}}))
    with pytest.raises(SpendCapExceeded):
        guard.check_admission("e2b", run="a")
    guard.check_admission("e2b", run="b")                        # run b is fine, e2b has no provider cap
    with pytest.raises(SpendCapExceeded):
        guard.check_admission("daytona", run="b")
    monkeypatch.setenv("SANDBOX_RUN_ID", "b"); monkeypatch.setenv("SANDBOX_RUN_CAP_USD", "0.01")
    with pytest.raises(SpendCapExceeded):
        guard.check_admission("e2b")


def test_status_headroom(env, monkeypatch):
    monkeypatch.setenv("SANDBOX_CAP_USD", "10")
    spend(env, running=True, hours=1)
    st = guard.status()
    assert st["headroom_usd"] < 10 and st["burn_usd_per_hour"] > 0 and st["hours_to_cap_at_current_burn"] > 0


def test_reconcile_closes_entries_whose_sandbox_is_gone(env):
    import time
    led = CostLedger(env / "led.jsonl", run="r")
    for sid in ("alive", "gone"):
        led._append({"t": time.time() - 3600, "ev": "open", "id": sid, "provider": "e2b", "run": "r", "sid": sid,
                     "cpu": 2, "ram_gib": 4, "disk_gib": 20, "labels": {}})
    assert guard.reconcile(env / "led.jsonl", live={"e2b": {"alive"}}) == 1          # "gone" no longer exists on e2b
    s = __import__("sandboxes").cost_summary(env / "led.jsonl")["providers"]["e2b"]
    assert s["closed"] == 1 and s["running"] == 1
    assert guard.reconcile(env / "led.jsonl", live={"e2b": None}) == 0              # provider could not be listed: leave alone
