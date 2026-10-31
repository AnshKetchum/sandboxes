"""Sessions: each client gets its own cost ledger; the machine-wide spend cap still adds all of them up."""
import json
import time

import pytest

from sandboxes import cost, fleet, guard, session
from sandboxes.cost import CostLedger, SpendCapExceeded
from sandboxes.pricing import cost_model_for


@pytest.fixture
def cache(tmp_path, monkeypatch):
    monkeypatch.setenv("SANDBOX_CACHE_DIR", str(tmp_path))
    for k in (cost.LEDGER_ENV, cost.SESSION_ENV, cost.RUN_ENV, "SANDBOX_CAP_USD", "SANDBOX_RUN_CAP_USD"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(cost, "_ledger", None)
    monkeypatch.setattr(guard, "CAPS_FILE", tmp_path / "caps.json")
    monkeypatch.setattr(guard, "_cache", {"t": 0.0, "key": None, "sum": None, "run_sum": None})
    return tmp_path


def _spend(provider="daytona", seconds=3600.0, cpu=2, ram=4, disk=10):
    """Open and immediately close one sandbox in the active ledger, billed for `seconds`."""
    led = cost.get_ledger()
    h = led.open(provider, cpu=cpu, ram_gib=ram, disk_gib=disk)
    led._open[h]["t"] -= seconds
    return led.close(h, cost_model_for(provider))


def test_session_gets_its_own_ledger(cache):
    s = session.open_session("euphoria", "v3-b07", tags={"config": "x.yaml"})
    assert s.ledger_path == cache / "sessions" / "euphoria" / s.id / "costs.jsonl"
    usd = _spend()
    assert usd > 0
    assert s.cost()["total_usd"] == pytest.approx(usd, rel=1e-3)
    assert s.hours() == pytest.approx(1.0, rel=1e-2)
    assert not (cache / "costs.jsonl").exists()                 # nothing leaked into the legacy shared ledger
    assert json.loads(s.meta_path.read_text())["tags"] == {"config": "x.yaml"}


def test_two_sessions_do_not_mix(cache):
    a = session.open_session("euphoria", "run-a")
    ua = _spend("daytona", 3600)
    b = session.open_session("greenhouse", "run-b")             # a new active session switches the process to the new ledger
    ub = _spend("modal", 7200)
    assert a.cost()["total_usd"] == pytest.approx(ua, rel=1e-3)
    assert b.cost()["total_usd"] == pytest.approx(ub, rel=1e-3)
    assert set(a.cost()["providers"]) == {"daytona"} and set(b.cost()["providers"]) == {"modal"}
    assert cost.summary_all()["total_usd"] == pytest.approx(ua + ub, rel=1e-3)


def test_summary_all_includes_the_legacy_ledger(cache):
    legacy = CostLedger(cache / "costs.jsonl")
    h = legacy.open("e2b", cpu=1, ram_gib=2, disk_gib=8); legacy._open[h]["t"] -= 3600
    ul = legacy.close(h, cost_model_for("e2b"))
    s = session.open_session("euphoria", "r")
    us = _spend("daytona")
    assert cost.summary_all()["total_usd"] == pytest.approx(ul + us, rel=1e-3)
    assert set(cost.summary_all()["providers"]) == {"e2b", "daytona"}


def test_lookup_by_id_name_and_prefix(cache):
    s1 = session.open_session("euphoria", "v3-b01"); time.sleep(1.1)
    s2 = session.open_session("euphoria", "v3-b01")             # same name twice: the newest wins by name
    assert session.get_session(s1.id).id == s1.id
    assert session.get_session("v3-b01").id == s2.id
    assert session.get_session(s1.id[:-2]).id == s1.id
    assert [x.id for x in session.list_sessions("euphoria")] == [s2.id, s1.id]
    assert session.list_sessions("greenhouse") == []
    with pytest.raises(KeyError):
        session.get_session("nope")


def test_context_manager_closes_and_deactivates(cache):
    with session.open_session("euphoria", "ctx") as s:
        assert session.current_session().id == s.id
    assert s.ended is not None and json.loads(s.meta_path.read_text())["ended"] is not None
    assert session.current_session() is None


def test_spend_cap_adds_up_every_session(cache):
    (cache / "caps.json").write_text(json.dumps({"total_usd": 1.0}))
    session.open_session("euphoria", "a"); _spend("modal", 3600)         # ~$0.38
    guard.check_admission("modal")                                       # under the cap
    session.open_session("greenhouse", "b"); _spend("modal", 3 * 3600)   # another ~$1.1 in a different ledger
    guard._cache["t"] = 0.0
    with pytest.raises(SpendCapExceeded):
        guard.check_admission("modal")


def test_reconcile_visits_every_ledger(cache):
    now = time.time()
    for client in ("euphoria", "greenhouse"):
        s = session.open_session(client, "r")
        s.ledger_path.write_text(json.dumps({"t": now - 600, "ev": "open", "id": client, "provider": "daytona", "run": "r", "sid": "gone-" + client,
                                             "cpu": 1, "ram_gib": 2, "disk_gib": 8}) + "\n")
    assert fleet._reconcile_core(None, {"daytona": set()}, now) == 2        # both entries were open, their sandboxes no longer exist
    assert fleet._reconcile_core(None, {"daytona": set()}, now) == 0
    assert all(any(json.loads(l)["ev"] == "close" for l in s.ledger_path.read_text().splitlines()) for s in session.list_sessions())


def test_env_opt_in(cache, monkeypatch):
    monkeypatch.setenv(session.CLIENT_ENV, "pr_task_synthesis"); monkeypatch.setenv(session.NAME_ENV, "v3a")
    s = session.ensure_env_session()
    assert s.client == "pr_task_synthesis" and s.name == "v3a"
    assert session.ensure_env_session().id == s.id                         # idempotent: a second call reuses the active session


def test_use_session_activates_then_restores(cache):
    outer = session.open_session("greenhouse", "outer")
    inner = session.open_session("euphoria", "inner", activate=False)
    assert session.current_session().id == outer.id
    with session.use_session(inner) as s:
        assert s.id == inner.id and session.current_session().id == inner.id
        _spend("daytona", 1800)                                  # billed to inner's ledger, not outer's
    assert session.current_session().id == outer.id               # the previous session is back
    assert inner.cost()["total_usd"] > 0 and outer.cost()["total_usd"] == 0
    with session.use_session(inner.id):                           # by id, and by directory
        assert session.current_session().id == inner.id
    with session.use_session(str(inner.dir)):
        assert session.current_session().id == inner.id


def test_use_session_restores_an_empty_environment(cache):
    s = session.open_session("euphoria", "x", activate=False)
    with session.use_session(s):
        pass
    assert session.current_session() is None
