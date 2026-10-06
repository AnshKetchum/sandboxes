"""The one command set: ls / stop / reap by owner and run, standard labels, the harbor adapter, and the abstraction audit."""
import asyncio
import json
import os
import sys
import time
import types

import pytest

from sandboxes import CostLedger, fleet, set_context, standard_labels
from sandboxes.cost import SpendCapExceeded, summary
from sandboxes.labels import normalize_backend, owner_of
from sandboxes.pricing import CostModel
from sandboxes.raw import ExecResult, LiveSandbox, RawSandbox
from sandboxes.registry import PROVIDERS, ProviderInfo, register_provider


def _sb(i, owner=None, run=None, state="started", age_h=1.0, legacy=None):
    labels = {}
    if owner:
        labels["sandboxes.owner"] = owner; labels["sandboxes.managed"] = "true"
    if run:
        labels["sandboxes.run"] = run
    if legacy:
        labels[legacy] = "1"
    return LiveSandbox("fy", i, time.time() - age_h * 3600, state, labels)


class FakeFy(RawSandbox):
    provider = "fy"
    hard_lifetime = True
    alive: list = []
    killed: list = []

    @classmethod
    async def list_live(cls): return [s for s in cls.alive if s.id not in cls.killed]

    @classmethod
    async def kill(cls, sandbox_id): cls.killed.append(sandbox_id)

    @classmethod
    def cost_model(cls, overrides=None): return CostModel("fy", vcpu_hour=1.0)

    async def _create(self): self.sandbox_id = "fy-new"
    async def _exec(self, c, cwd, env, t): return ExecResult(0)
    async def _upload(self, d, p): ...
    async def _download(self, p): return b""
    async def _delete(self): ...


@pytest.fixture
def fy(tmp_path, monkeypatch):
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(tmp_path / "led.jsonl"))
    monkeypatch.setattr(fleet, "_docker_ok", lambda: False)
    register_provider(ProviderInfo("fy", (), f"{__name__}:FakeFy", None, {}))
    FakeFy.killed = []
    FakeFy.alive = [_sb("a1", "euphoria", "r1"), _sb("a2", "euphoria", "r2"), _sb("g1", "greenhouse", "r1"),
                    _sb("old", None, None, legacy="agentdist", age_h=9), _sb("bad", "euphoria", "r1", state="BUILD_FAILED"),
                    _sb("stuck", "euphoria", "r1", state="creating", age_h=0.5), _sb("fresh", "euphoria", "r1", state="creating", age_h=0.01)]
    yield
    PROVIDERS.pop("fy", None)


def run(coro):
    return asyncio.run(coro)


def test_ls_filters_by_owner_run_and_age_and_knows_legacy_markers(fy):
    ids = lambda **kw: sorted(s.id for s in run(fleet.ls(providers=["fy"], **kw)))
    assert ids(owner="euphoria") == ["a1", "a2", "bad", "fresh", "stuck"]
    assert ids(owner="euphoria", run="r1") == ["a1", "bad", "fresh", "stuck"]
    assert ids(owner="pr-task-synthesis") == ["old"]                 # an `agentdist` sandbox from before the standard labels
    assert ids(older_than_hours=5) == ["old"]
    assert owner_of({"euphoria.managed": "true"}) == "euphoria"


def test_stop_needs_a_scope_and_only_touches_what_matches(fy):
    with pytest.raises(ValueError):
        run(fleet.stop(providers=["fy"]))
    dry = run(fleet.stop(owner="greenhouse", providers=["fy"], dry_run=True))
    assert [s.id for s in dry] == ["g1"] and FakeFy.killed == []
    run(fleet.stop(owner="euphoria", run="r1", providers=["fy"]))
    assert sorted(FakeFy.killed) == ["a1", "bad", "fresh", "stuck"]          # a2 (another run) and greenhouse's g1 are untouched


def test_reap_kills_only_broken_sandboxes(fy):
    out = run(fleet.reap(owner="euphoria", creating_older_than_min=8, providers=["fy"]))
    assert sorted(sb.id for sb, _ in out) == ["bad", "stuck"] and sorted(FakeFy.killed) == ["bad", "stuck"]     # a fresh creation is left alone
    assert fleet.classify_state("SandboxState.BUILD_FAILED") == "error" and fleet.classify_state("started") == "running"


def test_stop_closes_the_ledger_entries_of_what_it_killed(fy, tmp_path):
    led = CostLedger(tmp_path / "led.jsonl", run="x")
    led._append({"t": time.time() - 3600, "ev": "open", "id": "h1", "provider": "fy", "run": "x", "sid": "a1", "cpu": 2, "ram_gib": 4, "disk_gib": 10, "labels": {}})
    run(fleet.stop(owner="euphoria", run="r1", providers=["fy"]))
    s = summary(tmp_path / "led.jsonl")["providers"]["fy"]
    assert s["closed"] == 1 and s["running"] == 0 and s["closed_usd"] > 1.9


def test_docker_adapter_recognises_standard_labels_and_legacy_names(monkeypatch):
    lines = [
        {"ID": "c1", "Names": "sbx-abc", "Image": "python:3.11", "Labels": "sandboxes.managed=true,sandboxes.owner=euphoria,sandboxes.run=r9", "State": "running", "CreatedAt": "2026-10-06 10:00:00 +0000 UTC"},
        {"ID": "c2", "Names": "gh-r-20260918-t1-ab12", "Image": "python:3.11", "Labels": "", "State": "running", "CreatedAt": "2026-10-06 10:00:00 +0000 UTC"},
        {"ID": "c3", "Names": "postgres", "Image": "postgres:16", "Labels": "", "State": "running", "CreatedAt": "2026-10-06 10:00:00 +0000 UTC"},
    ]
    monkeypatch.setattr(fleet._subprocess, "run", lambda *a, **k: types.SimpleNamespace(stdout="\n".join(json.dumps(x) for x in lines), returncode=0))
    got = {s.id: owner_of(s.labels) for s in fleet._docker_live_sync()}
    assert got == {"c1": "euphoria", "c2": "greenhouse"}              # an unrelated container (postgres) is never ours


def test_every_sandbox_started_through_the_library_is_labelled(fy, monkeypatch, tmp_path):
    set_context(owner="demo", run="r-42")
    try:
        sbx = FakeFy.__new__(FakeFy)
        RawSandbox.__init__(sbx, __import__("sandboxes").SandboxSpec(image="x"), CostModel("fy", vcpu_hour=1.0))
        run(sbx.start())
        assert sbx.spec.labels["sandboxes.owner"] == "demo" and sbx.spec.labels["sandboxes.run"] == "r-42" and sbx.spec.labels["sandboxes.managed"] == "true"
        run(sbx.stop())
    finally:
        os.environ.pop("SANDBOX_OWNER", None); os.environ.pop("SANDBOX_RUN_ID", None)
    assert normalize_backend("daytona") == "sandboxes" and normalize_backend("docker") == "docker"


def test_harbor_adapter_labels_tracks_cost_and_respects_caps(tmp_path, monkeypatch):
    from sandboxes import harbor_adapter
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(tmp_path / "led.jsonl"))
    monkeypatch.setattr("sandboxes.guard.CAPS_FILE", tmp_path / "caps.json")
    made = {}

    class FakeHarborEnv:
        def __init__(self, **kw): made.update(kw); self._sandbox = types.SimpleNamespace(id="sb-77")
        async def start(self, force_build=False): ...
        async def stop(self, delete=False): ...

    mod = types.ModuleType("fake_harbor_env2"); mod.Env = FakeHarborEnv
    monkeypatch.setitem(sys.modules, "fake_harbor_env2", mod)
    monkeypatch.setitem(harbor_adapter.HARBOR_ENVS, "daytona", ("fake_harbor_env2", "Env"))
    task = types.SimpleNamespace(short_name="t1", paths=types.SimpleNamespace(environment_dir="/e"),
                                 config=types.SimpleNamespace(environment=types.SimpleNamespace(docker_image=None)))
    released = []
    env = harbor_adapter.make_env("daytona", task=task, trial_paths=None, session_id="t1__x", size={"cpu": 1, "memory_mb": 2048, "disk_gb": 8, "auto_delete_minutes": 0},
                                  owner="euphoria", run="r5", on_release=lambda: released.append(1))
    assert made["labels"]["sandboxes.owner"] == "euphoria" and made["labels"]["sandboxes.run"] == "r5"
    assert made["auto_delete_interval_mins"] == 240                    # 0 / unset never means "for ever"
    run(env.start()); run(env.stop(delete=True))
    s = summary(tmp_path / "led.jsonl")["providers"]["daytona"]
    assert s["opened"] == 1 and s["closed"] == 1 and released == [1]
    (tmp_path / "caps.json").write_text(json.dumps({"total_usd": 1.0}))
    CostLedger(tmp_path / "led.jsonl")._append({"t": time.time(), "ev": "storage", "provider": "daytona", "run": "", "kind": "image", "gib": 0, "seconds": 0, "usd": 5.0, "label": "x"})
    import sandboxes.guard as g
    g._CACHE_S = 0.0
    with pytest.raises(SpendCapExceeded):
        harbor_adapter.make_env("daytona", task=task, trial_paths=None, session_id="t1__y", size={})


def test_audit_finds_provider_specific_code_and_honours_exceptions(tmp_path):
    from sandboxes import audit
    (tmp_path / "bad.py").write_text("from daytona import AsyncDaytona\nx = 1\nif name == 'e2b':\n    pass\nk = 'FLY_API_KEY'\n")
    (tmp_path / "ok.py").write_text("from daytona import X  # sandboxes-audit: ok (reason)\n")
    (tmp_path / "analysis.py").write_text("prov = 'daytona' if name == 'daytona' else 'x'\n")
    (tmp_path / ".sandboxes-audit-ignore").write_text("analysis.py\n")
    hits = audit.scan([str(tmp_path)])
    assert sorted({h[0].split("/")[-1] for h in hits}) == ["bad.py"] and len(hits) == 3      # import, name branch, credential


@pytest.mark.parametrize("path", ["/ansh/euphoria-wt/euphoria", "/ansh/greenhouse/src", "/ansh/petunia/petunia",
                                  "/ansh/prism-datagen/synthesis/pr_task_synthesis", "/ansh/prism-datagen/synthesis/agentdist"])
def test_consuming_projects_hold_no_provider_logic(path):
    from sandboxes import audit
    if not os.path.isdir(path):
        pytest.skip(f"{path} not present")
    hits = audit.scan([path])
    assert not hits, "\n".join(f"{p}:{i}: {why}: {t}" for p, i, why, t in hits[:10])


def test_stop_closes_ledger_entries_even_when_the_provider_is_slow_to_stop_listing_them(fy, tmp_path, monkeypatch):
    led = CostLedger(tmp_path / "led.jsonl", run="x")
    led._append({"t": time.time() - 3600, "ev": "open", "id": "h1", "provider": "fy", "run": "x", "sid": "a1", "cpu": 2, "ram_gib": 4, "disk_gib": 10, "labels": {}})

    async def still_listed(cls):                 # a provider that keeps listing a sandbox for a few seconds after kill
        return list(cls.alive)
    monkeypatch.setattr(FakeFy, "list_live", classmethod(still_listed))
    run(fleet.stop(owner="euphoria", run="r1", providers=["fy"]))
    assert summary(tmp_path / "led.jsonl")["providers"]["fy"]["running"] == 0
