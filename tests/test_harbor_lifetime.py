"""Auto-deletion and cost tracking for harbor environments: every provider's environment is built with a FINITE lifetime and the
labels cleanup tooling needs, and a sandbox is neither leaked on a failed start nor counted twice in the cost ledger."""

import asyncio
import json
from pathlib import Path

import pytest

pytest.importorskip("harbor")

from harbor.models.task.config import EnvironmentConfig          # noqa: E402
from harbor.models.trial.paths import TrialPaths                   # noqa: E402

from sandboxes import harbor_adapter                               # noqa: E402
from sandboxes.harbor_adapter import harbor_env_class, harbor_providers, lifetime_kwargs   # noqa: E402
from sandboxes.labels import standard_labels                       # noqa: E402
from sandboxes.lifetime import clamp_lifetime_min                  # noqa: E402

SIZE = {"auto_delete_minutes": 180, "auto_stop_minutes": 60, "extra": {"region": "iad"}}
LABELS = {"harness": "test"}
EXPECTED_S = 180 * 60


def _build(provider, tmp_path, monkeypatch, **extra_kw):
    for k in ("DAYTONA_API_KEY", "E2B_API_KEY", "BL_API_KEY", "BL_WORKSPACE", "FLY_API_KEY"):
        monkeypatch.setenv(k, "test")
    env_dir = tmp_path / "environment"
    env_dir.mkdir(exist_ok=True)
    trial = TrialPaths(trial_dir=tmp_path / "trial")
    trial.mkdir()
    kw = dict(environment_dir=env_dir, environment_name="t", session_id="t__1", trial_paths=trial,
              task_env_config=EnvironmentConfig(docker_image="python:3.12-slim"), **extra_kw)
    kw.update(lifetime_kwargs(provider, SIZE, labels=LABELS))
    if provider == "blaxel":
        from sandboxes.compat import ensure_blaxel_importable
        ensure_blaxel_importable()
    return harbor_env_class(provider)(**kw)


@pytest.mark.parametrize("provider", harbor_providers())
def test_every_harbor_provider_gets_a_finite_lifetime(provider, tmp_path, monkeypatch):
    kw = lifetime_kwargs(provider, SIZE, labels=LABELS)
    if provider == "daytona":
        assert kw["auto_delete_interval_mins"] == 180 and kw["auto_stop_interval_mins"] == 60
    elif provider == "blaxel":
        assert kw["ttl"] == "180m"
    else:
        assert kw["lifetime_s"] == EXPECTED_S and kw["labels"] == LABELS
    # and the environment object actually takes it
    try:
        env = _build(provider, tmp_path, monkeypatch)
    except Exception as e:       # the provider's SDK is not installed in this environment
        pytest.skip(f"{provider}: cannot construct here ({type(e).__name__}: {e})")
    if provider == "daytona":
        assert env._auto_delete_interval == 180 and env._auto_stop_interval == 60
    elif provider == "blaxel":
        assert env._ttl == "180m"
    else:
        assert env._lifetime_s == EXPECTED_S


def _minutes(provider, kw):
    if provider == "daytona":
        return kw["auto_delete_interval_mins"]
    if provider == "blaxel":
        return int(kw["ttl"].rstrip("m"))
    return kw["lifetime_s"] // 60


def test_unset_lifetime_is_never_unlimited():
    for provider in harbor_providers():
        minutes = _minutes(provider, lifetime_kwargs(provider, {}, labels={}))
        assert minutes == clamp_lifetime_min(None) and 0 < minutes < 24 * 60, provider


def test_e2b_sandbox_is_created_with_the_lifetime_and_standard_labels(tmp_path, monkeypatch):
    pytest.importorskip("e2b")
    from sandboxes import harbor_e2b
    created = {}

    class FakeSandbox:
        @staticmethod
        async def create(**kw):
            created.update(kw)
            return object()

    monkeypatch.setattr(harbor_e2b._harbor_e2b, "AsyncSandbox", FakeSandbox)
    env = _build("e2b", tmp_path, monkeypatch)
    asyncio.run(env._create_sandbox())
    assert created["timeout"] == EXPECTED_S                      # harbor's own value is 86_400
    meta = created["metadata"]
    assert meta["harness"] == "test" and meta["environment_name"] == "t" and meta["session_id"] == "t__1"
    assert meta.items() >= standard_labels().items()             # sandboxes.managed (+ owner/run when set): what the janitor matches


class _FakeEnv:
    LEDGER_NATIVE = False

    def __init__(self, fail_start=False, native=False):
        self.fail_start, self.LEDGER_NATIVE, self.stops, self.session_id = fail_start, native, [], "t__1"

    async def start(self, force_build=False):
        if self.fail_start:
            raise RuntimeError("boom")

    async def stop(self, delete):
        self.stops.append(delete)


def _ledger_records(path: Path):
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()] if path.exists() else []


def test_a_failed_start_deletes_whatever_it_created(monkeypatch, tmp_path):
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(tmp_path / "costs.jsonl"))
    env = _FakeEnv(fail_start=True)
    harbor_adapter._track(env, "daytona", {"cpu": 1}, {}, None)
    with pytest.raises(RuntimeError):
        asyncio.run(env.start())
    assert env.stops == [True]


@pytest.mark.parametrize("native, expected_open_close", [(False, 2), (True, 0)])
def test_ledger_entry_is_recorded_once(monkeypatch, tmp_path, native, expected_open_close):
    ledger = tmp_path / "costs.jsonl"
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(ledger))
    env = _FakeEnv(native=native)
    harbor_adapter._track(env, "daytona", {"cpu": 1, "memory_mb": 2048, "disk_gb": 8}, {}, None)

    async def run():
        await env.start()
        await env.stop(True)
    asyncio.run(run())
    assert len(_ledger_records(ledger)) == expected_open_close      # a RawSandbox-backed env records its own entry; _track must not add a second
