"""DaytonaProvider — offline unit tests only. Opening a real Daytona sandbox
costs real money and wall-clock time, so this suite deliberately does NOT
create one; it covers what's testable without a live sandbox (cost math,
config validation, the task-directory contract, build/push preconditions).
A live smoke test against a real DAYTONA_API_KEY is a manual, deliberate
step — not part of this automated suite.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("harbor", reason="daytona extras not installed")

from sandboxes import SandboxProviderConfig
from sandboxes.daytona_provider import DaytonaProvider
from sandboxes.errors import SandboxNotOpenError, SandboxOpenError


class _FakeSandbox:
    """Stands in for `daytona.AsyncSandbox` — only the fields/method
    `_real_cost` actually reads: `organization_id`, `id`, and the private
    `_get_analytics_api_url` coroutine method it calls before ever touching
    the real Analytics API client."""

    def __init__(self, organization_id="org-1", sandbox_id="sbx-1"):
        self.organization_id = organization_id
        self.id = sandbox_id

    async def _get_analytics_api_url(self):
        return "https://analytics.example.test"


class _FakeEnv:
    def __init__(self, sandbox):
        self._sandbox = sandbox


def test_get_total_runtime_cost_is_zero_with_no_rate_configured():
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    p._opened_at = 100.0
    p._closed_at = 460.0  # 0.1 hour later
    assert p.get_total_runtime_cost() == 0.0  # usd_per_hour defaults to 0.0


def test_get_total_runtime_cost_before_open_is_zero():
    p = DaytonaProvider(SandboxProviderConfig(name="daytona", usd_per_hour=1.0))
    assert p.get_total_runtime_cost() == 0.0


def test_get_total_runtime_cost_math_while_open(monkeypatch):
    p = DaytonaProvider(SandboxProviderConfig(name="daytona", usd_per_hour=2.0))
    p._opened_at = 1000.0
    monkeypatch.setattr("time.monotonic", lambda: 1000.0 + 1800.0)  # 0.5h elapsed, still open
    assert p.get_total_runtime_cost() == pytest.approx(1.0)  # 0.5h * $2/h


def test_get_total_runtime_cost_freezes_after_close():
    p = DaytonaProvider(SandboxProviderConfig(name="daytona", usd_per_hour=2.0))
    p._opened_at = 0.0
    p._closed_at = 3600.0  # exactly 1 hour
    assert p.get_total_runtime_cost() == pytest.approx(2.0)
    # calling it again later must not keep accruing past close()
    assert p.get_total_runtime_cost() == pytest.approx(2.0)


def test_get_elapsed_seconds_before_open_is_zero():
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    assert p.get_elapsed_seconds() == 0.0


def test_get_elapsed_seconds_freezes_after_close():
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    p._opened_at = 0.0
    p._closed_at = 42.0
    assert p.get_elapsed_seconds() == 42.0


def test_stats_composes_elapsed_seconds_and_cost_from_the_base_class():
    """stats() lives on SandboxProvider, not reimplemented per-provider —
    proven by getting the right numbers out of DaytonaProvider without this
    module ever defining stats() itself."""
    p = DaytonaProvider(SandboxProviderConfig(name="daytona", usd_per_hour=2.0))
    p._opened_at = 0.0
    p._closed_at = 3600.0
    assert p.stats() == {"seconds": 3600.0, "cost_usd": pytest.approx(2.0)}


def test_real_cost_is_none_without_an_open_sandbox():
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    assert p._real_cost() is None


def test_get_total_runtime_cost_prefers_the_analytics_api_over_the_estimate(monkeypatch):
    """When the Analytics API answers, its `price` sum wins even though it
    disagrees with what the usd_per_hour estimate would have computed —
    proving the real figure isn't just a tie-breaker."""
    p = DaytonaProvider(SandboxProviderConfig(name="daytona", usd_per_hour=2.0))
    p._env = _FakeEnv(_FakeSandbox())
    p._opened_at = 1000.0
    p._closed_at = 4600.0  # 1 hour -> estimate would be $2.00
    p._opened_at_wall = datetime(2026, 1, 1, tzinfo=timezone.utc)
    p._closed_at_wall = datetime(2026, 1, 1, 1, tzinfo=timezone.utc)

    async def fake_fetch(sandbox, organization_id, sandbox_id, start, end):
        assert organization_id == "org-1"
        assert sandbox_id == "sbx-1"
        return 0.37  # the "real" metered figure, deliberately != the estimate

    monkeypatch.setattr("sandboxes.daytona_provider._fetch_analytics_cost", fake_fetch)
    assert p.get_total_runtime_cost() == pytest.approx(0.37)


def test_get_total_runtime_cost_falls_back_when_analytics_api_has_no_answer(monkeypatch):
    p = DaytonaProvider(SandboxProviderConfig(name="daytona", usd_per_hour=2.0))
    p._env = _FakeEnv(_FakeSandbox())
    p._opened_at = 0.0
    p._closed_at = 3600.0
    p._opened_at_wall = datetime(2026, 1, 1, tzinfo=timezone.utc)
    p._closed_at_wall = datetime(2026, 1, 1, 1, tzinfo=timezone.utc)

    async def fake_fetch(*a, **kw):
        return None  # e.g. deployment has no separate Analytics API

    monkeypatch.setattr("sandboxes.daytona_provider._fetch_analytics_cost", fake_fetch)
    assert p.get_total_runtime_cost() == pytest.approx(2.0)  # 1h * $2/h estimate


def test_get_total_runtime_cost_falls_back_when_analytics_api_raises(monkeypatch):
    """A network/permissions failure querying the real cost must never
    surface to the caller — it silently falls back to the estimate."""
    p = DaytonaProvider(SandboxProviderConfig(name="daytona", usd_per_hour=2.0))
    p._env = _FakeEnv(_FakeSandbox())
    p._opened_at = 0.0
    p._closed_at = 3600.0
    p._opened_at_wall = datetime(2026, 1, 1, tzinfo=timezone.utc)
    p._closed_at_wall = datetime(2026, 1, 1, 1, tzinfo=timezone.utc)

    async def fake_fetch(*a, **kw):
        raise RuntimeError("analytics API unreachable")

    monkeypatch.setattr("sandboxes.daytona_provider._fetch_analytics_cost", fake_fetch)
    assert p.get_total_runtime_cost() == pytest.approx(2.0)


def test_real_cost_is_none_when_sandbox_lacks_identity_fields():
    """A sandbox object missing organization_id/id (e.g. a stub, or an SDK
    shape change) is treated as unusable rather than crashing."""
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    p._env = _FakeEnv(_FakeSandbox(organization_id=None, sandbox_id="sbx-1"))
    p._opened_at_wall = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert p._real_cost() is None


def test_session_id_without_name_prefix_is_unchanged():
    from sandboxes.daytona_provider import _session_id
    sid = _session_id(None, "my-task")
    assert sid.startswith("my-task__")
    assert sid.count("__") == 1


def test_session_id_with_name_prefix_is_prepended():
    """A caller-supplied name_prefix (e.g. a run id) shows up in the
    Daytona session id, mirroring DockerProvider's container-name prefix."""
    from sandboxes.daytona_provider import _session_id
    sid = _session_id("r-20260914-231318-t-0004", "my-task")
    assert sid.startswith("r-20260914-231318-t-0004__my-task__")


def test_run_command_before_open_raises():
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    with pytest.raises(SandboxNotOpenError):
        p.run_command("echo hi")


def test_open_rejects_volumes(tmp_path, monkeypatch):
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key-not-real")
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    with pytest.raises(SandboxOpenError, match="does not support host bind mounts"):
        p.open(str(tmp_path), volumes={"/host/dir": "/container/dir"})


def test_close_before_open_is_a_noop():
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    p.close()  # must not raise


def test_open_requires_daytona_api_key(tmp_path, monkeypatch):
    monkeypatch.delenv("DAYTONA_API_KEY", raising=False)
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    with pytest.raises(SandboxOpenError, match="DAYTONA_API_KEY"):
        p.open(str(tmp_path))


def test_open_rejects_a_directory_with_no_task_toml(tmp_path, monkeypatch):
    monkeypatch.setenv("DAYTONA_API_KEY", "test-key-not-real")
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    with pytest.raises(SandboxOpenError, match="task.toml"):
        p.open(str(tmp_path))


def test_build_sandbox_image_requires_builder_image_config(tmp_path):
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    result = p.build_sandbox_image(tmp_path, tag="whatever:latest")
    assert not result.ok
    assert "builder_image" in result.detail


def test_push_sandbox_image_requires_builder_image_config():
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    result = p.push_sandbox_image("whatever:latest", registry="localhost:5000")
    assert not result.ok
    assert "builder_image" in result.detail


def test_add_dependency_ledger_shape_matches_docker_provider():
    """Both providers share the SAME DependencyEntry model (imported from
    sandboxes.models, not redefined per-provider) — a caller switching
    providers gets an identical ledger shape back, by construction."""
    from sandboxes import DockerProvider
    import sandboxes.daytona_provider as daytona_mod
    import sandboxes.docker_provider as docker_mod

    assert daytona_mod.DependencyEntry is docker_mod.DependencyEntry
