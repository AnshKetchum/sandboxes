"""DaytonaProvider — offline unit tests only. Opening a real Daytona sandbox
costs real money and wall-clock time, so this suite deliberately does NOT
create one; it covers what's testable without a live sandbox (cost math,
config validation, the task-directory contract, build/push preconditions).
A live smoke test against a real DAYTONA_API_KEY is a manual, deliberate
step — not part of this automated suite.
"""

from __future__ import annotations

import pytest

pytest.importorskip("harbor", reason="daytona extras not installed")

from sandboxes import SandboxProviderConfig
from sandboxes.daytona_provider import DaytonaProvider
from sandboxes.errors import SandboxNotOpenError, SandboxOpenError


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


def test_run_command_before_open_raises():
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    with pytest.raises(SandboxNotOpenError):
        p.run_command("echo hi")


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
