"""DaytonaProvider — REAL integration tests against the actual Daytona API.

Unlike test_daytona_provider.py (deliberately offline-only), these open real
sandboxes: real time, real cost (small — a couple of minutes of the cheapest
tier), real network calls. Skipped entirely unless DAYTONA_API_KEY is set, so
the offline suite (and CI without credentials) is unaffected.

Kept intentionally small: one open/exec/close roundtrip test, one for the
name_prefix -> session_id -> harbor.session_id label chain, one for scoped
kill_sandboxes(name_contains=...). Every sandbox this file opens is closed in
a finally block; a run that gets interrupted mid-test still has the shared
auto_delete_minutes safety net (see configs/daytona.yaml) as a backstop.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

pytest.importorskip("harbor", reason="daytona extras not installed")

DAYTONA_API_KEY = os.environ.get("DAYTONA_API_KEY", "")

pytestmark = pytest.mark.skipif(
    not DAYTONA_API_KEY,
    reason="DAYTONA_API_KEY not set — real Daytona integration tests skipped",
)


def _wait_until(predicate, *, timeout=20, interval=1):
    """Daytona's list endpoint is eventually consistent — a sandbox just
    created (or just deleted) can be briefly absent (or briefly still
    present) from list() results. Bounded poll, never an unbounded wait —
    fails loudly past `timeout` rather than hanging."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    return last


@pytest.fixture
def minimal_task_dir(tmp_path):
    """The smallest valid harbor task directory DaytonaProvider.open() will
    accept — task.toml + environment/Dockerfile, nothing else."""
    (tmp_path / "environment").mkdir()
    (tmp_path / "task.toml").write_text(
        'schema_version = "1.4"\n[task]\nname = "sandboxes/integration-test"\n')
    (tmp_path / "environment" / "Dockerfile").write_text(
        "FROM python:3.11-slim\nWORKDIR /workspace\n")
    (tmp_path / "instruction.md").write_text("# integration test\n")
    return tmp_path


def test_open_exec_close_roundtrip_against_a_real_sandbox(minimal_task_dir):
    from sandboxes import DaytonaProvider, SandboxProviderConfig

    p = DaytonaProvider(SandboxProviderConfig(
        name="daytona", disk_gb=8, cpu=1, memory_mb=2048,
        auto_stop_minutes=5, auto_delete_minutes=0))
    try:
        p.open(str(minimal_task_dir), workdir="/workspace")
        assert p.is_open
        r = p.run_command("echo hello-from-real-daytona")
        assert r.exit_code == 0
        assert "hello-from-real-daytona" in r.output
        stats = p.stats()
        assert stats["seconds"] > 0
        assert stats["cost_usd"] >= 0
    finally:
        p.close()
    assert not p.is_open


def test_name_prefix_reaches_the_real_sandboxes_harbor_session_id_label(minimal_task_dir):
    """Confirms the actual mechanism `greenhouse clean`'s Daytona teardown
    (and find_sandboxes/kill_sandboxes's own name_contains filter) depends
    on: harbor's DaytonaEnvironment sets a real, queryable
    'harbor.session_id' label from the session_id we build, and our
    name_prefix is folded into that same session_id (see _session_id)."""
    from sandboxes import DaytonaProvider, SandboxProviderConfig
    from sandboxes.daytona_provider import find_sandboxes

    marker = f"sbxtest-{uuid.uuid4().hex[:10]}"
    p = DaytonaProvider(SandboxProviderConfig(
        name="daytona", disk_gb=8, cpu=1, memory_mb=2048,
        auto_stop_minutes=5, auto_delete_minutes=0))
    try:
        p.open(str(minimal_task_dir), workdir="/workspace", name_prefix=marker)
        matches = _wait_until(lambda: find_sandboxes(name_contains=marker))
        assert matches and len(matches) == 1
        assert marker in matches[0].labels.get("harbor.session_id", "")
        assert not find_sandboxes(name_contains="definitely-not-a-real-marker-xyz")
    finally:
        p.close()


def test_kill_sandboxes_scoped_by_name_contains_only_deletes_the_match(minimal_task_dir):
    from sandboxes import DaytonaProvider, SandboxProviderConfig
    from sandboxes.daytona_provider import find_sandboxes, kill_sandboxes

    marker_a = f"sbxtest-a-{uuid.uuid4().hex[:10]}"
    marker_b = f"sbxtest-b-{uuid.uuid4().hex[:10]}"
    pa = DaytonaProvider(SandboxProviderConfig(name="daytona", auto_stop_minutes=5))
    pb = DaytonaProvider(SandboxProviderConfig(name="daytona", auto_stop_minutes=5))
    try:
        pa.open(str(minimal_task_dir), workdir="/workspace", name_prefix=marker_a)
        pb.open(str(minimal_task_dir), workdir="/workspace", name_prefix=marker_b)
        # both must be visible in list() before we can meaningfully assert
        # kill_sandboxes only touched one of them — see _wait_until's docstring.
        assert _wait_until(lambda: find_sandboxes(name_contains=marker_a))
        assert _wait_until(lambda: find_sandboxes(name_contains=marker_b))

        n = kill_sandboxes(name_contains=marker_a)
        assert n == 1
        assert _wait_until(lambda: not find_sandboxes(name_contains=marker_a))
        assert len(find_sandboxes(name_contains=marker_b)) == 1
        pa._is_open = False  # already deleted by kill_sandboxes — close() would no-op anyway
    finally:
        pa.close()
        pb.close()
