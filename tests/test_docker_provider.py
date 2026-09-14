"""DockerProvider — integration tests against the REAL local docker daemon
(no mocking of `docker` itself; this package's whole point is to be a thin,
faithful wrapper, so exercising the actual CLI is the meaningful test here).
Requires docker on PATH; skipped otherwise so this suite still runs
somewhere without it.
"""

from __future__ import annotations

import shutil
import subprocess

import pytest

from sandboxes import DockerProvider, SandboxProviderConfig
from sandboxes.errors import SandboxNotOpenError, SandboxOpenError

BASE_IMAGE = "python:3.11-slim"

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None
    or subprocess.run(["docker", "info"], capture_output=True, timeout=10).returncode != 0,
    reason="docker not available",
)


@pytest.fixture
def provider():
    p = DockerProvider(SandboxProviderConfig(name="docker"))
    yield p
    p.close()  # idempotent even if a test already closed it, or never opened


def test_open_close_roundtrip(provider):
    assert not provider.is_open
    provider.open(BASE_IMAGE)
    assert provider.is_open
    provider.close()
    assert not provider.is_open
    provider.close()  # idempotent, must not raise


def test_run_command_before_open_raises(provider):
    with pytest.raises(SandboxNotOpenError):
        provider.run_command("echo hi")


def test_open_unknown_image_raises_sandbox_open_error(provider):
    with pytest.raises(SandboxOpenError):
        provider.open("this-image-definitely-does-not-exist:latest")


def test_run_command_basic(provider):
    provider.open(BASE_IMAGE, workdir="/tmp")
    r = provider.run_command("echo hello-sandboxes")
    assert r.exit_code == 0
    assert "hello-sandboxes" in r.output


def test_run_command_respects_cwd_override(provider):
    provider.open(BASE_IMAGE, workdir="/tmp")
    r = provider.run_command("pwd", cwd="/")
    assert r.output.strip() == "/"
    r2 = provider.run_command("pwd")  # falls back to open()'s workdir
    assert r2.output.strip() == "/tmp"


def test_run_command_nonzero_exit_is_reported_not_raised(provider):
    provider.open(BASE_IMAGE)
    r = provider.run_command("exit 7")
    assert r.exit_code == 7


def test_put_file_single_file_roundtrip(provider, tmp_path):
    provider.open(BASE_IMAGE, workdir="/tmp")
    src = tmp_path / "hello.txt"
    src.write_text("hello from the host\n")
    provider.put_file(src, "/tmp/hello.txt")
    r = provider.run_command("cat /tmp/hello.txt")
    assert r.output == "hello from the host\n"
    # docker cp writes root:root by default -- put_file's chmod must make it
    # readable/executable regardless of which user run_command execs as.
    perm = provider.run_command("stat -c %a /tmp/hello.txt")
    assert perm.output.strip() == "755"


def test_put_file_custom_permissions(provider, tmp_path):
    provider.open(BASE_IMAGE, workdir="/tmp")
    src = tmp_path / "secret.txt"
    src.write_text("shh\n")
    provider.put_file(src, "/tmp/secret.txt", permissions="600")
    perm = provider.run_command("stat -c %a /tmp/secret.txt")
    assert perm.output.strip() == "600"


def test_put_file_directory_mode(provider, tmp_path):
    provider.open(BASE_IMAGE, workdir="/tmp")
    d = tmp_path / "pkg"
    d.mkdir()
    (d / "a.txt").write_text("a\n")
    (d / "sub").mkdir()
    (d / "sub" / "b.txt").write_text("b\n")

    provider.put_file(d, "/tmp/pkg", mode="dir")
    r = provider.run_command("cat /tmp/pkg/a.txt /tmp/pkg/sub/b.txt")
    assert r.output == "a\nb\n"


def test_get_file_roundtrip(provider, tmp_path):
    provider.open(BASE_IMAGE, workdir="/tmp")
    provider.run_command("echo 'from the sandbox' > /tmp/out.txt")
    dest = tmp_path / "downloaded" / "out.txt"
    provider.get_file("/tmp/out.txt", dest)
    assert dest.read_text() == "from the sandbox\n"


def test_add_dependency_installs_live_and_records_the_ledger(provider):
    provider.open(BASE_IMAGE, workdir="/tmp")
    assert provider.dependencies == []
    provider.add_dependency("pip install --quiet pyyaml", note="needed for a fixture")
    r = provider.run_command("python3 -c 'import yaml; print(yaml.__name__)'")
    assert r.exit_code == 0
    assert r.output.strip() == "yaml"

    deps = provider.dependencies
    assert len(deps) == 1
    assert deps[0].command == "pip install --quiet pyyaml"
    assert deps[0].note == "needed for a fixture"
    assert deps[0].step == 0


def test_add_dependency_ledger_is_a_copy_not_a_live_reference(provider):
    provider.open(BASE_IMAGE)
    snapshot = provider.dependencies
    provider.add_dependency("true")
    assert snapshot == []  # the earlier snapshot must not have mutated


def test_get_total_runtime_cost_is_always_zero_for_docker(provider):
    provider.open(BASE_IMAGE)
    assert provider.get_total_runtime_cost() == 0.0
    provider.close()
    assert provider.get_total_runtime_cost() == 0.0


def test_session_context_manager_closes_on_exception(tmp_path):
    provider = DockerProvider()
    with pytest.raises(RuntimeError):
        with provider.session(BASE_IMAGE):
            assert provider.is_open
            raise RuntimeError("boom")
    assert not provider.is_open


def test_build_sandbox_image_and_open_from_it(tmp_path):
    ctx = tmp_path / "ctx"
    ctx.mkdir()
    (ctx / "Dockerfile").write_text(
        "FROM python:3.11-slim\nRUN echo built-by-sandboxes-test > /marker.txt\n"
    )
    provider = DockerProvider()
    tag = "sandboxes-test-build:latest"
    try:
        result = provider.build_sandbox_image(ctx, tag=tag, timeout=300)
        assert result.ok, result.detail
        assert result.image == tag

        provider.open(tag)
        r = provider.run_command("cat /marker.txt")
        assert r.output.strip() == "built-by-sandboxes-test"
    finally:
        provider.close()
        subprocess.run(["docker", "rmi", "-f", tag], capture_output=True, timeout=30)


def test_build_sandbox_image_failure_is_reported_not_raised(tmp_path):
    ctx = tmp_path / "bad_ctx"
    ctx.mkdir()
    (ctx / "Dockerfile").write_text("FROM this-base-image-does-not-exist:latest\n")
    provider = DockerProvider()
    result = provider.build_sandbox_image(ctx, tag="sandboxes-test-bad:latest", timeout=60)
    assert not result.ok
    assert result.detail
