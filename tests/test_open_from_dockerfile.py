"""open_from_dockerfile: one provider-agnostic "build this context and start a sandbox from it" call.
docker builds under a content tag, daytona opens a harbor task dir around the context, raw-backed providers with
`builds_dockerfile` (modal, fly) build it themselves; the rest refuse. Builds are content-addressed, and a failed
build is an ImageBuildError carrying the build output."""
import asyncio
import hashlib
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest

from sandboxes import DockerProvider, SandboxProviderConfig
from sandboxes import generic_provider
from sandboxes.buildctx import context_digest
from sandboxes.errors import ImageBuildError, SandboxOpenError
from sandboxes.generic_provider import BlaxelProvider, E2BProvider, ModalProvider


def _ctx(tmp_path: Path, dockerfile: str = "FROM python:3.11-slim\n", name: str = "environment") -> Path:
    d = tmp_path / name
    d.mkdir(parents=True)
    (d / "Dockerfile").write_text(dockerfile)
    (d / "app.py").write_text("print(1)\n")
    return d


def test_context_digest_matches_the_euphoria_greenhouse_env_digest(tmp_path):
    ctx = _ctx(tmp_path)
    h = hashlib.sha256()
    for p in sorted(ctx.rglob("*")):
        if p.is_file():
            h.update(p.relative_to(ctx).as_posix().encode())
            h.update(p.read_bytes())
    assert context_digest(ctx) == h.hexdigest()[:16] == context_digest(ctx, str(ctx / "Dockerfile"))
    (ctx / "Other.Dockerfile").write_text("FROM debian:12\n")
    assert context_digest(ctx, "Other.Dockerfile") != context_digest(ctx)


@pytest.mark.parametrize("cls", [E2BProvider, BlaxelProvider])
def test_providers_without_builds_dockerfile_refuse(cls, tmp_path):
    p = cls(SandboxProviderConfig(name=cls.name))
    assert not p.can_open_from_dockerfile()
    with pytest.raises(SandboxOpenError, match="cannot build a Dockerfile"):
        p.open_from_dockerfile(_ctx(tmp_path))


def test_a_capability_override_turns_it_off():
    assert ModalProvider(SandboxProviderConfig(name="modal")).can_open_from_dockerfile()
    off = SandboxProviderConfig(name="modal", capabilities={"builds_dockerfile": False})
    assert not ModalProvider(off).can_open_from_dockerfile()


class _FakeRaw:
    specs: list = []
    fail_build = False

    def __init__(self, spec, model=None):
        self.spec = spec
        _FakeRaw.specs.append(spec)

    async def start(self):
        if _FakeRaw.fail_build:
            raise ImageBuildError("modal: image build failed for Dockerfile: ERROR: No matching distribution")
        return self

    async def stop(self, sync_dirs=()):
        return 0.0


def test_raw_backed_provider_hands_the_context_to_the_backend(monkeypatch, tmp_path):
    monkeypatch.setattr(generic_provider, "load_class", lambda _p: _FakeRaw)
    _FakeRaw.specs.clear()
    _FakeRaw.fail_build = False
    ctx = _ctx(tmp_path)
    cfg = SandboxProviderConfig(name="modal", image="gen/base:1", auto_delete_minutes=300, extra={"runtime": "vm"})
    p = ModalProvider(cfg)
    p.open_from_dockerfile(ctx, workdir="/app", name_prefix="gh-r1-t1", labels={"greenhouse.task": "t1"})
    s = _FakeRaw.specs[0]
    assert s.image == ""                                             # the config's image would win over the Dockerfile
    assert s.extra["dockerfile"] == str(ctx / "Dockerfile") and s.extra["context_dir"] == str(ctx)
    assert s.extra["runtime"] == "vm" and s.lifetime_s == 300 * 60
    assert s.labels == {"prefix": "gh-r1-t1", "greenhouse.task": "t1"}
    assert p.is_open and p._workdir == "/app"
    p.close()


@pytest.mark.parametrize("capability, network, blocked", [({}, "none", True), ({}, "bridge", False),
                                                          ({"blocks_network": False}, "none", False)])
def test_network_none_seals_a_backend_that_can_block_it(monkeypatch, tmp_path, capability, network, blocked):
    monkeypatch.setattr(generic_provider, "load_class", lambda _p: _FakeRaw)
    _FakeRaw.specs.clear()
    _FakeRaw.fail_build = False
    cfg = SandboxProviderConfig(name="modal", capabilities=capability)
    ModalProvider(cfg).open_from_dockerfile(_ctx(tmp_path), network=network)
    ModalProvider(cfg).open("python:3.12-slim", network=network)
    assert [spec.extra.get("block_network", False) for spec in _FakeRaw.specs] == [blocked, blocked]


def test_a_backend_build_failure_stays_an_image_build_error(monkeypatch, tmp_path):
    monkeypatch.setattr(generic_provider, "load_class", lambda _p: _FakeRaw)
    _FakeRaw.fail_build = True
    try:
        with pytest.raises(ImageBuildError, match="No matching distribution"):
            ModalProvider(SandboxProviderConfig(name="modal")).open_from_dockerfile(_ctx(tmp_path))
    finally:
        _FakeRaw.fail_build = False


# -- modal: content-keyed image cache + build log tail ------------------------------------------------------------

modal = pytest.importorskip("modal")
from sandboxes.backends import modal_raw  # noqa: E402
from sandboxes.raw import SandboxSpec  # noqa: E402


def test_modal_image_cache_is_keyed_by_content_not_path(monkeypatch, tmp_path):
    ctx = _ctx(tmp_path)
    built = []

    class FakeImage:
        def __init__(self, df):
            self.df = df
            self.build = type("B", (), {"aio": self._build})()

        async def _build(self, app):
            built.append(Path(self.df).read_text())
            return self

    modal_raw._images.clear()
    monkeypatch.setattr(modal.Image, "from_dockerfile", staticmethod(lambda df, **k: FakeImage(df)))
    spec = SandboxSpec(image="", extra={"dockerfile": str(ctx / "Dockerfile"), "context_dir": str(ctx)})

    asyncio.run(modal_raw._image(object(), spec))
    asyncio.run(modal_raw._image(object(), spec))
    assert len(built) == 1                                           # unchanged context: one build
    (ctx / "Dockerfile").write_text("FROM python:3.12-slim\n")       # a repair edits it in place
    asyncio.run(modal_raw._image(object(), spec))
    assert built == ["FROM python:3.11-slim\n", "FROM python:3.12-slim\n"]


def test_modal_build_failure_carries_the_build_log_tail(monkeypatch, tmp_path):
    ctx = _ctx(tmp_path)

    class FailingImage:
        build = type("B", (), {"aio": staticmethod(lambda app: _raise())})()

    async def _raise():
        raise modal.exception.ImageBuildError("Image build failed. View the build logs: modal image logs im-Abc123", "im-Abc123")

    class Entry:
        def __init__(self, m):
            self.message = m

    async def fetch(layers=None):
        for m in ("=> Step 1: RUN pip install nope\n", "[notice] A new release of pip\n",
                  "ERROR: No matching distribution found for nope\n"):
            yield Entry(m)

    seen = {}

    def from_id(i):
        seen["id"] = i
        return type("I", (), {"logs": type("L", (), {"fetch": type("F", (), {"aio": staticmethod(fetch)})()})()})()

    modal_raw._images.clear()
    monkeypatch.setattr(modal.Image, "from_dockerfile", staticmethod(lambda df, **k: FailingImage()))
    monkeypatch.setattr(modal.Image, "from_id", staticmethod(from_id))
    spec = SandboxSpec(image="", extra={"dockerfile": str(ctx / "Dockerfile"), "context_dir": str(ctx)})
    with pytest.raises(ImageBuildError) as ei:
        asyncio.run(modal_raw._image(object(), spec))
    msg = str(ei.value)
    assert seen["id"] == "im-Abc123"
    assert "No matching distribution found for nope" in msg and "[notice]" not in msg


# -- daytona: a harbor task dir around the context ---------------------------------------------------------------

def test_daytona_opens_the_task_dir_beside_the_context_or_a_throwaway_one(monkeypatch, tmp_path):
    from sandboxes.daytona_provider import DaytonaProvider
    seen = []

    def fake_open(self, image, **kw):
        task = Path(image)
        seen.append((task, sorted(x.name for x in task.iterdir()), (task / "environment" / "Dockerfile").read_text()))
        self._is_open = True

    monkeypatch.setattr(DaytonaProvider, "open", fake_open)
    harbor = tmp_path / "harbor"
    env = _ctx(harbor)
    (harbor / "task.toml").write_text("[environment]\ncpus = 2\n")
    p = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    p.open_from_dockerfile(env)
    assert seen[0][0] == harbor                                      # the real task: its task.toml applies

    bare = _ctx(tmp_path / "bare", name="ctx")
    q = DaytonaProvider(SandboxProviderConfig(name="daytona"))
    q.open_from_dockerfile(bare)
    task, names, dockerfile = seen[1]
    assert names == ["environment", "instruction.md", "task.toml"] and dockerfile == "FROM python:3.11-slim\n"
    q._is_open = False
    q.close()
    assert not task.exists()                                         # the throwaway task dir goes with the sandbox

    with pytest.raises(SandboxOpenError, match="Dockerfile only"):
        DaytonaProvider(SandboxProviderConfig(name="daytona")).open_from_dockerfile(bare, dockerfile="Other.Dockerfile")


def test_daytona_build_failure_becomes_an_image_build_error(monkeypatch, tmp_path):
    from sandboxes.daytona_provider import DaytonaProvider

    def fake_open(self, image, **kw):
        try:
            raise RuntimeError("Sandbox entered BUILD_FAILED state: pip install nope")
        except RuntimeError as e:
            raise SandboxOpenError(f"daytona sandbox start failed: {e}") from e

    monkeypatch.setattr(DaytonaProvider, "open", fake_open)
    with pytest.raises(ImageBuildError, match="pip install nope"):
        DaytonaProvider(SandboxProviderConfig(name="daytona")).open_from_dockerfile(_ctx(tmp_path))


# -- docker: real daemon -------------------------------------------------------------------------------------------

_docker_ok = shutil.which("docker") is not None and subprocess.run(["docker", "info"], capture_output=True, timeout=10).returncode == 0


@pytest.mark.skipif(not _docker_ok, reason="docker not available")
def test_docker_builds_once_per_content_and_releases(monkeypatch, tmp_path):
    ns = f"sbx-test-{uuid.uuid4().hex[:8]}"
    ctx = _ctx(tmp_path, "FROM python:3.11-slim\nCOPY app.py /app/app.py\n")
    cfg = SandboxProviderConfig(name="docker", extra={"image_namespace": ns})
    p = DockerProvider(cfg)
    p.open_from_dockerfile(ctx, workdir="/app", labels={"sbx.test": "1"})
    try:
        assert p.run_command("python app.py").output.strip() == "1"
    finally:
        p.close()

    def no_build(*a, **k):
        raise AssertionError("rebuilt an unchanged context")
    q = DockerProvider(cfg)
    monkeypatch.setattr(q, "build_sandbox_image", no_build)
    q.open_from_dockerfile(ctx)
    q.close()
    assert q.release_dockerfile_image(ctx)
    assert subprocess.run(["docker", "image", "inspect", f"{ns}:{context_digest(ctx)}"], capture_output=True).returncode != 0


@pytest.mark.skipif(not _docker_ok, reason="docker not available")
def test_docker_build_failure_is_an_image_build_error_with_output(tmp_path):
    ctx = _ctx(tmp_path, "FROM python:3.11-slim\nRUN echo marker-xyz && exit 3\n")
    p = DockerProvider(SandboxProviderConfig(name="docker", extra={"image_namespace": f"sbx-test-{uuid.uuid4().hex[:8]}"}))
    with pytest.raises(ImageBuildError, match="marker-xyz|exit code: 3"):
        p.open_from_dockerfile(ctx)


@pytest.mark.skipif(not _docker_ok, reason="docker not available")
def test_docker_joins_and_creates_the_configured_network(tmp_path):
    net = f"sbx-test-net-{uuid.uuid4().hex[:8]}"
    p = DockerProvider(SandboxProviderConfig(name="docker", extra={"network": net}))
    try:
        p.open("python:3.11-slim")
        r = subprocess.run(["docker", "inspect", "-f", "{{json .NetworkSettings.Networks}}", p.raw_env],
                           capture_output=True, text=True)
        assert net in r.stdout
    finally:
        p.close()
        subprocess.run(["docker", "network", "rm", net], capture_output=True)
