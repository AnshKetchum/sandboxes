"""modal 1.5 has no runtime= argument on Sandbox.create: the runtime comes from MODAL_FUNCTION_RUNTIME, which the backend sets."""
import asyncio

import pytest

modal = pytest.importorskip("modal")

from sandboxes.backends import modal_raw    # noqa: E402
from sandboxes.raw import SandboxSpec       # noqa: E402


def test_create_sets_the_runtime_and_passes_no_runtime_argument(monkeypatch):
    seen = {}

    class FakeSandbox:
        object_id = "sb-1"

    async def fake_create(**kw):
        seen.update(kw)
        return FakeSandbox()

    async def fake_app():
        return object()

    monkeypatch.delenv("MODAL_FUNCTION_RUNTIME", raising=False)
    monkeypatch.setattr(modal_raw, "_get_app", fake_app)
    monkeypatch.setattr(modal.Sandbox, "create", type("C", (), {"aio": staticmethod(fake_create)})())
    sbx = modal_raw.ModalRaw(SandboxSpec(image="python:3.12-slim", extra={"runtime": "vm"}))
    asyncio.run(sbx._create())
    assert "runtime" not in seen and sbx.sandbox_id == "sb-1"
    import os
    assert os.environ["MODAL_FUNCTION_RUNTIME"] == "vm"


def test_a_dockerfile_is_built_once_for_concurrent_sandboxes_and_a_failed_build_is_not_retried(monkeypatch, tmp_path):
    from sandboxes.errors import ImageBuildError
    (tmp_path / "Dockerfile").write_text("FROM debian:12\n")
    builds = []

    class FakeImage:
        def __init__(self, fail):
            self.fail = fail
            self.build = type("B", (), {"aio": self._build})()

        async def _build(self, app):
            builds.append(1)
            await asyncio.sleep(0.01)
            if self.fail:
                raise modal.exception.ImageBuildError("boom", "im-1")
            return self

    for fail in (False, True):
        builds.clear()
        modal_raw._images.clear()
        monkeypatch.setattr(modal.Image, "from_dockerfile", staticmethod(lambda *a, f=fail, **k: FakeImage(f)))
        spec = SandboxSpec(image="", extra={"dockerfile": str(tmp_path / "Dockerfile"), "context_dir": str(tmp_path)})

        async def three():
            return await asyncio.gather(*(modal_raw._image(object(), spec) for _ in range(3)), return_exceptions=True)
        res = asyncio.run(three())
        assert len(builds) == 1                                          # the 3 tries of a task share one build
        assert all(isinstance(r, ImageBuildError) for r in res) if fail else all(not isinstance(r, Exception) for r in res)


def test_modal_builds_dockerfiles_and_a_failed_build_is_not_retried_by_start(monkeypatch):
    from sandboxes.registry import PROVIDERS
    assert PROVIDERS["modal"].capabilities["builds_dockerfile"] and not PROVIDERS["modal"].capabilities["requires_prebuilt_image"]
    from sandboxes.errors import ImageBuildError
    calls = []

    async def _create(self):
        calls.append(1)
        raise ImageBuildError("nope")

    monkeypatch.setattr(modal_raw.ModalRaw, "_create", _create)
    import os
    os.environ["SANDBOX_COST_LEDGER"] = os.devnull
    with __import__("pytest").raises(ImageBuildError):
        asyncio.run(modal_raw.ModalRaw(SandboxSpec(image="x")).start())
    assert len(calls) == 1
