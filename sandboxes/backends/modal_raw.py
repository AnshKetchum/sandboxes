"""Modal: sandboxes from a registry image. `runtime: vm` (the default here) is a real VM with its own Linux kernel and a large
root disk, which is what docker-in-docker needs; `gvisor` is Modal's cheaper-to-start default sandbox and cannot run a daemon.
Billed per requested core-second and GiB-second; the lifetime (`timeout`, up to 24 h) is enforced by Modal.

Credentials: MODAL_TOKEN_ID + MODAL_TOKEN_SECRET, or a profile in ~/.modal.toml (`modal token new`).
`cpu` is requested AND limited (a fixed allotment, so it compares with the other providers): measured, a core-limit of N gives
N processes' worth of CPU; a request without a limit lets a sandbox burst to ~4x. `mem_gb` becomes MiB. No per-sandbox disk setting.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from ..errors import CapacityError, ImageBuildError
from ..raw import ExecResult, LiveSandbox, RawSandbox

_app = None
_app_lock = None
_images: dict = {}          # image key -> the Task that builds (or just resolves) it, shared by every sandbox of that image in this process
APP_NAME = "sandboxes"


async def _image(app, spec):
    """The modal Image for `spec`: its registry image, or built from `extra["dockerfile"]` (+ `extra["context_dir"]`). Built once per
    process however many sandboxes want it (the 3 tries of a task), and a failed build is remembered, so siblings fail at once."""
    import modal
    dockerfile = spec.extra.get("dockerfile")
    key = f"{dockerfile}|{spec.extra.get('context_dir')}" if dockerfile else spec.image
    if key not in _images:
        if dockerfile:
            image = modal.Image.from_dockerfile(str(dockerfile), context_dir=str(spec.extra.get("context_dir") or Path(dockerfile).parent))

            async def build():
                try:
                    return await image.build.aio(app)
                except modal.exception.ImageBuildError as e:
                    raise ImageBuildError(f"modal: image build failed for {dockerfile}: {str(e)[:300]}") from e
            _images[key] = asyncio.ensure_future(build())
        else:
            fut = asyncio.get_running_loop().create_future()
            fut.set_result(modal.Image.from_registry(spec.image))
            _images[key] = fut
    return await asyncio.shield(_images[key])


async def _get_app():
    import modal
    global _app, _app_lock
    if _app_lock is None:
        _app_lock = asyncio.Lock()
    async with _app_lock:
        if _app is None:
            _app = await modal.App.lookup.aio(APP_NAME, create_if_missing=True)
    return _app


def _is_capacity(e: Exception) -> bool:
    m = (type(e).__name__ + " " + str(e)).lower()
    return "resourceexhausted" in m or "resource_exhausted" in m or "limit" in m and "container" in m or "too many" in m or "quota" in m


class ModalRaw(RawSandbox):
    provider = "modal"
    hard_lifetime = True           # `timeout` at create ends the sandbox

    def __init__(self, spec, model=None):
        super().__init__(spec, model)
        self._sb = None

    @classmethod
    async def list_live(cls):
        import modal
        boxes = []
        async for sb in modal.Sandbox.list.aio(app_id=(await _get_app()).app_id, tags={"sandboxes": "1"}):
            boxes.append(sb)
        sem = asyncio.Semaphore(20)

        async def tags_of(sb):
            async with sem:
                try:
                    return dict(await sb.get_tags.aio())          # the standard owner/run labels travel as Modal tags
                except Exception:  # noqa: BLE001
                    return {"sandboxes": "1"}
        tags = await asyncio.gather(*(tags_of(sb) for sb in boxes))
        return [LiveSandbox("modal", sb.object_id, None, "", t) for sb, t in zip(boxes, tags)]      # Modal does not report a creation time

    @classmethod
    async def kill(cls, sandbox_id):
        import modal
        try:
            await (await modal.Sandbox.from_id.aio(sandbox_id)).terminate.aio()
        except Exception as e:  # noqa: BLE001
            if "not found" not in str(e).lower() and "terminated" not in str(e).lower():
                raise

    async def _create(self) -> None:
        import modal
        s = self.spec
        app = await _get_app()
        img = await _image(app, s)
        # modal 1.5 has no `runtime=` argument: the sandbox runtime is the process-wide MODAL_FUNCTION_RUNTIME setting (vm = a real VM
        # with its own kernel and a large root disk; unset = gvisor). One runtime per process, which is how every config here uses it.
        modal.config.config.override_locally("function_runtime", s.extra.get("runtime", "vm"))
        try:
            self._sb = await modal.Sandbox.create.aio(
                app=app, image=img, cpu=(float(s.cpu), float(s.cpu)), memory=int(s.mem_gb) * 1024, timeout=int(s.lifetime_s),
                tags={"sandboxes": "1", **{k: str(v) for k, v in (s.labels or {}).items()}},
                env={k: str(v) for k, v in (s.env or {}).items()} or None, region=s.extra.get("region"))
        except Exception as e:  # noqa: BLE001
            if _is_capacity(e):
                raise CapacityError("modal", str(e)[:200]) from e
            raise
        self.sandbox_id = self._sb.object_id

    async def _exec(self, command, cwd, env, timeout) -> ExecResult:
        p = await self._sb.exec.aio("bash", "-lc", command, workdir=cwd, env={k: str(v) for k, v in (env or {}).items()} or None, timeout=int(timeout))
        out, err = await asyncio.gather(p.stdout.read.aio(), p.stderr.read.aio())
        await p.wait.aio()
        return ExecResult(p.returncode if p.returncode is not None else -1, (out or "") + (err or ""))

    async def _upload(self, data: bytes, remote_path: str) -> None:
        await self._sb.filesystem.write_bytes.aio(data, remote_path)

    async def _download(self, remote_path: str) -> bytes:
        return await self._sb.filesystem.read_bytes.aio(remote_path)

    async def _delete(self) -> None:
        await self._sb.terminate.aio()
