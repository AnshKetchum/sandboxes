"""Modal: sandboxes from a registry image. `runtime: vm` (the default here) is a real VM with its own Linux kernel and a large
root disk, which is what docker-in-docker needs; `gvisor` is Modal's cheaper-to-start default sandbox and cannot run a daemon.
Billed per requested core-second and GiB-second; the lifetime (`timeout`, up to 24 h) is enforced by Modal.

Credentials: MODAL_TOKEN_ID + MODAL_TOKEN_SECRET, or a profile in ~/.modal.toml (`modal token new`).
`cpu` is requested AND limited (a fixed allotment, so it compares with the other providers): measured, a core-limit of N gives
N processes' worth of CPU; a request without a limit lets a sandbox burst to ~4x. `mem_gb` becomes MiB. No per-sandbox disk setting.
"""

from __future__ import annotations

import asyncio

from ..errors import CapacityError
from ..raw import ExecResult, LiveSandbox, RawSandbox

_app = None
_app_lock = None
_images: dict = {}
APP_NAME = "sandboxes"


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
        out = []
        async for sb in modal.Sandbox.list.aio(app_id=(await _get_app()).app_id, tags={"sandboxes": "1"}):
            out.append(LiveSandbox("modal", sb.object_id, None, "", {"sandboxes": "1"}))      # Modal does not report a creation time
        return out

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
        img = _images.setdefault(s.image, modal.Image.from_registry(s.image))
        try:
            self._sb = await modal.Sandbox.create.aio(
                app=app, image=img, cpu=(float(s.cpu), float(s.cpu)), memory=int(s.mem_gb) * 1024, timeout=int(s.lifetime_s),
                runtime=s.extra.get("runtime", "vm"), tags={"sandboxes": "1", **{k: str(v) for k, v in (s.labels or {}).items()}},
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
