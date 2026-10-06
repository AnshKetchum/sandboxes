"""Daytona: a sandbox created from a registry image, own docker daemon via the boot command."""

from __future__ import annotations

import asyncio
import shlex
from typing import Optional

from ..errors import CapacityError
from ..raw import ExecResult, RawSandbox


def _is_capacity(msg: str) -> bool:
    m = msg.lower()
    return any(k in m for k in ("quota", "limit exceeded", "maximum", "too many", "429", "insufficient"))


class DaytonaRaw(RawSandbox):
    provider = "daytona"

    def __init__(self, spec, model=None):
        super().__init__(spec, model)
        self._client = None
        self._sb = None

    async def _create(self) -> None:
        from daytona import AsyncDaytona, CreateSandboxFromImageParams, Image, Resources

        s = self.spec
        self._client = AsyncDaytona()
        params = CreateSandboxFromImageParams(
            image=Image.base(s.image),
            resources=Resources(cpu=int(s.cpu), memory=int(s.mem_gb), disk=int(s.disk_gb)),
            env_vars={k: str(v) for k, v in (s.env or {}).items()},
            labels={"agentdist": "1", "sandboxes.managed": "true", **{k: str(v) for k, v in (s.labels or {}).items()}},
            auto_stop_interval=int(s.lifetime_s // 60),
            auto_delete_interval=int(s.lifetime_s // 60) + 60,
            network_block_all=False,
        )
        try:
            self._sb = await self._client.create(params, timeout=s.create_timeout)
        except Exception as e:  # noqa: BLE001
            try:
                await self._client.close()
            except Exception:  # noqa: BLE001
                pass
            if _is_capacity(str(e)):
                raise CapacityError("daytona", str(e)[:200]) from e
            raise
        self.sandbox_id = self._sb.id

    async def _exec(self, command, cwd, env, timeout) -> ExecResult:
        r = await self._sb.process.exec(f"bash -lc {shlex.quote(command)}", cwd=cwd,
                                        env={k: str(v) for k, v in (env or {}).items()} or None, timeout=int(timeout))
        return ExecResult(r.exit_code if r.exit_code is not None else -1, r.result or "")

    async def _upload(self, data: bytes, remote_path: str) -> None:
        await self._sb.fs.upload_file(data, remote_path)

    async def _download(self, remote_path: str) -> bytes:
        return await self._sb.fs.download_file(remote_path)

    async def _delete(self) -> None:
        try:
            await self._client.delete(self._sb)
        finally:
            try:
                await self._client.close()
            except Exception:  # noqa: BLE001
                pass
