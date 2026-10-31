"""Daytona: a sandbox created from a registry image, own docker daemon via the boot command."""

from __future__ import annotations

import asyncio
import shlex
from typing import Optional

from ..errors import CapacityError
import datetime as dt

from ..raw import ExecResult, LiveSandbox, RawSandbox


def _is_capacity(msg: str) -> bool:
    m = msg.lower()
    return any(k in m for k in ("quota", "limit exceeded", "maximum", "too many", "429", "insufficient"))


from ..labels import is_ours                    # standard or legacy markers; never harbor.* (other projects in the org use those)


def _epoch(ts):
    try:
        t = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return (t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)).timestamp()
    except (TypeError, ValueError):
        return None


class DaytonaRaw(RawSandbox):
    provider = "daytona"
    hard_lifetime = False          # auto-stop is inactivity based: only the janitor is a hard cap

    @classmethod
    async def list_live(cls):
        from daytona import AsyncDaytona
        c, out = AsyncDaytona(), []
        try:
            res = c.list()
            items = []
            if hasattr(res, "__aiter__"):                      # the harbor-pinned sdk yields sandboxes
                async for sb in res:
                    items.append(sb)
            else:
                items = list((await res).items)
            for sb in items:
                labels = dict(getattr(sb, "labels", None) or {})
                if is_ours(labels):
                    out.append(LiveSandbox("daytona", sb.id, _epoch(getattr(sb, "created_at", None)), str(getattr(sb, "state", "")), labels))
        finally:
            await c.close()
        return out

    @classmethod
    async def kill(cls, sandbox_id):
        from daytona import AsyncDaytona
        c = AsyncDaytona()
        try:
            await c.delete(await c.get(sandbox_id))
        except Exception as e:  # noqa: BLE001
            if "not found" not in str(e).lower():
                raise
        finally:
            await c.close()

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
            labels={k: str(v) for k, v in (s.labels or {}).items()},          # standard labels from RawSandbox.start
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
