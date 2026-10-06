"""E2B: a sandbox from a template that is built once per (image, cpu, memory) from the registry
image and reused. Commands run as root. Storage is free on E2B, concurrency is plan-capped (Pro: 100)."""

from __future__ import annotations

import asyncio
import hashlib
import shlex
import threading

from ..errors import CapacityError
from ..raw import ExecResult, LiveSandbox, RawSandbox

_template_locks: dict[str, asyncio.Lock] = {}
_guard = threading.Lock()


def template_alias(image: str, cpu: int, mem_gb: int) -> str:
    return "sbx-" + hashlib.sha1(f"{image}|{cpu}|{mem_gb}".encode()).hexdigest()[:12]


async def ensure_template(image: str, cpu: int, mem_gb: int, *, registry_auth: tuple = (None, None)) -> str:
    """Build (once) and return the E2B template alias for `image` at this size."""
    from e2b import AsyncTemplate, Template

    alias = template_alias(image, cpu, mem_gb)
    with _guard:
        lock = _template_locks.setdefault(alias, asyncio.Lock())
    async with lock:
        if await AsyncTemplate.alias_exists(alias):
            return alias
        user, pw = registry_auth
        tpl = Template().from_image(image, username=user, password=pw) if user else Template().from_image(image)
        try:
            await AsyncTemplate.build(tpl, alias, cpu_count=cpu, memory_mb=mem_gb * 1024)
        except Exception:  # noqa: BLE001 — another process may have built it first
            if not await AsyncTemplate.alias_exists(alias):
                raise
    return alias


class E2BRaw(RawSandbox):
    provider = "e2b"
    hard_lifetime = True           # `timeout` at create ends the sandbox

    @classmethod
    async def list_live(cls):
        from e2b import AsyncSandbox
        pag, out = AsyncSandbox.list(), []
        while True:
            for x in await pag.next_items():
                st = getattr(x, "started_at", None)
                out.append(LiveSandbox("e2b", x.sandbox_id, st.timestamp() if st else None, str(getattr(x, "state", "")), dict(getattr(x, "metadata", None) or {})))
            if not pag.has_next:
                return out

    @classmethod
    async def kill(cls, sandbox_id):
        from e2b import AsyncSandbox
        await AsyncSandbox.kill(sandbox_id)

    def __init__(self, spec, model=None):
        super().__init__(spec, model)
        self._sb = None

    async def _create(self) -> None:
        from e2b import AsyncSandbox, RateLimitException

        s = self.spec
        alias = s.extra.get("template") or await ensure_template(s.image, int(s.cpu), int(s.mem_gb))
        try:
            self._sb = await AsyncSandbox.create(
                alias, timeout=int(s.lifetime_s), envs={k: str(v) for k, v in (s.env or {}).items()} or None,
                metadata={k: str(v) for k, v in {"sandboxes": "1", **(s.labels or {})}.items()},
                request_timeout=s.create_timeout)
        except RateLimitException as e:
            raise CapacityError("e2b", str(e)[:200]) from e
        self.sandbox_id = self._sb.sandbox_id

    async def _exec(self, command, cwd, env, timeout) -> ExecResult:
        from e2b import CommandExitException

        try:
            r = await self._sb.commands.run(f"bash -lc {shlex.quote(command)}", user="root", cwd=cwd,
                                            envs={k: str(v) for k, v in (env or {}).items()} or None,
                                            timeout=float(timeout), request_timeout=float(timeout) + 30)
            return ExecResult(r.exit_code, (r.stdout or "") + (r.stderr or ""))
        except CommandExitException as e:
            return ExecResult(e.exit_code, (e.stdout or "") + (e.stderr or ""))

    async def _upload(self, data: bytes, remote_path: str) -> None:
        await self._sb.files.write(remote_path, data, user="root", request_timeout=360)

    async def _download(self, remote_path: str) -> bytes:
        return bytes(await self._sb.files.read(remote_path, format="bytes", user="root", request_timeout=360))

    async def _delete(self) -> None:
        await self._sb.kill()
