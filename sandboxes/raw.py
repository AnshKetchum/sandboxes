"""Raw async sandboxes: one tiny interface every provider implements.

    sbx = await open_raw("e2b", SandboxSpec(image="registry/…:latest", cpu=2, mem_gb=4, boot_cmd="dind-start.sh"))
    r = await sbx.exec("docker info", timeout=60)       # r.rc, r.stdout
    await sbx.put_file("/local/dir", "/work/dir")       # file or directory, both ways
    await sbx.get_file("/work/out", "/local/out")
    await sbx.stop()                                    # deletes it and records its cost

This is the layer callers with their own async code (agentdist's executors, euphoria's job
bodies) use. `SandboxProvider` (sandboxes.base) is the synchronous, one-sandbox-per-object API on
top of it. Providers only implement five primitives (`_create`, `_exec`, `_upload`, `_download`,
`_delete`); directories, retries, the boot command, cost bookkeeping and lifetimes live here.
"""

from __future__ import annotations

import asyncio
import io
import os
import shlex
import tarfile
import tempfile
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Optional

from .cost import get_ledger
from .errors import CapacityError, SandboxCreateError, SandboxError
from .pricing import CostModel, cost_model_for


@dataclass
class SandboxSpec:
    image: str                              # registry reference the provider can pull
    cpu: int = 2
    mem_gb: int = 4
    disk_gb: int = 10
    env: dict = field(default_factory=dict)
    labels: dict = field(default_factory=dict)
    boot_cmd: str = ""                      # run once after creation (e.g. dind-start.sh)
    boot_timeout: int = 300
    lifetime_s: int = 4 * 3600              # hard stop the provider enforces if we never delete
    create_timeout: int = 300
    create_tries: int = 4
    extra: dict = field(default_factory=dict)   # provider-specific knobs (region, template, ...)


@dataclass
class ExecResult:
    rc: int
    stdout: str = ""


class RawSandbox(ABC):
    provider: ClassVar[str] = "base"

    def __init__(self, spec: SandboxSpec, model: Optional[CostModel] = None):
        self.spec = spec
        self.model = model or cost_model_for(self.provider)
        self.sandbox_id: str = ""
        self.env_id = uuid.uuid4().hex
        self._ledger_handle: Optional[str] = None
        self.cost_usd = 0.0

    # -- provider primitives ----------------------------------------------------------------------
    @abstractmethod
    async def _create(self) -> None:
        """Create the sandbox; set self.sandbox_id. Raise CapacityError when the provider is full."""

    @abstractmethod
    async def _exec(self, command: str, cwd: Optional[str], env: Optional[dict], timeout: int) -> ExecResult:
        """Run `bash -lc command` as root. Never raises for a non-zero exit."""

    @abstractmethod
    async def _upload(self, data: bytes, remote_path: str) -> None: ...

    @abstractmethod
    async def _download(self, remote_path: str) -> bytes: ...

    @abstractmethod
    async def _delete(self) -> None: ...

    # -- lifecycle ----------------------------------------------------------------------------------
    async def start(self) -> "RawSandbox":
        last: Optional[Exception] = None
        for attempt in range(self.spec.create_tries):
            try:
                await self._create()
                break
            except CapacityError:
                raise                       # the router decides what to do about a full provider
            except Exception as e:  # noqa: BLE001 — provider flakes are retried
                last = e
                await asyncio.sleep(10 * (attempt + 1))
        else:
            raise SandboxCreateError(f"{self.provider}: sandbox creation failed after "
                                     f"{self.spec.create_tries} tries: {last}")
        s = self.spec
        self._ledger_handle = get_ledger().open(self.provider, cpu=s.cpu, ram_gib=s.mem_gb, disk_gib=s.disk_gb,
                                                labels=s.labels, sandbox_id=self.sandbox_id)
        try:
            if s.boot_cmd:
                r = await self.exec(s.boot_cmd, timeout=s.boot_timeout)
                if r.rc != 0:
                    raise SandboxCreateError(f"{self.provider}: boot command failed (rc={r.rc}): {r.stdout[-1500:]}")
        except BaseException:
            await self.stop(sync_dirs=())
            raise
        return self

    async def stop(self, sync_dirs: tuple = ()) -> float:
        """Delete the sandbox and record its cost. `sync_dirs`: ((sandbox_dir, local_dir), ...) copied
        out first. Returns the dollar cost of this sandbox."""
        if not self.sandbox_id:
            return self.cost_usd
        try:
            for sb_dir, local_dir in sync_dirs:
                try:
                    await self.get_file(sb_dir, local_dir)
                except Exception:  # noqa: BLE001 — losing the sync-back must not leak the sandbox
                    pass
        finally:
            try:
                await asyncio.wait_for(self._delete(), timeout=300)
            except Exception:  # noqa: BLE001 — the provider-side lifetime is the backstop
                pass
            if self._ledger_handle:
                self.cost_usd = get_ledger().close(self._ledger_handle, self.model)
                self._ledger_handle = None
            self.sandbox_id = ""
        return self.cost_usd

    # -- exec -------------------------------------------------------------------------------------------
    async def exec(self, command: str, *, cwd: Optional[str] = None, env: Optional[dict] = None,
                   timeout: Optional[int] = None) -> ExecResult:
        try:
            return await self._exec(command, cwd, env, int(timeout or 7200))
        except CapacityError:
            raise
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if "timeout" in msg.lower() or "timed out" in msg.lower():
                msg = f"Execution timed out after {timeout} seconds."
            return ExecResult(-1, msg)

    # -- files (file or directory) ----------------------------------------------------------------------
    async def put_file(self, local: "str | Path", remote: str) -> None:
        src = Path(local)
        if src.is_dir():
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as tar:
                tar.add(str(src), arcname=".")
            tmp = f"/tmp/.sbx-{uuid.uuid4().hex}.tar"
            await self._upload(buf.getvalue(), tmp)
            r = await self.exec(f"mkdir -p {shlex.quote(remote)} && tar -xf {tmp} -C {shlex.quote(remote)} && rm -f {tmp}", timeout=600)
            if r.rc != 0:
                raise SandboxError(f"{self.provider}: upload of directory {src} failed: {r.stdout[-500:]}")
            return
        r = await self.exec(f"mkdir -p {shlex.quote(str(Path(remote).parent))}", timeout=60)
        if r.rc != 0:
            raise SandboxError(f"{self.provider}: mkdir failed for {remote}: {r.stdout[-500:]}")
        await self._upload(src.read_bytes(), remote)
        if os.access(src, os.X_OK):
            await self.exec(f"chmod +x {shlex.quote(remote)}", timeout=60)

    async def get_file(self, remote: str, local: "str | Path") -> None:
        dst = Path(local)
        probe = await self.exec(f"test -d {shlex.quote(remote)} && echo DIR || (test -e {shlex.quote(remote)} && echo FILE || echo MISSING)", timeout=60)
        kind = (probe.stdout or "").strip().splitlines()[-1] if probe.stdout else "MISSING"
        if kind == "MISSING":
            raise SandboxError(f"{self.provider}: {remote} not found in the sandbox")
        dst.parent.mkdir(parents=True, exist_ok=True)
        if kind == "DIR":
            tmp = f"/tmp/.sbx-{uuid.uuid4().hex}.tar"
            r = await self.exec(f"tar -cf {tmp} -C {shlex.quote(remote)} .", timeout=900)
            if r.rc != 0:
                raise SandboxError(f"{self.provider}: tar of {remote} failed: {r.stdout[-500:]}")
            data = await self._download(tmp)
            await self.exec(f"rm -f {tmp}", timeout=60)
            dst.mkdir(parents=True, exist_ok=True)
            with tarfile.open(fileobj=io.BytesIO(data), mode="r") as tar:
                tar.extractall(str(dst))
            return
        dst.write_bytes(await self._download(remote) or b"")
