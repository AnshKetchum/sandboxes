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
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Optional

from .cost import get_ledger
from .errors import CapacityError, ImageBuildError, SandboxCreateError, SandboxError
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

    def __post_init__(self):
        from .lifetime import clamp_lifetime_min
        self.lifetime_s = clamp_lifetime_min(self.lifetime_s // 60) * 60      # always finite, never above the ceiling


@dataclass
class LiveSandbox:
    """One sandbox that exists on a provider right now (what `RawSandbox.list_live` returns)."""
    provider: str
    id: str
    created_at: Optional[float] = None          # epoch seconds
    state: str = ""
    labels: dict = field(default_factory=dict)


@dataclass
class StoredArtifact:
    """Billed storage a provider holds for us (what `RawSandbox.stored_artifacts` returns): an image, snapshot or volume."""
    provider: str
    kind: str                                    # "image" | "snapshot" | "volume": the keys of CostModel.storage_usd
    id: str
    gib: float
    created_at: Optional[float] = None


@dataclass
class ExecResult:
    rc: int
    stdout: str = ""


class RawSandbox(ABC):
    """What a provider implements. Cost tracking and automatic cost cutting are first class here: a provider is not
    complete until it can say what it bills for (`cost_model`), what exists right now (`list_live`), how to end one (`kill`),
    and whether it ends sandboxes by itself at their lifetime (`hard_lifetime`). Everything generic in the library (the
    uniform cost ledger, caps, the guard, the janitor, ledger reconciliation) is built only on these.

    Subclass contract (checked when the class is defined):
      provider        short name, the key in registry.PROVIDERS and in config `name:`
      hard_lifetime   True if the provider itself ends a sandbox at its lifetime (E2B timeout, Blaxel ttl); False if it
                      only stops idle ones (Daytona), in which case the janitor is the only hard cap
      _create/_exec/_upload/_download/_delete   per-sandbox primitives
      list_live/kill  fleet operations (classmethods, no instance needed)
      stored_artifacts  optional: billed storage (images, snapshots, volumes) the provider holds for us
      cost_model      optional override; the default comes from pricing.DEFAULT_MODELS[provider]
    """
    provider: ClassVar[str] = "base"
    hard_lifetime: ClassVar[bool] = False
    max_call_s: ClassVar[Optional[int]] = None      # per-call time cap of the provider's exec API; longer commands run detached and are polled

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        if "provider" not in cls.__dict__ and not getattr(cls, "__abstractmethods__", None):
            raise TypeError(f"{cls.__name__} must define `provider`")
        if cls.__dict__.get("provider") and "hard_lifetime" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must declare `hard_lifetime` (does the provider end sandboxes at their lifetime itself?)")

    # -- fleet interface (class level) -----------------------------------------------------------------
    @classmethod
    @abstractmethod
    async def list_live(cls) -> "list[LiveSandbox]":
        """Every sandbox of OURS that exists now. On an account shared with other projects return only ours (by label)."""

    @classmethod
    @abstractmethod
    async def kill(cls, sandbox_id: str) -> None:
        """End one sandbox by id. Must not raise if it is already gone."""

    @classmethod
    async def stored_artifacts(cls) -> "list[StoredArtifact]":
        """Billed storage held now. Default: none (E2B storage is free)."""
        return []

    @classmethod
    async def meter(cls, seconds: float = 0.0) -> float:
        """Put costs that no sandbox entry carries into the ledger (a provider that bills machines, not sandboxes: GKE's nodes).
        Called on the guard / janitor cadence; returns USD added. Default: nothing to add."""
        return 0.0

    @classmethod
    async def housekeep(cls) -> dict:
        """Provider-specific cleanup the generic janitor cannot do (GKE: queued deletes, orphaned pods, idle node pools).
        Called on the janitor cadence; returns a report. Default: nothing to do."""
        return {}

    @classmethod
    def cost_model(cls, overrides: Optional[dict] = None) -> CostModel:
        return cost_model_for(cls.provider, overrides)

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
        from .labels import standard_labels
        self.spec.labels = standard_labels(extra=self.spec.labels)       # managed + owner + run, whatever the provider calls labels
        from .guard import check_admission
        check_admission(self.provider, provider_cap_usd=self.spec.extra.get("spend_cap_usd"))      # SpendCapExceeded before anything is created
        last: Optional[Exception] = None
        for attempt in range(self.spec.create_tries):
            try:
                from .ratelimit import acquire_create
                await acquire_create(self.provider)             # the provider's creation rate limit, shared by every process on this host
                await self._create()
                break
            except (CapacityError, ImageBuildError) as e:
                if isinstance(e, CapacityError) and "rate limit" in str(e).lower():     # throttled, not full: back off and try again
                    last = e
                    await asyncio.sleep(2 + 3 * attempt)
                    continue
                if isinstance(e, ImageBuildError):
                    from .health import record
                    record(self.provider, False)
                raise                       # the router decides what to do about a full provider; a failed image build fails again
            except Exception as e:  # noqa: BLE001 — provider flakes are retried
                last = e
                await asyncio.sleep(10 * (attempt + 1))
        else:
            from .health import record
            record(self.provider, False)
            raise SandboxCreateError(f"{self.provider}: sandbox creation failed after "
                                     f"{self.spec.create_tries} tries: {last}")
        from .health import record
        record(self.provider, True)             # the circuit breaker (health.py) watches how starts go on each provider
        s = self.spec
        self._ledger_handle = get_ledger().open(self.provider, cpu=s.cpu, ram_gib=s.mem_gb, disk_gib=s.disk_gb,
                                                labels=s.labels, sandbox_id=self.sandbox_id,
                                                usd_per_hour=self.model.hourly_usd(s.cpu, s.mem_gb, s.disk_gb))
        try:
            loop_gb = int(s.extra.get("docker_loop_gb") or 0)
            if loop_gb:      # docker's overlay2 cannot sit on an overlay root (blaxel, fly): give it an ext4 loop file instead
                r = await self.exec(f"truncate -s {loop_gb}G /mnt/docker.img && mkfs.ext4 -q -F /mnt/docker.img && mkdir -p /var/lib/docker && "
                                    f"mount -o loop /mnt/docker.img /var/lib/docker", timeout=120)
                if r.rc != 0:
                    raise SandboxCreateError(f"{self.provider}: docker loop setup failed (rc={r.rc}): {r.stdout[-300:]}")
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
            t = int(timeout or 7200)
            if self.max_call_s and t > self.max_call_s:
                return await self._exec_detached(command, cwd, env, t)
            return await self._exec(command, cwd, env, t)
        except CapacityError:
            raise
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if "timeout" in msg.lower() or "timed out" in msg.lower():
                msg = f"Execution timed out after {timeout} seconds."
            return ExecResult(-1, msg)

    async def _exec_detached(self, command: str, cwd: Optional[str], env: Optional[dict], timeout: int) -> ExecResult:
        """For providers whose exec API caps one call (Fly ~60 s): upload the command as a script, run it in the background,
        poll for its exit code, return its output."""
        tag = uuid.uuid4().hex[:10]; script, out, rcf = f"/tmp/.x-{tag}.sh", f"/tmp/.x-{tag}.out", f"/tmp/.x-{tag}.rc"
        body = "".join(f"export {k}={shlex.quote(str(v))}\n" for k, v in (env or {}).items()) + (f"cd {shlex.quote(cwd)} || exit 1\n" if cwd else "") + command + "\n"
        await self._upload(body.encode(), script)
        r = await self._exec(f"nohup sh -c 'bash -l {script} > {out} 2>&1; echo $? > {rcf}' >/dev/null 2>&1 &", None, None, 20)
        if r.rc != 0:
            return ExecResult(r.rc, "could not start the detached command: " + r.stdout[-300:])
        deadline = time.time() + timeout
        while time.time() < deadline:
            await asyncio.sleep(3)
            r = await self._exec(f"cat {rcf} 2>/dev/null", None, None, 20)
            if r.stdout.strip().lstrip("-").isdigit():
                rc = int(r.stdout.strip())
                o = await self._exec(f"tail -c 200000 {out}; rm -f {out} {rcf} {script}", None, None, 30)
                return ExecResult(rc, o.stdout)
        await self._exec(f"pkill -f {script}; true", None, None, 20)
        return ExecResult(-1, f"Execution timed out after {timeout} seconds.")

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
