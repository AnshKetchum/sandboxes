"""Synchronous SandboxProvider for any provider that has a RawSandbox backend (E2B, Blaxel, ...).

`open(image)` takes a registry image reference (like DockerProvider takes a tag): the backend builds a
template/image from it once and starts sandboxes from that. Everything else is the standard
SandboxProvider surface; costs come from the provider's CostModel and land in the cost ledger.
"""

from __future__ import annotations

import asyncio
import atexit
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar, Optional, Union

from .base import SandboxProvider
from .errors import BuildError, CapacityError, PushError, SandboxNotOpenError, SandboxOpenError
from .models import BuildResult, CommandResult, DependencyEntry, PushResult, SandboxProviderConfig
from .pricing import cost_model_for
from .raw import RawSandbox, SandboxSpec
from .registry import PROVIDERS, load_class

_loop: Optional[asyncio.AbstractEventLoop] = None
_loop_lock = threading.Lock()
_OUTPUT_TAIL = 12000


def _shared_loop() -> asyncio.AbstractEventLoop:
    global _loop
    with _loop_lock:
        if _loop is None:
            loop = asyncio.new_event_loop()
            threading.Thread(target=lambda: (asyncio.set_event_loop(loop), loop.run_forever()),
                             name="sandboxes-raw-loop", daemon=True).start()
            _loop = loop
        return _loop


def run_sync(coro, timeout: Optional[float] = None):
    fut = asyncio.run_coroutine_threadsafe(coro, _shared_loop())
    try:
        return fut.result(timeout=timeout)
    except TimeoutError:
        fut.cancel()
        raise TimeoutError(f"operation exceeded its {timeout}s timeout (a timeout, not a network error)") from None


class RawBackedProvider(SandboxProvider):
    name: ClassVar[str] = "base"

    def __init__(self, config: Optional[SandboxProviderConfig] = None):
        super().__init__(config)
        self._raw: Optional[RawSandbox] = None
        self._workdir = "/"

    def _spec(self, image: str, name_prefix: Optional[str]) -> SandboxSpec:
        c = self.config
        extra = dict(c.extra or {})
        return SandboxSpec(
            image=c.image or image, cpu=int(c.cpu or 2), mem_gb=int((c.memory_mb or 4096) / 1024), disk_gb=int(c.disk_gb or 10),
            labels={"prefix": name_prefix} if name_prefix else {},
            boot_cmd=extra.pop("boot_cmd", ""),
            lifetime_s=int((c.auto_delete_minutes or 240) * 60), extra=extra)

    def open(self, image: str, *, workdir: str = "/", network: str = "bridge", volumes: Optional[dict] = None,
             name_prefix: Optional[str] = None) -> None:
        if self._is_open:
            raise SandboxOpenError("already open — call close() first")
        if volumes:
            raise SandboxOpenError(f"{self.name} does not support host bind mounts (volumes=); use put_file/get_file")
        cls = load_class(PROVIDERS[self.name].raw)
        raw = cls(self._spec(image, name_prefix), cost_model_for(self.name, self.config.pricing))
        try:
            run_sync(raw.start(), timeout=raw.spec.create_timeout * raw.spec.create_tries + 900)
        except CapacityError as e:
            raise SandboxOpenError(f"{self.name} is at capacity: {e}") from e
        except Exception as e:
            raise SandboxOpenError(f"{self.name} sandbox start failed: {e}") from e
        self._raw, self._workdir = raw, workdir or "/"
        self._is_open = True
        self._opened_at, self._closed_at = time.monotonic(), None

    def close(self) -> None:
        if not self._is_open:
            return
        try:
            run_sync(self._raw.stop(), timeout=400)
        except Exception:  # noqa: BLE001 — the provider-side lifetime is the backstop
            pass
        self._closed_at = time.monotonic()
        self._is_open = False

    def _require(self) -> RawSandbox:
        if not self._is_open or self._raw is None:
            raise SandboxNotOpenError("sandbox is not open — call open() first")
        return self._raw

    def put_file(self, local_path: Union[str, Path], remote_path: str, *, mode: str = "file",
                 permissions: Optional[str] = None) -> None:
        raw = self._require()
        run_sync(raw.put_file(local_path, remote_path), timeout=420)
        if permissions:
            self.run_command(f"chmod -R {permissions} '{remote_path}'")

    def get_file(self, remote_path: str, local_path: Union[str, Path]) -> None:
        run_sync(self._require().get_file(remote_path, local_path), timeout=960)

    def run_command(self, command: str, *, timeout: float = 300.0, cwd: Optional[str] = None) -> CommandResult:
        raw = self._require()
        r = run_sync(raw.exec(command, cwd=cwd or self._workdir, timeout=int(timeout)), timeout=timeout + 60)
        timed_out = r.rc == -1 and "timed out" in (r.stdout or "")
        out = r.stdout or ""
        return CommandResult(exit_code=None if timed_out else r.rc, output=out if len(out) <= _OUTPUT_TAIL else out[-_OUTPUT_TAIL:],
                             timed_out=timed_out)

    def add_dependency(self, command: str, *, note: str = "") -> None:
        self.run_command(command)
        self._dependencies.append(DependencyEntry(step=len(self._dependencies), command=command, note=note,
                                                  added_at=datetime.now(timezone.utc).isoformat()))

    def build_sandbox_image(self, context_dir, *, tag, timeout=1800.0, dockerfile=None) -> BuildResult:
        return BuildResult(ok=False, image=tag, detail=f"{self.name} does not build images from a context; build with the "
                           "docker provider, push to a registry, and open() the registry reference")

    def push_sandbox_image(self, tag, *, registry=None, timeout=600.0) -> PushResult:
        return PushResult(ok=False, image=tag, detail=f"{self.name} has no image push; use the docker provider")

    def get_total_runtime_cost(self) -> float:
        if self._opened_at is None:
            return 0.0
        c = self.config
        m = cost_model_for(self.name, c.pricing)
        return round(m.sandbox_usd(float(c.cpu or 2), (c.memory_mb or 4096) / 1024.0, float(c.disk_gb or 10),
                                   self.get_elapsed_seconds()), 6)


class E2BProvider(RawBackedProvider):
    name: ClassVar[str] = "e2b"


class BlaxelProvider(RawBackedProvider):
    name: ClassVar[str] = "blaxel"
