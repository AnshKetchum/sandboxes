"""A harbor environment for ANY provider that has a RawSandbox backend (registry.PROVIDERS[name].raw).

harbor ships native environments for only some providers (daytona, e2b, blaxel). This class lets the rest (fly, and whatever is
added next) run harbor tasks without hand-writing a harbor environment per provider: it opens the task's PREBUILT image
(`task_env_config.docker_image`) as a raw sandbox and maps harbor's exec / upload / download onto the raw backend.

Limit: no Dockerfile builds. A task without a `docker_image` is rejected at construction, so the caller can pick another provider.
Cost accounting is the RawSandbox's own (it opens/closes the ledger entry in start()/stop()), so `LEDGER_NATIVE` tells
harbor_adapter / euphoria not to count the sandbox a second time.

    cls = environment_class("fly")             # a BaseEnvironment subclass bound to that provider
    env = cls(environment_dir=..., ..., sandbox_extra={"region": "iad"}, lifetime_s=4 * 3600)
"""

from __future__ import annotations

import math
import shlex
from pathlib import Path
from typing import Optional

from harbor.environments.base import BaseEnvironment, ExecResult
from harbor.environments.capabilities import EnvironmentCapabilities, EnvironmentResourceCapabilities
from harbor.environments.definition import effective_exec_cwd

from .raw import SandboxSpec
from .registry import PROVIDERS, load_class


class RawSandboxEnvironment(BaseEnvironment):
    PROVIDER: str = ""                 # set on the per-provider subclass
    LEDGER_NATIVE = True               # the RawSandbox records its own cost ledger entry

    def __init__(self, *args, sandbox_extra: Optional[dict] = None, lifetime_s: Optional[int] = None,
                 labels: Optional[dict] = None, **kwargs):
        self._sandbox_extra = dict(sandbox_extra or {})
        self._lifetime_s = lifetime_s
        self._labels = dict(labels or {})
        self._raw = None
        super().__init__(*args, **kwargs)

    @staticmethod
    def type() -> str:
        return "sandboxes-raw"

    @classmethod
    def resource_capabilities(cls) -> EnvironmentResourceCapabilities:
        return EnvironmentResourceCapabilities(cpu_request=True, memory_request=True)

    @property
    def capabilities(self) -> EnvironmentCapabilities:
        return EnvironmentCapabilities()

    def _validate_definition(self):
        if not self.task_env_config.docker_image:
            raise RuntimeError(f"{self.PROVIDER}: a raw sandbox runs a prebuilt image, and this task has no docker_image "
                               f"(Dockerfile builds are not supported on this provider)")

    # -- lifecycle ----------------------------------------------------------------------------------------
    async def start(self, force_build: bool):
        cfg = self.task_env_config
        spec = SandboxSpec(
            image=cfg.docker_image,
            cpu=int(self._effective_cpus or 1),
            mem_gb=max(1, math.ceil((self._effective_memory_mb or 2048) / 1024)),
            disk_gb=max(1, math.ceil((self._effective_storage_mb or 8192) / 1024)),
            env=dict(self._startup_env()),
            labels=dict(self._labels),
            extra={k: v for k, v in self._sandbox_extra.items() if k != "docker_loop_gb"},   # no docker daemon in a harbor task's sandbox
            **({"lifetime_s": int(self._lifetime_s)} if self._lifetime_s else {}),
        )
        self._raw = load_class(PROVIDERS[self.PROVIDER].raw)(spec)
        await self._raw.start()
        await self.ensure_dirs(self._mount_targets(writable_only=True))
        await self._upload_environment_dir_after_start()

    async def stop(self, delete: bool):
        raw, self._raw = self._raw, None
        if raw is not None:
            await raw.stop()

    @property
    def sandbox_id(self) -> str:
        return getattr(self._raw, "sandbox_id", "") or ""

    def _require(self):
        if self._raw is None:
            raise RuntimeError("Sandbox not found. Please start the environment first.")
        return self._raw

    # -- files --------------------------------------------------------------------------------------------
    async def upload_file(self, source_path: Path | str, target_path: str):
        await self._require().put_file(source_path, target_path)

    async def upload_dir(self, source_dir: Path | str, target_dir: str):
        await self._require().put_file(source_dir, target_dir)       # a directory's CONTENTS land in target_dir

    async def download_file(self, source_path: str, target_path: Path | str):
        await self._require().get_file(source_path, target_path)

    async def download_dir(self, source_dir: str, target_dir: Path | str):
        await self._require().get_file(source_dir, target_dir)

    # -- exec ---------------------------------------------------------------------------------------------
    @staticmethod
    def _as_user(command: str, user: str | int | None) -> str:
        if user is None or str(user) in ("root", "0"):
            return command
        who = shlex.quote(str(user))
        name = f'"$(getent passwd {who} | cut -d: -f1)"' if str(user).isdigit() else who
        return f"su {name} -s /bin/bash -c {shlex.quote(command)}"

    async def exec(self, command: str, cwd: str | None = None, env: dict[str, str] | None = None,
                   timeout_sec: int | None = None, user: str | int | None = None) -> ExecResult:
        raw = self._require()
        user = self._resolve_user(user)
        r = await raw.exec(self._as_user(command, user), cwd=effective_exec_cwd(cwd, self.task_env_config.workdir, None),
                           env=self._merge_env(env), timeout=timeout_sec)
        return ExecResult(stdout=r.stdout, stderr="", return_code=r.rc)


def environment_class(provider: str):
    """The harbor environment class for `provider`'s raw backend (one class per provider, cached)."""
    info = PROVIDERS.get(provider)
    if info is None or not info.raw:
        raise KeyError(f"{provider!r} has no raw sandbox backend, so no harbor environment can be built over it")
    name = f"{provider.capitalize()}RawEnvironment"
    cls = _CLASSES.get(provider)
    if cls is None:
        cls = _CLASSES[provider] = type(name, (RawSandboxEnvironment,), {"PROVIDER": provider, "__module__": __name__})
    return cls


_CLASSES: dict = {}
