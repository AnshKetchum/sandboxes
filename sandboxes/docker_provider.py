"""DockerProvider — local docker daemon backend. The default provider: free,
no metering, no external account needed, and the fallback every codebase
already falls back to when a remote backend degrades.

Ported faithfully from the equivalent logic proven in production across
euphoria (`euphoria/api.py::TaskSandbox`, `_open_task_sandbox_docker`,
`build.py`) and greenhouse (`agent/toolimpl/sandbox.py::DockerSandbox`) —
same retry-on-image-cache-race handling, same root-chmod-after-cp workaround
for docker cp always writing root:root, same tail-truncated exec output cap.
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar, Optional, Union

from .base import SandboxProvider
from .errors import BuildError, PushError, SandboxNotOpenError, SandboxOpenError
from .models import BuildResult, CommandResult, DependencyEntry, PushResult, SandboxProviderConfig

# A container-start error that means the image vanished between build and
# run (a concurrent `docker image prune` race), not a real failure — worth
# one rebuild-and-retry rather than surfacing immediately. Kept in sync with
# euphoria's own `_IMAGE_GONE_RE` (euphoria/api.py) by inspection, not import
# (this package has no euphoria dependency).
_IMAGE_GONE_RE = re.compile(
    r"Unable to find image|No such image|pull access denied|"
    r"repository .* does not exist|manifest unknown|not found: manifest|"
    r"parent snapshot .* does not exist|snapshot .* does not exist",
    re.I,
)

_OUTPUT_TAIL = 12000


def _tail(text: str, n: int = _OUTPUT_TAIL) -> str:
    return text if len(text) <= n else text[-n:]


def _docker(*args: str, timeout: float) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)


def _sanitize_name_prefix(prefix: str, max_len: int = 100) -> str:
    """A docker container name must match [a-zA-Z0-9][a-zA-Z0-9_.-]* — collapse
    everything else to '-' and trim, so a caller can pass e.g. a run id
    (already safe) or something messier without open() ever rejecting it."""
    s = re.sub(r"[^A-Za-z0-9_.-]", "-", prefix).strip("-")[:max_len]
    return s or "sbx"


class DockerProvider(SandboxProvider):
    name: ClassVar[str] = "docker"

    def __init__(self, config: Optional[SandboxProviderConfig] = None):
        super().__init__(config)
        self._container_name: Optional[str] = None
        self._workdir: str = "/"

    # -- lifecycle ------------------------------------------------------

    def open(
        self,
        image: str,
        *,
        workdir: str = "/",
        network: str = "bridge",
        volumes: Optional[dict[str, str]] = None,
        name_prefix: Optional[str] = None,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """`image` is an already-built (or pre-pulled) image tag. Starts a
        long-lived `sleep infinity` container from it, retrying up to 3x on
        a known image-cache-race error (a concurrent prune evicted the tag
        between caller-side checks and this call). `volumes` are bind
        mounts: {host_path: "container_path[:mode]"}, mode "rw" if omitted —
        for a workspace an agent writes to directly through the host
        filesystem, without a put_file/get_file round trip per write.
        `name_prefix` (e.g. a run id) becomes the container's own name
        prefix instead of the default "sbx" — so a caller can later find/
        kill everything from one job via `docker ps --filter name=<prefix>`
        without a separate label mechanism."""
        if self._is_open:
            raise SandboxOpenError("already open — call close() first")
        prefix = _sanitize_name_prefix(name_prefix) if name_prefix else "sbx"
        name = f"{prefix}-" + re.sub(r"[^A-Za-z0-9_.-]", "-", uuid.uuid4().hex[:12])
        cfg = self.config
        args = ["run", "-d", "--name", name, "--network", network]
        # `labels` (e.g. {"greenhouse": "1", "greenhouse.run": "<run id>"}) let a
        # caller find exactly its own containers on a host shared with other
        # agents: `docker ps --filter label=greenhouse.run=<run id>`.
        from .labels import standard_labels
        std = standard_labels(run=name_prefix if name_prefix and not os.environ.get("SANDBOX_RUN_ID") else None, extra=labels)
        for k, v in std.items():
            args += ["--label", f"{k}={v}"]
        if cfg.cpu:
            args += ["--cpus", str(cfg.cpu)]
        if cfg.memory_mb:
            args += ["--memory", f"{cfg.memory_mb}m"]
        if cfg.pids_limit:
            args += ["--pids-limit", str(cfg.pids_limit)]
        for host_path, spec in (volumes or {}).items():
            container_path, _, mode = spec.partition(":")
            mode = mode or "rw"
            args += ["-v", f"{host_path}:{container_path}:{mode}"]
        args += [image, "sleep", "infinity"]

        last_stderr = ""
        for attempt in range(3):
            r = _docker(*args, timeout=90)
            if r.returncode == 0:
                break
            last_stderr = _tail(r.stderr, 4000)
            if attempt == 2 or not _IMAGE_GONE_RE.search(last_stderr):
                raise SandboxOpenError(f"docker run failed to start sandbox: {last_stderr}")
            time.sleep(2 * (attempt + 1))
        else:
            raise SandboxOpenError(f"docker run failed to start sandbox: {last_stderr}")
        self._container_name = name
        self._workdir = workdir
        self._is_open = True
        self._opened_at = time.monotonic()
        self._closed_at: Optional[float] = None

    def close(self) -> None:
        if not self._is_open:
            return
        _docker("rm", "-f", self._container_name, timeout=60)
        self._closed_at = time.monotonic()
        self._is_open = False

    def _require_open(self) -> str:
        if not self._is_open or self._container_name is None:
            raise SandboxNotOpenError("sandbox is not open — call open() first")
        return self._container_name

    @property
    def raw_env(self) -> str:
        """The real container name this provider opened — the docker-side
        equivalent of DaytonaProvider.raw_env's escape hatch, for a caller
        that wants to run its own `docker exec`/`docker cp` directly rather
        than through run_command/put_file/get_file (e.g. a library needing
        a feature this provider doesn't wrap)."""
        return self._require_open()

    # -- files ------------------------------------------------------------

    def put_file(
        self,
        local_path: Union[str, Path],
        remote_path: str,
        *,
        mode: str = "file",
        permissions: Optional[str] = None,
    ) -> None:
        name = self._require_open()
        # docker cp always writes the new file(s) as root:root — a container
        # ending in a non-root USER (the common case) then can't read what it
        # was just given unless we chmod it back open as root explicitly
        # (docker exec --user root works reliably here; running the chmod as
        # the container's own default user would fail to chmod a file it
        # doesn't own).
        default_perm = "755" if mode == "file" else "a+rwX"
        perm = permissions or default_perm
        if mode == "dir":
            _docker("exec", "--user", "root", name, "mkdir", "-p", remote_path, timeout=30)
            _docker("cp", str(Path(local_path)) + "/.", f"{name}:{remote_path}/", timeout=120)
            _docker("exec", "--user", "root", name, "chmod", "-R", perm, remote_path, timeout=30)
        else:
            _docker("cp", str(local_path), f"{name}:{remote_path}", timeout=60)
            _docker("exec", "--user", "root", name, "chmod", perm, remote_path, timeout=20)

    def get_file(self, remote_path: str, local_path: Union[str, Path]) -> None:
        name = self._require_open()
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        _docker("cp", f"{name}:{remote_path}", str(local_path), timeout=60)

    # -- exec ---------------------------------------------------------------

    def run_command(
        self,
        command: str,
        *,
        timeout: float = 300.0,
        cwd: Optional[str] = None,
    ) -> CommandResult:
        name = self._require_open()
        workdir = cwd or self._workdir
        try:
            r = _docker("exec", "-w", workdir, name, "bash", "-lc", command, timeout=timeout)
        except subprocess.TimeoutExpired:
            return CommandResult(exit_code=None, output="", timed_out=True)
        return CommandResult(exit_code=r.returncode, output=_tail(r.stdout + r.stderr))

    # -- dependencies ---------------------------------------------------

    def add_dependency(self, command: str, *, note: str = "") -> None:
        self.run_command(command)
        self._dependencies.append(DependencyEntry(
            step=len(self._dependencies),
            command=command,
            note=note,
            added_at=datetime.now(timezone.utc).isoformat(),
        ))

    # -- image build / push ----------------------------------------------

    def build_sandbox_image(
        self,
        context_dir: Union[str, Path],
        *,
        tag: str,
        timeout: float = 1800.0,
        network: Optional[str] = None,
        dockerfile: Optional[str] = None,
    ) -> BuildResult:
        t0 = time.monotonic()
        args = ["build", "-t", tag]
        if network:
            # RUN steps of a build use the DEFAULT bridge; on a shared host with a /28
            # bridge that fails apt/pip with no route — `host` sidesteps it.
            args += ["--network", network]
        if dockerfile:
            args += ["-f", dockerfile]
        args.append(str(context_dir))
        try:
            r = _docker(*args, timeout=timeout)
        except subprocess.TimeoutExpired:
            return BuildResult(ok=False, image=tag,
                               detail=f"build timed out after {timeout}s",
                               seconds=time.monotonic() - t0)
        ok = r.returncode == 0
        detail = "built" if ok else _tail((r.stdout + r.stderr).strip(), 3000)
        return BuildResult(ok=ok, image=tag if ok else "", detail=detail,
                           seconds=time.monotonic() - t0)

    def push_sandbox_image(
        self,
        tag: str,
        *,
        registry: Optional[str] = None,
        timeout: float = 600.0,
    ) -> PushResult:
        t0 = time.monotonic()
        reg = registry or self.config.registry
        ref = f"{reg}/{tag}" if reg and not tag.startswith(reg) else tag
        if ref != tag:
            r = _docker("tag", tag, ref, timeout=30)
            if r.returncode != 0:
                return PushResult(ok=False, image=ref, detail=_tail(r.stderr, 2000),
                                  seconds=time.monotonic() - t0)
        try:
            r = _docker("push", ref, timeout=timeout)
        except subprocess.TimeoutExpired:
            return PushResult(ok=False, image=ref, detail=f"push timed out after {timeout}s",
                              seconds=time.monotonic() - t0)
        ok = r.returncode == 0
        detail = "pushed" if ok else _tail((r.stdout + r.stderr).strip(), 3000)
        return PushResult(ok=ok, image=ref if ok else "", detail=detail,
                          seconds=time.monotonic() - t0)

    # -- cost -------------------------------------------------------------

    def get_total_runtime_cost(self) -> float:
        return 0.0
