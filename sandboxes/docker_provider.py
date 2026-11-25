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

import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar, Optional, Union

import yaml

from .base import SandboxProvider
from .buildctx import context_digest, resolve_dockerfile
from .errors import BuildError, ImageBuildError, PushError, SandboxNotOpenError, SandboxOpenError
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
_COMPOSE_FILE = "docker-compose.yaml"      # Harbor's multi-container environment file, beside the Dockerfile
_COMPOSE_WAIT_S = 600                       # healthchecks and one-shot (seed) services, at `up --wait`


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


_networks_ready: set = set()


def _ensure_network(name: str, subnet: Optional[str]) -> str:
    """`name` if it exists or could be created (bridge driver, `subnet` when
    given), else "bridge". A user-defined network is how a shared host gets
    past a small default bridge (a /28 on the euphoria/greenhouse host:
    13 addresses for every agent's containers) without touching others'."""
    if name in ("bridge", "host", "none") or name in _networks_ready:
        return name
    try:
        if _docker("network", "inspect", name, timeout=20).returncode != 0:
            args = ["network", "create", "--driver", "bridge", "--label", "sandboxes.managed=true"]
            if subnet:
                args += ["--subnet", subnet]
            c = _docker(*args, name, timeout=60)
            if c.returncode != 0 and "already exists" not in c.stderr:
                print(f"sandboxes: could not create docker network {name!r} ({c.stderr.strip()[:200]}); "
                      f"falling back to bridge", flush=True)
                return "bridge"
    except subprocess.TimeoutExpired:
        return "bridge"
    _networks_ready.add(name)
    return name


class DockerProvider(SandboxProvider):
    """Config `extra` knobs (all optional):
      network          network a sandbox joins when open() is given the
                       default "bridge" — created on first use
      network_subnet   subnet for creating `network`
      build_network    network RUN steps of open_from_dockerfile's build use
                       (e.g. "host" when the default bridge has no route)
      image_namespace  repository open_from_dockerfile tags its builds under
                       (`<namespace>:<context digest>`; default sandboxes-ctx)
    """

    name: ClassVar[str] = "docker"

    def __init__(self, config: Optional[SandboxProviderConfig] = None):
        super().__init__(config)
        self._container_name: Optional[str] = None
        self._workdir: str = "/"
        self._compose: Optional[list[str]] = None      # the compose CLI prefix of a multi-container environment
        self._compose_dir: Optional[str] = None        # its generated main/infra compose files

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
        extra = cfg.extra or {}
        if network == "bridge" and extra.get("network"):
            network = _ensure_network(str(extra["network"]), extra.get("network_subnet"))
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

    def can_open_from_dockerfile(self) -> bool:
        return True

    def _dockerfile_tag(self, context_dir, dockerfile: Optional[str]) -> str:
        ns = (self.config.extra or {}).get("image_namespace") or "sandboxes-ctx"
        return f"{ns}:{context_digest(context_dir, dockerfile)}"

    def open_from_dockerfile(
        self,
        context_dir: Union[str, Path],
        *,
        dockerfile: Optional[str] = None,
        workdir: str = "/",
        network: str = "bridge",
        name_prefix: Optional[str] = None,
        labels: Optional[dict[str, str]] = None,
        build_timeout: float = 1800.0,
    ) -> None:
        """`docker build` the context under its content tag unless that tag
        already exists locally (an unchanged context reopens instantly),
        then `open()` it. Not built through `open()`'s retry: an image-gone
        race after the existence check is `open()`'s own retry case."""
        tag = self._build_context(context_dir, dockerfile, build_timeout)
        self.open(tag, workdir=workdir, network=network, name_prefix=name_prefix, labels=labels)

    def _build_context(self, context_dir, dockerfile: Optional[str], build_timeout: float) -> str:
        """The context's content tag, built unless it already exists locally."""
        tag = self._dockerfile_tag(context_dir, dockerfile)
        try:
            exists = _docker("image", "inspect", tag, timeout=20).returncode == 0
        except subprocess.TimeoutExpired:
            exists = False
        if not exists:
            r = self.build_sandbox_image(
                context_dir, tag=tag, timeout=build_timeout,
                network=(self.config.extra or {}).get("build_network"),
                dockerfile=str(resolve_dockerfile(context_dir, dockerfile)) if dockerfile else None)
            if not r.ok:
                raise ImageBuildError(f"docker: image build failed for {context_dir}: {r.detail}")
        return tag

    def can_open_from_compose(self) -> bool:
        return True

    def open_from_compose(
        self,
        context_dir: Union[str, Path],
        *,
        workdir: str = "/",
        network: str = "bridge",
        name_prefix: Optional[str] = None,
        labels: Optional[dict[str, str]] = None,
        build_timeout: float = 1800.0,
    ) -> None:
        """A Harbor multi-container environment, layered as harbor's own docker environment layers it: the
        context's Dockerfile built (content-tagged, as open_from_dockerfile) as `main`, sleeping; then the task's
        docker-compose.yaml; then infra last (the standard labels on every service, main's cpu/memory/pids limits,
        and for network "none" an internal default network: no egress, the services still reach one another).
        `up --wait` returns once every healthcheck passes and every one-shot service has exited 0; a failure takes
        the project down and raises SandboxOpenError with the services' log tails. `main` becomes the sandbox, so
        run_command, put_file, get_file and stats work as for a single container; close() removes the project and
        its volumes."""
        if self._is_open:
            raise SandboxOpenError("already open — call close() first")
        context_dir = Path(context_dir)
        task_compose = context_dir / _COMPOSE_FILE
        if not task_compose.is_file():
            raise SandboxOpenError(f"docker: {task_compose} does not exist; open_from_dockerfile opens a single container")
        tag = self._build_context(context_dir, None, build_timeout)
        prefix = _sanitize_name_prefix(name_prefix) if name_prefix else "sbx"
        project = re.sub(r"[^a-z0-9_-]", "-", f"{prefix}-{uuid.uuid4().hex[:12]}".lower())
        from .labels import standard_labels
        std = standard_labels(run=name_prefix if name_prefix and not os.environ.get("SANDBOX_RUN_ID") else None, extra=labels)
        services = (yaml.safe_load(task_compose.read_text()) or {}).get("services") or {}
        infra: dict = {"services": {name: {"labels": dict(std)} for name in {"main", *services}}}
        cfg = self.config
        limits = {"cpus": cfg.cpu, "mem_limit": f"{cfg.memory_mb}m" if cfg.memory_mb else None, "pids_limit": cfg.pids_limit}
        infra["services"]["main"].update({key: value for key, value in limits.items() if value})
        if network == "none":
            infra["networks"] = {"default": {"internal": True}}
        files_dir = tempfile.mkdtemp(prefix=f"{project}-compose-")
        base_path, infra_path = Path(files_dir) / "main.json", Path(files_dir) / "infra.json"
        base_path.write_text(json.dumps({"services": {"main": {"image": tag, "command": ["sh", "-c", "sleep infinity"]}}}))
        infra_path.write_text(json.dumps(infra))
        self._compose = ["compose", "-p", project, "-f", str(base_path), "-f", str(task_compose), "-f", str(infra_path)]
        self._compose_dir = files_dir
        up = _docker(*self._compose, "up", "-d", "--wait", "--wait-timeout", str(_COMPOSE_WAIT_S),
                     timeout=_COMPOSE_WAIT_S + 120)
        main = _docker(*self._compose, "ps", "-q", "main", timeout=60).stdout.strip() if up.returncode == 0 else ""
        if not main:
            logs = _docker(*self._compose, "logs", "--no-color", "--tail", "40", timeout=120)
            self._close_compose()
            raise SandboxOpenError(f"docker compose up failed: {_tail(up.stderr, 3000)}\n"
                                   f"services' logs:\n{_tail(logs.stdout, 6000)}")
        self._container_name = main
        self._workdir = workdir
        self._is_open = True
        self._opened_at = time.monotonic()
        self._closed_at = None

    def _close_compose(self) -> None:
        _docker(*self._compose, "down", "-v", "--remove-orphans", "--timeout", "10", timeout=300)
        shutil.rmtree(self._compose_dir, ignore_errors=True)
        self._compose = None

    def release_dockerfile_image(
        self, context_dir: Union[str, Path], *, dockerfile: Optional[str] = None
    ) -> bool:
        """`docker image rm` the context's tag; docker refuses while a
        container still uses it, which is the right answer."""
        try:
            r = _docker("image", "rm", self._dockerfile_tag(context_dir, dockerfile), timeout=120)
        except subprocess.TimeoutExpired:
            return False
        return r.returncode == 0

    def close(self) -> None:
        if not self._is_open:
            return
        if self._compose:
            self._close_compose()
        else:
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
