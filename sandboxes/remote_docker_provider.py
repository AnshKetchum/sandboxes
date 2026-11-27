"""RemoteDockerProvider: DockerProvider with its docker host in a cloud VM instead of this machine.

One VM per sandbox, opened from a docker-in-docker image on a provider whose sandboxes are VMs that can run a daemon
(Modal with runtime=vm, Fly, E2B). Every docker command DockerProvider runs (build, compose up, exec into main, cp,
compose run for renew_main) runs inside that VM, and the host paths it hands docker (the build context, the generated
compose files, files copied in or out of a container) are uploaded to the same paths in the VM first. A Harbor
multi-container task so comes up there as its whole stack, sealed by the same internal compose network, and `main` is
renewed for grading the same way, while the agent and the verifier only ever see `main`.

Config `extra` (all optional):
  host                         the VM's provider (default modal)
  host_extra                   that provider's config extra (default {"runtime": "vm"} for modal)
  host_image                   the VM's image; it must start dockerd with host_boot (default the dind builder image)
  host_boot                    the command that starts dockerd in the VM (default dind-start.sh)
  host_cpu / host_memory_mb / host_disk_gb   the VM's size (default 4 / 8192 / 40): the whole stack runs in it
  compose_plugin_url           a docker compose v2 plugin, installed when the VM's docker has none
"""

from __future__ import annotations

import shlex
import subprocess
import uuid
from pathlib import Path
from typing import ClassVar, Optional, Union

from .docker_provider import DockerProvider
from .errors import ImageBuildError, SandboxOpenError
from .models import SandboxProviderConfig
from .registry import PROVIDERS, load_class

_HOST_IMAGE = "registry-b.deepgrove.ai/openenv/swe-env-builder-dind:latest"
_HOST_EXTRA = {"modal": {"runtime": "vm"}, "fly": {"docker_loop_gb": 30}}
_COMPOSE_PLUGIN_URL = "https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64"
_PLUGIN_DIR = "/usr/local/lib/docker/cli-plugins"
_STAGE = "/tmp/sandboxes-stage"
_STDOUT_MARK = "__sandboxes_remote_docker_stdout__"     # stderr before it, stdout after: the exec merges the two


class RemoteDockerProvider(DockerProvider):
    name: ClassVar[str] = "docker-remote"

    def __init__(self, config: Optional[SandboxProviderConfig] = None):
        super().__init__(config)
        self._host = None                   # the VM's SandboxProvider
        self._uploaded: set[str] = set()    # paths already in the VM while the stack is being opened
        self._host_usd = 0.0

    # -- the VM --------------------------------------------------------------

    def _start_host(self):
        if self._host is not None:
            return self._host
        extra = dict(self.config.extra or {})
        name = str(extra.get("host") or "modal").lower()
        info = PROVIDERS.get(name)
        if info is None or not info.raw or not info.sync:
            raise SandboxOpenError(f"docker-remote: {name!r} has no VM backend to host docker")
        host_extra = {**_HOST_EXTRA.get(name, {}), **dict(extra.get("host_extra") or {}),
                      "boot_cmd": str(extra.get("host_boot") or "dind-start.sh")}
        config = SandboxProviderConfig(name=name, cpu=float(extra.get("host_cpu") or 4),
                                       memory_mb=int(extra.get("host_memory_mb") or 8192),
                                       disk_gb=int(extra.get("host_disk_gb") or 40),
                                       auto_delete_minutes=self.config.auto_delete_minutes, extra=host_extra)
        host = load_class(info.sync)(config)
        host.open(str(extra.get("host_image") or _HOST_IMAGE))
        self._host = host
        url = str(extra.get("compose_plugin_url") or _COMPOSE_PLUGIN_URL)
        plugin = host.run_command(f"docker compose version >/dev/null 2>&1 || (mkdir -p {_PLUGIN_DIR} && curl -fsSL "
                                  f"{shlex.quote(url)} -o {_PLUGIN_DIR}/docker-compose && chmod +x "
                                  f"{_PLUGIN_DIR}/docker-compose && docker compose version)", timeout=300)
        if plugin.exit_code != 0:
            self._close_host()
            raise SandboxOpenError(f"docker-remote: no docker compose in the {name} VM: {plugin.output[-1500:]}")
        return host

    def _close_host(self) -> None:
        host, self._host = self._host, None
        self._uploaded.clear()
        if host is not None:
            try:
                host.close()
            finally:
                self._host_usd += host.get_total_runtime_cost()

    # -- the docker host hooks -----------------------------------------------

    def _docker(self, *args: str, timeout: float) -> subprocess.CompletedProcess:
        host = self._start_host()
        stage = f"/tmp/sandboxes-docker-{uuid.uuid4().hex[:12]}"
        command = (f"docker {shlex.join(args)} >{stage}.out 2>{stage}.err; rc=$?; cat {stage}.err; echo; "
                   f"echo {_STDOUT_MARK}; cat {stage}.out; rm -f {stage}.out {stage}.err; exit $rc")
        result = host.run_command(command, timeout=timeout)
        if result.timed_out:
            raise subprocess.TimeoutExpired(["docker", *args], timeout)
        stderr, mark, stdout = result.output.partition(f"{_STDOUT_MARK}\n")
        if not mark:                        # the tail cut the marker: all of it is the command's stdout
            stderr, stdout = "", result.output
        code = result.exit_code if result.exit_code is not None else 1
        return subprocess.CompletedProcess(["docker", *args], code, stdout=stdout, stderr=stderr.rstrip("\n"))

    def _image_exists(self, tag: str) -> bool:
        try:
            return self._docker("image", "inspect", tag, timeout=20).returncode == 0
        except subprocess.TimeoutExpired:
            return False

    def _host_path(self, path: Union[str, Path]) -> str:
        """`path` uploaded to the same absolute path in the VM. While the stack is being opened a path goes up once (the
        build context serves the build and compose); once it is open every call uploads afresh (tests, solutions)."""
        local = Path(path).absolute()
        key = str(local)
        if self._is_open or key not in self._uploaded:
            host = self._start_host()
            parent = key if local.is_dir() else str(local.parent)
            host.run_command(f"mkdir -p {shlex.quote(parent)}", timeout=60)
            host.put_file(local, key, mode="dir" if local.is_dir() else "file")
            if not self._is_open:
                self._uploaded.add(key)
        return key

    def _fetch(self, source: str, local_path: Union[str, Path]) -> None:
        host = self._start_host()
        stage = f"{_STAGE}/{uuid.uuid4().hex}"
        host.run_command(f"mkdir -p {_STAGE}", timeout=60)
        self._docker("cp", source, stage, timeout=60)
        host.get_file(stage, local_path)
        host.run_command(f"rm -rf {shlex.quote(stage)}", timeout=60)

    def _build_context(self, context_dir, dockerfile: Optional[str], build_timeout: float) -> str:
        """The context's content tag, built in this VM (each VM has its own images, so no host-wide build lock)."""
        tag = self._dockerfile_tag(context_dir, dockerfile)
        if self._image_exists(tag):
            return tag
        result = self.build_sandbox_image(context_dir, tag=tag, timeout=build_timeout,
                                          network=(self.config.extra or {}).get("build_network"), dockerfile=dockerfile)
        if not result.ok:
            raise ImageBuildError(f"docker-remote: image build failed for {context_dir}: {result.detail}")
        return tag

    # -- lifecycle -------------------------------------------------------------

    def open(self, image: str, *, workdir: str = "/", network: str = "bridge", volumes: Optional[dict] = None,
             name_prefix: Optional[str] = None, labels: Optional[dict] = None) -> None:
        if volumes:
            raise SandboxOpenError("docker-remote: bind mounts of this machine's paths cannot reach the VM")
        try:
            super().open(image, workdir=workdir, network=network, name_prefix=name_prefix, labels=labels)
        except Exception:
            self._close_host()
            raise

    def open_from_compose(self, context_dir, **options) -> None:
        try:
            super().open_from_compose(context_dir, **options)
        except Exception:
            self._close_host()
            raise
        self._uploaded.clear()

    def open_from_dockerfile(self, context_dir, **options) -> None:
        try:
            super().open_from_dockerfile(context_dir, **options)
        except Exception:
            self._close_host()
            raise

    def close(self) -> None:
        try:
            super().close()
        finally:
            self._close_host()

    def get_total_runtime_cost(self) -> float:
        running = self._host.get_total_runtime_cost() if self._host is not None else 0.0
        return self._host_usd + running
