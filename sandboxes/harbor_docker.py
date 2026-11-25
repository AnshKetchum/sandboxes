"""A harbor environment on local docker, through DockerProvider: a task with environment/docker-compose.yaml comes up as its
whole stack (open_from_compose: `main` is the sandbox the agent and the verifier run in, the services beside it), any other
task as its Dockerfile's container (open_from_dockerfile). Harbor's own docker environment cannot run a sealed compose task:
its no-network sidecar takes over the network of every service, so a seeder cannot reach the services it seeds.

A compose task is graded in a fresh `main` of the same stack (DockerProvider.renew_main, when the verifier uploads the tests):
the services keep the state the solver left, and nothing it changed inside its workstation reaches the grade.

The provider is synchronous, so each call runs in a thread: one process's sessions on this environment do not block each other.
Docker has no provider-side lifetime; a stack ends at stop(), or by its labels (`sandboxes stop --run ...`, fleet.stop)."""

from __future__ import annotations

import asyncio
import shlex
from pathlib import Path
from typing import Optional

from harbor.environments.base import BaseEnvironment, ExecResult
from harbor.environments.capabilities import EnvironmentCapabilities, EnvironmentResourceCapabilities
from harbor.environments.definition import effective_exec_cwd

from .docker_provider import DockerProvider
from .harbor_raw import dockerfile_workdir
from .models import SandboxProviderConfig

COMPOSE_FILE = "docker-compose.yaml"
TESTS_DIR = "/tests"            # where harbor's and euphoria's verifiers upload the tests, right before grading
EXEC_TIMEOUT_S = 86400          # harbor passes none for an agent's long commands; the agent's own limits end those


class DockerHarborEnvironment(BaseEnvironment):
    LEDGER_NATIVE = False           # track_env records its cost-ledger entry (docker prices at 0)

    def __init__(self, *args, lifetime_s: Optional[int] = None, labels: Optional[dict] = None, **kwargs):
        # docker never ends a stack itself: the lifetime goes on its labels (sandboxes.lifetime_s) for cleanup by age
        self._lifetime_s = lifetime_s
        self.labels = {**dict(labels or {}), **({"sandboxes.lifetime_s": str(lifetime_s)} if lifetime_s else {})}
        self.provider: Optional[DockerProvider] = None
        super().__init__(*args, **kwargs)

    @staticmethod
    def type() -> str:
        return "sandboxes-docker"

    @classmethod
    def resource_capabilities(cls) -> EnvironmentResourceCapabilities:
        return EnvironmentResourceCapabilities(cpu_limit=True, memory_limit=True)

    @property
    def capabilities(self) -> EnvironmentCapabilities:
        return EnvironmentCapabilities(disable_internet=True, docker_compose=True)

    @property
    def uses_compose(self) -> bool:
        return (Path(self.environment_dir) / COMPOSE_FILE).is_file()

    @property
    def _sandbox_name(self) -> str:
        """The `main` container's name, for the cost ledger's entry (harbor_adapter._sandbox_id)."""
        return self.provider.raw_env if self.provider is not None and self.provider._is_open else ""

    def _validate_definition(self):
        if not (Path(self.environment_dir) / "Dockerfile").is_file():
            raise RuntimeError(f"docker: {self.environment_dir} has no Dockerfile")

    def get_workdir(self) -> str:
        return self.task_env_config.workdir or dockerfile_workdir(self.environment_dir) or "/"

    def open_provider(self) -> None:
        config = SandboxProviderConfig(name="docker", cpu=self._effective_cpus or None, memory_mb=self._effective_memory_mb or None)
        provider = DockerProvider(config)
        options = dict(workdir=self.get_workdir(), network="none" if self._network_disabled else "bridge",
                       name_prefix=self.session_id, labels=self.labels)
        if self.uses_compose:
            provider.open_from_compose(self.environment_dir, **options)
        else:
            provider.open_from_dockerfile(self.environment_dir, **options)
        self.provider = provider

    async def start(self, force_build: bool):
        await asyncio.to_thread(self.open_provider)

    async def stop(self, delete: bool):
        provider, self.provider = self.provider, None
        if provider is not None:
            await asyncio.to_thread(provider.close)

    def require(self) -> DockerProvider:
        if self.provider is None:
            raise RuntimeError("Sandbox not found. Please start the environment first.")
        return self.provider

    def renew_for_grading(self, target: str) -> None:
        """Grading starts when the verifier uploads the tests to /tests (harbor's and euphoria's verifiers both do, right
        before test.sh): a compose task is then graded in a fresh `main` on the same stack, so nothing the solver did
        inside its workstation (/etc/hosts, a fake service on localhost, the tools the grader runs) reaches the grade,
        while the services keep the state it left."""
        if self.uses_compose and str(target).rstrip("/").startswith(TESTS_DIR):
            self.require().renew_main()

    async def upload_file(self, source_path, target_path: str):
        await asyncio.to_thread(self.renew_for_grading, target_path)
        await asyncio.to_thread(self.require().put_file, source_path, target_path)

    async def upload_dir(self, source_dir, target_dir: str):
        await asyncio.to_thread(self.renew_for_grading, target_dir)
        await asyncio.to_thread(self.require().put_file, source_dir, target_dir, mode="dir")

    async def download_file(self, source_path: str, target_path):
        await asyncio.to_thread(self.require().get_file, source_path, target_path)

    async def download_dir(self, source_dir: str, target_dir):
        Path(target_dir).mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self.require().get_file, source_dir.rstrip("/") + "/.", target_dir)

    def get_command(self, command: str, env: Optional[dict[str, str]], user) -> str:
        """`command` with the environment's and the call's variables exported, run as `user` when that is not root."""
        user = self._resolve_user(user)
        exports = "".join(f"export {key}={shlex.quote(str(value))}; " for key, value in (self._merge_env(env) or {}).items())
        full = exports + command
        if user is not None and str(user) not in ("root", "0"):
            full = f"su {shlex.quote(str(user))} -s /bin/bash -c {shlex.quote(full)}"
        return full

    async def exec(self, command: str, cwd: Optional[str] = None, env: Optional[dict[str, str]] = None,
                   timeout_sec: Optional[int] = None, user=None) -> ExecResult:
        full = self.get_command(command, env, user)
        workdir = effective_exec_cwd(cwd, self.task_env_config.workdir, self.get_workdir())
        result = await asyncio.to_thread(self.require().run_command, full, timeout=timeout_sec or EXEC_TIMEOUT_S, cwd=workdir)
        return ExecResult(stdout=result.output, stderr="", return_code=124 if result.timed_out else result.exit_code)
