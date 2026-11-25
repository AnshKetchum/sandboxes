"""The harbor environment on local docker: a compose task comes up as its stack and a Dockerfile task as one container, commands
run as the task's user with its variables, and a stopped stack is taken down whole (fleet's docker removal)."""

import asyncio
import json
import shlex
import subprocess

import pytest

pytest.importorskip("harbor")

from harbor.models.task.config import EnvironmentConfig          # noqa: E402
from harbor.models.trial.paths import TrialPaths                   # noqa: E402

from sandboxes import fleet, harbor_docker                         # noqa: E402
from sandboxes.harbor_adapter import harbor_env_class, harbor_providers, lifetime_kwargs   # noqa: E402
from sandboxes.models import CommandResult                         # noqa: E402


class FakeProvider:
    instances: list = []

    def __init__(self, config):
        self.config, self.calls, self._is_open = config, [], False
        FakeProvider.instances.append(self)

    def open_from_compose(self, environment_dir, **options):
        self.calls.append(("compose", str(environment_dir), options))
        self._is_open = True

    def open_from_dockerfile(self, environment_dir, **options):
        self.calls.append(("dockerfile", str(environment_dir), options))
        self._is_open = True

    @property
    def raw_env(self):
        return "t__1-main"

    def run_command(self, command, timeout=300.0, cwd=None):
        self.calls.append(("run", command, cwd))
        return CommandResult(exit_code=0, output="ok\n", timed_out=False)

    def close(self):
        self.calls.append(("close",))
        self._is_open = False

    def renew_main(self):
        self.calls.append(("renew",))

    def put_file(self, source, target, mode="file"):
        self.calls.append(("put", str(target), mode))


def make_env(tmp_path, monkeypatch, compose: bool):
    monkeypatch.setattr(harbor_docker, "DockerProvider", FakeProvider)
    env_dir = tmp_path / "environment"
    env_dir.mkdir()
    (env_dir / "Dockerfile").write_text("FROM python:3.12-slim\nWORKDIR /workspace\n")
    if compose:
        (env_dir / "docker-compose.yaml").write_text("services:\n  main:\n    build: .\n")
    trial = TrialPaths(trial_dir=tmp_path / "trial")
    trial.mkdir()
    kw = dict(environment_dir=env_dir, environment_name="t", session_id="t__1", trial_paths=trial,
              task_env_config=EnvironmentConfig())
    kw.update(lifetime_kwargs("docker", {"auto_delete_minutes": 180}, labels={"owner": "euphoria"}))
    return harbor_env_class("docker")(**kw)


def test_docker_is_a_harbor_provider():
    assert "docker" in harbor_providers() and harbor_env_class("docker") is harbor_docker.DockerHarborEnvironment


@pytest.mark.parametrize("compose", [True, False])
def test_a_compose_task_opens_its_stack_and_a_dockerfile_task_its_container(tmp_path, monkeypatch, compose):
    env = make_env(tmp_path, monkeypatch, compose)
    asyncio.run(env.start(force_build=False))
    kind, _, options = env.provider.calls[0]
    assert kind == ("compose" if compose else "dockerfile")
    assert options["workdir"] == "/workspace" and options["name_prefix"] == "t__1"
    assert options["labels"] == {"owner": "euphoria", "sandboxes.lifetime_s": str(180 * 60)}
    assert env._sandbox_name == "t__1-main"
    provider = env.provider
    asyncio.run(env.stop(delete=True))
    assert provider.calls[-1] == ("close",) and env.provider is None


def test_a_command_runs_as_the_given_user_with_its_variables(tmp_path, monkeypatch):
    env = make_env(tmp_path, monkeypatch, compose=True)
    asyncio.run(env.start(force_build=False))
    result = asyncio.run(env.exec("echo $GREETING", env={"GREETING": "hi there"}, user="agent"))
    _, command, cwd = env.provider.calls[-1]
    inner = env.get_command("echo $GREETING", {"GREETING": "hi there"}, None)
    assert inner == "export GREETING='hi there'; echo $GREETING"
    assert command == f"su agent -s /bin/bash -c {shlex.quote(inner)}"
    assert cwd == "/workspace" and result.return_code == 0 and result.stdout == "ok\n"
    asyncio.run(env.exec("id"))
    assert env.provider.calls[-1][1] == "id"


@pytest.mark.parametrize("compose", [True, False])
def test_a_compose_task_is_graded_in_a_fresh_main_once_and_other_uploads_are_not(tmp_path, monkeypatch, compose):
    env = make_env(tmp_path, monkeypatch, compose)
    asyncio.run(env.start(force_build=False))
    asyncio.run(env.upload_dir(tmp_path, "/solution"))
    asyncio.run(env.upload_dir(tmp_path, "/tests"))
    asyncio.run(env.upload_file(tmp_path / "x", "/tests/test_state.py"))
    kinds = [call[0] for call in env.provider.calls[1:]]
    assert kinds == (["put", "renew", "put", "put"] if compose else ["put", "put", "put"])
    record = tmp_path / "trial" / harbor_docker.GRADING_RECORD
    assert record.is_file() == compose
    if compose:
        assert json.loads(record.read_text())["solver_main"] == "t__1-main"


def test_removing_a_stack_container_takes_its_project_down(monkeypatch):
    calls = []

    def fake_run(args, **kwargs):
        calls.append(args)
        stdout = "t-1-abc\n" if args[1] == "inspect" and args[-1] == "stack-main" else "<no value>\n"
        return subprocess.CompletedProcess(args, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(fleet._subprocess, "run", fake_run)
    fleet._remove_docker("stack-main")
    assert calls[-1][:5] == ["docker", "compose", "-p", "t-1-abc", "down"] and "-v" in calls[-1]
    fleet._remove_docker("lone")
    assert calls[-1] == ["docker", "rm", "-f", "lone"]
