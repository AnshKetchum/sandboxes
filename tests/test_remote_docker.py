"""RemoteDockerProvider routes DockerProvider's docker commands and host paths into a VM: checked against a fake VM host
that records what it is asked to run and receive."""
import shlex
from pathlib import Path

import pytest

from sandboxes.errors import SandboxOpenError
from sandboxes.models import CommandResult
from sandboxes.remote_docker_provider import RemoteDockerProvider, _STDOUT_MARK


class FakeHost:
    def __init__(self, fail_up: bool = False):
        self.commands, self.uploads, self.fetches = [], [], []
        self.closed = False
        self.fail_up = fail_up

    def run_command(self, command, *, timeout=300.0, cwd=None):
        self.commands.append(command)
        if not command.startswith("docker "):
            return CommandResult(exit_code=0, output="")
        args = shlex.split(command.split(" >", 1)[0])[1:]
        if "up" in args and self.fail_up:
            return CommandResult(exit_code=1, output=f"up failed\n{_STDOUT_MARK}\n")
        stdout = "cid123\n" if args[-3:] == ["ps", "-q", "main"] else ""
        if args[:2] == ["image", "inspect"]:
            return CommandResult(exit_code=1, output=f"no such image\n{_STDOUT_MARK}\n")
        return CommandResult(exit_code=0, output=f"WARN compose warning\n{_STDOUT_MARK}\n{stdout}")

    def put_file(self, local_path, remote_path, *, mode="file", permissions=None):
        self.uploads.append((str(local_path), remote_path, mode))

    def get_file(self, remote_path, local_path):
        self.fetches.append((remote_path, str(local_path)))
        Path(local_path).write_text("fetched")

    def close(self):
        self.closed = True

    def get_total_runtime_cost(self):
        return 0.25


@pytest.fixture
def context(tmp_path):
    env = tmp_path / "environment"
    env.mkdir()
    (env / "Dockerfile").write_text("FROM busybox\n")
    (env / "docker-compose.yaml").write_text("services:\n  gitea:\n    image: gitea/gitea\n")
    return env


def make_provider(host):
    provider = RemoteDockerProvider()
    provider._host = host
    return provider


def test_a_compose_stack_opens_inside_the_vm(context):
    host = FakeHost()
    provider = make_provider(host)
    provider.open_from_compose(context, workdir="/root", network="none")
    uploaded = [remote for _, remote, _ in host.uploads]
    assert str(context.absolute()) in uploaded and any(path.endswith("main.json") for path in uploaded)
    assert uploaded.count(str(context.absolute())) == 1      # the build and compose share one upload
    docker = [shlex.split(command.split(" >", 1)[0])[1:] for command in host.commands if command.startswith("docker ")]
    assert any(args[0] == "build" for args in docker)
    assert any("up" in args and "--wait" in args for args in docker)
    assert provider.raw_env == "cid123"      # stdout read past compose's stderr warning


def test_commands_run_in_main_and_files_come_back_through_the_vm(context, tmp_path):
    host = FakeHost()
    provider = make_provider(host)
    provider.open_from_compose(context, workdir="/root")
    provider.run_command("echo hi")
    assert "docker exec -w /root cid123 bash -lc 'echo hi'" in host.commands[-1]
    local = tmp_path / "reward.txt"
    provider.get_file("/logs/verifier/reward.txt", local)
    assert local.read_text() == "fetched" and host.fetches[-1][0].startswith("/tmp/sandboxes-stage/")
    provider.put_file(context / "Dockerfile", "/tests/x")
    provider.put_file(context / "Dockerfile", "/tests/x")
    assert sum(remote == str((context / "Dockerfile").absolute()) for _, remote, _ in host.uploads) >= 2   # fresh each time


def test_close_takes_the_stack_down_and_closes_the_vm(context):
    host = FakeHost()
    provider = make_provider(host)
    provider.open_from_compose(context, workdir="/root")
    provider.close()
    assert any("down" in command for command in host.commands) and host.closed
    assert provider.get_total_runtime_cost() == 0.25


def test_a_stack_that_does_not_come_up_closes_its_vm(context):
    host = FakeHost(fail_up=True)
    provider = make_provider(host)
    with pytest.raises(SandboxOpenError):
        provider.open_from_compose(context, workdir="/root")
    assert host.closed


def test_bind_mounts_are_refused():
    provider = make_provider(FakeHost())
    with pytest.raises(SandboxOpenError):
        provider.open("busybox", volumes={"/data": "/data"})
