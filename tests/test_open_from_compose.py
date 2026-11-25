"""open_from_compose: a Harbor multi-container environment on docker. `main` is the context's Dockerfile, built under
its content tag and sleeping, layered first; the task's docker-compose.yaml next; infra last (the standard labels on
every service, main's limits, an internal network when sealed). A failed `up` takes the project down and says why;
close() removes the project and its volumes; providers that cannot run one refuse."""
import json
import subprocess
from pathlib import Path

import pytest

from sandboxes import DockerProvider, SandboxProviderConfig
from sandboxes import docker_provider
from sandboxes.errors import SandboxOpenError
from sandboxes.generic_provider import E2BProvider

COMPOSE = """services:
  main:
    depends_on: {seed: {condition: service_completed_successfully}}
  notes:
    image: python:3.12-slim
  seed:
    image: python:3.12-slim
"""


class _FakeDocker:
    """Plays the docker CLI: records each call (with the generated compose files' contents), succeeds unless told
    `up` fails or hangs."""

    def __init__(self, up_fails: bool = False, up_hangs: bool = False):
        self.calls: list[list[str]] = []
        self.files: dict[str, dict] = {}
        self.up_fails = up_fails
        self.up_hangs = up_hangs

    def __call__(self, *args, timeout):
        self.calls.append(list(args))
        for index, arg in enumerate(args):
            if arg == "-f" and args[index + 1].endswith(".json"):
                self.files[Path(args[index + 1]).name] = json.loads(Path(args[index + 1]).read_text())
        if "up" in args and self.up_hangs:
            raise subprocess.TimeoutExpired(["docker", *args], timeout)
        stdout, code = "", 0
        if "up" in args and self.up_fails:
            code = 1
        elif "ps" in args:
            stdout = "main-container-id\n"
        elif "logs" in args:
            stdout = "notes  | listening\n"
        return type("Result", (), {"returncode": code, "stdout": stdout, "stderr": "dependency failed" if code else ""})


def _ctx(tmp_path: Path) -> Path:
    ctx = tmp_path / "environment"
    ctx.mkdir()
    (ctx / "Dockerfile").write_text("FROM python:3.12-slim\nWORKDIR /app\n")
    (ctx / "docker-compose.yaml").write_text(COMPOSE)
    return ctx


@pytest.fixture
def fake(monkeypatch):
    docker = _FakeDocker()
    monkeypatch.setattr(docker_provider, "_docker", docker)
    monkeypatch.setattr(DockerProvider, "_build_context", lambda self, ctx, dockerfile, timeout: "sandboxes-ctx:abc")
    return docker


def test_main_is_the_built_context_under_the_task_compose_with_infra_last(fake, tmp_path):
    provider = DockerProvider(SandboxProviderConfig(name="docker", cpu=2, memory_mb=2048))
    provider.open_from_compose(_ctx(tmp_path), workdir="/app", network="none", name_prefix="Run 7", labels={"task": "t1"})
    up = next(call for call in fake.calls if "up" in call)
    files = [up[index + 1] for index, arg in enumerate(up) if arg == "-f"]
    assert files[0].endswith("main.json") and files[1].endswith("docker-compose.yaml") and files[2].endswith("infra.json")
    assert "--wait" in up and up[2].startswith("run-7-") and up[up.index("--project-directory") + 1].endswith("environment")
    assert fake.files["main.json"]["services"]["main"] == {"image": "sandboxes-ctx:abc",
                                                            "command": ["sh", "-c", "sleep infinity"]}
    infra = fake.files["infra.json"]
    assert set(infra["services"]) == {"main", "notes", "seed"}
    assert all(service["labels"]["task"] == "t1" for service in infra["services"].values())
    assert infra["services"]["main"]["cpus"] == 2 and infra["services"]["main"]["mem_limit"] == "2048m"
    assert infra["networks"] == {"default": {"internal": True}}
    assert provider._require_open() == "main-container-id"
    provider.close()
    down = next(call for call in fake.calls if "down" in call)
    assert "-v" in down and down[down.index("--rmi") + 1] == "local"
    assert provider._compose is None


def test_an_open_network_adds_no_network_override(fake, tmp_path):
    provider = DockerProvider()
    provider.open_from_compose(_ctx(tmp_path))
    assert "networks" not in fake.files["infra.json"]
    provider.close()


def test_a_failed_up_takes_the_project_down_and_says_why(monkeypatch, tmp_path):
    docker = _FakeDocker(up_fails=True)
    monkeypatch.setattr(docker_provider, "_docker", docker)
    monkeypatch.setattr(DockerProvider, "_build_context", lambda self, ctx, dockerfile, timeout: "sandboxes-ctx:abc")
    provider = DockerProvider()
    with pytest.raises(SandboxOpenError, match="dependency failed(.|\n)*notes  \\| listening"):
        provider.open_from_compose(_ctx(tmp_path))
    assert any("down" in call for call in docker.calls) and not provider._is_open


def test_an_up_that_never_returns_takes_the_project_down(monkeypatch, tmp_path):
    docker = _FakeDocker(up_hangs=True)
    monkeypatch.setattr(docker_provider, "_docker", docker)
    monkeypatch.setattr(DockerProvider, "_build_context", lambda self, ctx, dockerfile, timeout: "sandboxes-ctx:abc")
    provider = DockerProvider()
    with pytest.raises(SandboxOpenError, match="did not return"):
        provider.open_from_compose(_ctx(tmp_path))
    assert any("down" in call for call in docker.calls) and not provider._is_open and provider._compose is None


def test_no_compose_file_or_no_compose_provider_refuses(fake, tmp_path):
    ctx = _ctx(tmp_path)
    (ctx / "docker-compose.yaml").unlink()
    with pytest.raises(SandboxOpenError, match="does not exist"):
        DockerProvider().open_from_compose(ctx)
    other = E2BProvider(SandboxProviderConfig(name="e2b"))
    assert not other.can_open_from_compose() and DockerProvider().can_open_from_compose()
    with pytest.raises(SandboxOpenError, match="multi-container"):
        other.open_from_compose(ctx)
