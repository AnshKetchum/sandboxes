"""Parallel opens of one task build its content-tagged image once: builders of a tag on this host take turns, and a
builder that finds the image built while it waited does not build it again."""

import threading
import time

from sandboxes import docker_provider
from sandboxes.models import BuildResult, SandboxProviderConfig


def test_parallel_builders_of_one_tag_build_it_once(tmp_path, monkeypatch):
    built, exists = [], set()
    monkeypatch.setattr(docker_provider, "_image_exists", lambda tag: tag in exists)

    def fake_build(self, context_dir, *, tag, timeout, network=None, dockerfile=None):
        built.append(tag)
        time.sleep(0.3)                     # long enough for the other builders to queue on the lock
        exists.add(tag)
        return BuildResult(ok=True, image=tag)

    monkeypatch.setattr(docker_provider.DockerProvider, "build_sandbox_image", fake_build)
    monkeypatch.setattr(docker_provider.DockerProvider, "_dockerfile_tag", lambda self, c, d: "sandboxes-ctx:abc123")
    providers = [docker_provider.DockerProvider(SandboxProviderConfig(name="docker")) for _ in range(4)]
    threads = [threading.Thread(target=provider._build_context, args=(tmp_path, None, 60)) for provider in providers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert built == ["sandboxes-ctx:abc123"]


def test_a_build_that_lost_the_race_to_another_process_is_no_error(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(docker_provider, "_image_exists", lambda tag: len(calls) > 0)
    monkeypatch.setattr(docker_provider.DockerProvider, "_dockerfile_tag", lambda self, c, d: "sandboxes-ctx:def456")

    def fake_build(self, context_dir, *, tag, timeout, network=None, dockerfile=None):
        calls.append(tag)
        return BuildResult(ok=False, detail='image "sandboxes-ctx:def456": already exists')

    monkeypatch.setattr(docker_provider.DockerProvider, "build_sandbox_image", fake_build)
    tag = docker_provider.DockerProvider(SandboxProviderConfig(name="docker"))._build_context(tmp_path, None, 60)
    assert tag == "sandboxes-ctx:def456" and calls == ["sandboxes-ctx:def456"]
