"""Every provider in the registry must be usable from harbor: either harbor ships a native environment for it (HARBOR_ENVS) or it
has a RawSandbox backend, which gets the generic prebuilt-image environment (harbor_raw). Adding a provider without either fails here."""

import pytest

from sandboxes.harbor_adapter import HARBOR_ENVS, harbor_providers
from sandboxes.registry import PROVIDERS


def _remote(name):
    return not PROVIDERS[name].capabilities.get("local")


def test_every_remote_provider_has_a_harbor_path():
    missing = [n for n in PROVIDERS if _remote(n) and n not in harbor_providers()]
    assert not missing, f"no harbor environment for {missing}: give them a RawSandbox backend (registry `raw`) or add a native HARBOR_ENVS entry"


def test_raw_backed_providers_build_a_harbor_class():
    pytest.importorskip("harbor")
    from harbor.environments.base import BaseEnvironment
    from sandboxes.harbor_adapter import harbor_env_class
    for name in harbor_providers():
        if name not in HARBOR_ENVS:
            cls = harbor_env_class(name)
            assert issubclass(cls, BaseEnvironment) and cls.PROVIDER == name and cls.LEDGER_NATIVE


def test_every_provider_declares_whether_it_needs_a_prebuilt_image():
    undeclared = [n for n, i in PROVIDERS.items() if "requires_prebuilt_image" not in i.capabilities]
    assert not undeclared, f"declare requires_prebuilt_image in registry.DEFAULT_CAPABILITIES for {undeclared}"


def test_the_declaration_matches_what_the_provider_harbor_path_can_do():
    # harbor's native environments (HARBOR_ENVS) build a task's Dockerfile; the generic raw-backed one (harbor_raw) only boots an image
    for name in harbor_providers():
        assert PROVIDERS[name].capabilities["requires_prebuilt_image"] == (name not in HARBOR_ENVS and not PROVIDERS[name].capabilities.get("builds_dockerfile")), name


def test_dockerfile_workdir_is_the_last_absolute_one(tmp_path):
    pytest.importorskip("harbor")                       # harbor_raw imports harbor (the `daytona` extra)
    from sandboxes.harbor_raw import dockerfile_workdir
    (tmp_path / "Dockerfile").write_text("FROM x\nWORKDIR /a\nRUN true\nworkdir /workspace\nWORKDIR rel\n")
    assert dockerfile_workdir(tmp_path) == "/workspace"
    assert dockerfile_workdir(tmp_path / "nope") is None
