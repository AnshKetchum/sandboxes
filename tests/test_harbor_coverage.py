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
