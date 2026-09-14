"""Config loading + the `get_sandbox_provider()` factory.

    provider = get_sandbox_provider()                       # SANDBOX_CONFIG_PATH env, else docker default
    provider = get_sandbox_provider("config/run.yaml")       # explicit path wins over the env var
    provider = get_sandbox_provider(config_path=None)        # same as no-arg

Config shape — the SAME `sandbox_providers:` list already used by euphoria
and petunia configs, so an existing config file needs no translation:

    sandbox_providers:
      - name: daytona
        disk_gb: 8
        cpu: 1
        memory_mb: 2048
        peak_concurrent_sandboxes: 230
        spend_cap_usd: 2500
        auto_stop_minutes: 120
        auto_delete_minutes: 0

A bare `sandbox_providers: [{name: docker}]`, an empty/missing
`sandbox_providers:` key, or no config at all — all resolve to a
DockerProvider with default settings. The list's FIRST entry is the one
used; supporting more than one active provider at a time is explicitly out
of scope (see the package README).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Union

import yaml

from .base import SandboxProvider
from .errors import ConfigError
from .models import SandboxProviderConfig

SANDBOX_CONFIG_PATH_ENV = "SANDBOX_CONFIG_PATH"

_PROVIDERS: dict[str, type[SandboxProvider]] = {}


def _register_builtin_providers() -> None:
    if _PROVIDERS:
        return
    from .docker_provider import DockerProvider

    _PROVIDERS["docker"] = DockerProvider
    try:
        from .daytona_provider import DaytonaProvider
    except ImportError:
        pass  # `daytona`/`harbor` extras not installed — docker-only install
    else:
        _PROVIDERS["daytona"] = DaytonaProvider


def _load_provider_config(config_path: Path) -> SandboxProviderConfig:
    if not config_path.is_file():
        raise ConfigError(f"sandbox config not found: {config_path}")
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"failed to parse YAML config {config_path}: {e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"sandbox config root must be a mapping: {config_path}")
    providers = raw.get("sandbox_providers") or []
    if not isinstance(providers, list) or not providers:
        return SandboxProviderConfig(name="docker")
    entry = providers[0]
    if not isinstance(entry, dict):
        raise ConfigError(f"sandbox_providers[0] must be a mapping: {config_path}")
    try:
        return SandboxProviderConfig.model_validate(entry)
    except Exception as e:  # pydantic ValidationError
        raise ConfigError(f"invalid sandbox_providers[0] in {config_path}: {e}") from e


def get_sandbox_provider(config_path: Optional[Union[str, Path]] = None) -> SandboxProvider:
    """Resolve config (explicit path > `SANDBOX_CONFIG_PATH` env > docker
    default) and return a ready-to-`.open()` provider instance."""
    _register_builtin_providers()

    resolved = config_path or os.environ.get(SANDBOX_CONFIG_PATH_ENV)
    if resolved is None:
        cfg = SandboxProviderConfig(name="docker")
    else:
        cfg = _load_provider_config(Path(resolved))

    name = (cfg.name or "docker").strip().lower()
    provider_cls = _PROVIDERS.get(name)
    if provider_cls is None:
        supported = ", ".join(sorted(_PROVIDERS)) or "docker"
        raise ConfigError(
            f"unsupported sandbox provider {name!r} (supported: {supported}); "
            f"install the `sandboxes[daytona]` extra if you meant daytona"
        )
    return provider_cls(cfg)
