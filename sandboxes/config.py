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
DockerProvider with default settings.

SEVERAL providers can be listed at once (daytona + e2b + blaxel ...). A listed provider is used only
when its API key(s) are in the environment (registry.py names them; `api_key_env:` overrides) and
`enabled` is not false. `get_sandbox_provider()` returns the first usable one (lowest `priority`,
then list order); `SandboxRouter.from_config()` (router.py) spreads sandboxes over all usable ones by
free capacity. Entries may be inline dicts, a path string, or `{base: path, overrides: {...}}`.

`sandbox_providers` can ALSO be a reference to the shared base configs in
`sandboxes/configs/*.yaml` instead of an inline list — the point of those
files: euphoria's own per-experiment configs had been independently
copy-pasting an identical daytona block into 7 different files, with only
`peak_concurrent_sandboxes` genuinely varying per run. Two shapes:

    sandbox_providers: /ansh/sandboxes/configs/daytona.yaml   # no overrides
    sandbox_providers:                                        # with overrides
      base: /ansh/sandboxes/configs/daytona.yaml
      overrides: {peak_concurrent_sandboxes: 48}

`load_sandbox_providers(base_path, overrides)` does this resolution as a
plain list-of-dicts (not a pydantic model) — the entry point for a host
codebase with its OWN separate config system (euphoria, petunia) that wants
to splice the shared config into its own dict without depending on this
package's models at all.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Union

import yaml

from .base import SandboxProvider
from .errors import ConfigError
from .models import SandboxProviderConfig
from .registry import PROVIDERS

SANDBOX_CONFIG_PATH_ENV = "SANDBOX_CONFIG_PATH"

_PROVIDERS: dict[str, type[SandboxProvider]] = {}


def _register_builtin_providers() -> None:
    """Fill _PROVIDERS from registry.PROVIDERS; a provider whose SDK extra is not installed is left out."""
    if _PROVIDERS:
        return
    from .registry import load_class

    for name, info in PROVIDERS.items():
        if not info.sync:
            continue
        try:
            _PROVIDERS[name] = load_class(info.sync)
        except ImportError:
            pass  # that provider's extra (daytona/e2b/blaxel SDK) is not installed


_KNOWN_KEYS = ("docker", "daytona", "e2b", "blaxel")


def _read_yaml(path: Path) -> dict:
    if not path.is_file():
        raise ConfigError(f"sandbox base config not found: {path}")
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"failed to parse YAML config {path}: {e}") from e


def _apply_overrides(entries: list[dict], overrides: Optional[dict]) -> list[dict]:
    """`overrides` is either flat ({peak_concurrent_sandboxes: 48}, applied to the FIRST entry, the old
    single-provider behaviour) or keyed by provider name ({e2b: {...}, daytona: {...}})."""
    if not overrides:
        return [dict(e) for e in entries]
    per_provider = isinstance(overrides, dict) and all(k in _KNOWN_KEYS or k in PROVIDERS for k in overrides) \
        and all(isinstance(v, dict) for v in overrides.values())
    out = []
    for i, e in enumerate(entries):
        e = dict(e)
        if per_provider:
            e.update(overrides.get(str(e.get("name", "")).lower(), {}))
        elif i == 0:
            e.update(overrides)
        out.append(e)
    return out


def load_sandbox_providers(base_path: Union[str, Path], overrides: Optional[dict] = None) -> list[dict]:
    """Load `base_path`'s `sandbox_providers` list (a shared config file such as `configs/multi.yaml`)
    and apply `overrides` (flat: first entry; or keyed by provider name). Returns ALL entries as plain
    dicts, the raw YAML shape, for a host codebase with its own config system to splice in."""
    raw = _read_yaml(Path(base_path))
    providers = _resolve_sandbox_providers((raw or {}).get("sandbox_providers"), _depth=1)
    return _apply_overrides(providers, overrides)


def _resolve_sandbox_providers(value, _depth: int = 0) -> list[dict]:
    """Normalize a `sandbox_providers` YAML value into a plain list-of-dicts: an inline list whose items
    are dicts, path strings or `{base, overrides}`; a bare path string; or `{base, overrides}`."""
    if _depth > 4:
        raise ConfigError("sandbox_providers references nest too deeply")
    if isinstance(value, str) and value.strip():
        return load_sandbox_providers(value)
    if isinstance(value, dict) and "base" in value:
        return load_sandbox_providers(value["base"], value.get("overrides"))
    if isinstance(value, list):
        out: list[dict] = []
        for item in value:
            if isinstance(item, (str, dict)) and (isinstance(item, str) or "base" in item):
                out.extend(_resolve_sandbox_providers(item, _depth + 1))
            elif isinstance(item, dict):
                out.append(item)
        return out
    return []


def load_provider_entries(config_path: Optional[Union[str, Path]] = None) -> dict:
    """{"providers": [entry dicts...], "total_spend_cap_usd": float|None} for a config file (explicit path,
    else SANDBOX_CONFIG_PATH, else a single local docker entry)."""
    resolved = config_path or os.environ.get(SANDBOX_CONFIG_PATH_ENV)
    if resolved is None:
        return {"providers": [{"name": "docker"}], "total_spend_cap_usd": None}
    raw = _read_yaml(Path(resolved))
    if not isinstance(raw, dict):
        raise ConfigError(f"sandbox config root must be a mapping: {resolved}")
    providers = _resolve_sandbox_providers(raw.get("sandbox_providers")) or [{"name": "docker"}]
    return {"providers": providers, "total_spend_cap_usd": raw.get("total_spend_cap_usd")}


def _load_provider_config(config_path: Path) -> SandboxProviderConfig:
    """The first USABLE entry (enabled, its API keys present), ordered by `priority` then list order."""
    from .registry import usable

    entries = load_provider_entries(config_path)["providers"]
    cfgs = []
    for i, e in enumerate(entries):
        if not isinstance(e, dict):
            raise ConfigError(f"sandbox_providers[{i}] must be a mapping: {config_path}")
        try:
            cfgs.append(SandboxProviderConfig.model_validate(e))
        except Exception as ex:  # pydantic ValidationError
            raise ConfigError(f"invalid sandbox_providers[{i}] in {config_path}: {ex}") from ex
    for cfg in sorted(cfgs, key=lambda c: c.priority):       # sorted() is stable: list order breaks ties
        if usable(cfg)[0]:
            return cfg
    # None is usable. A single-provider config keeps its historical behaviour: return the first entry that
    # is not explicitly disabled, so the provider itself reports the missing key at open() time.
    for cfg in sorted(cfgs, key=lambda c: c.priority):
        if cfg.enabled:
            return cfg
    raise ConfigError(f"every sandbox provider in {config_path} is disabled")


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
            f"install the matching extra (`sandboxes[daytona|e2b|blaxel]`) if you meant one of those"
        )
    return provider_cls(cfg)
