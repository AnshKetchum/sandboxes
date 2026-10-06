"""Which providers exist, what each needs in the environment, and what each can do.

A provider is USABLE when it is enabled in the config AND every env var it needs is set. That is the
whole contract for adding a provider to a run: list it in `sandbox_providers:` and export its key.

    PROVIDERS["e2b"]      -> ProviderInfo(env_keys=("E2B_API_KEY",), raw=..., sync=...)
    usable(cfg)           -> (True, "") or (False, "E2B_API_KEY not set")
"""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass, field
from typing import Optional

from .models import SandboxProviderConfig


@dataclass(frozen=True)
class ProviderInfo:
    name: str
    env_keys: tuple = ()
    raw: Optional[str] = None            # "module:Class" of the async RawSandbox (None: local only)
    sync: Optional[str] = None           # "module:Class" of the sync SandboxProvider
    capabilities: dict = field(default_factory=dict)
    available: Optional[object] = None   # optional callable() -> bool: credentials that are not plain env vars (e.g. a profile file)


# `dind`: can run a docker daemon with real storage inside the sandbox (what pr_task_synthesis needs).
# `image_ref`: opens a sandbox directly from a registry image reference.
DEFAULT_CAPABILITIES = {
    "daytona": {"dind": True, "image_ref": True, "persistent_disk_gb": 10},
    "e2b": {"dind": True, "image_ref": True, "persistent_disk_gb": 20},
    "blaxel": {"dind": False, "image_ref": True, "persistent_disk_gb": 0,
               "note": "root fs is RAM-backed (50% of memory); docker only fits in a small loop file"},
    "docker": {"dind": False, "image_ref": True, "persistent_disk_gb": 0, "local": True},
    "fly": {"dind": True, "image_ref": True, "persistent_disk_gb": 8, "note": "Firecracker microVM, real disk (8 GB rootfs by default)"},
    "modal": {"dind": True, "image_ref": True, "persistent_disk_gb": 500, "note": "runtime=vm (real VM, large root disk); gvisor cannot run a daemon"},
}


def _modal_credentials() -> bool:
    return bool(os.environ.get("MODAL_TOKEN_ID") and os.environ.get("MODAL_TOKEN_SECRET")) or os.path.exists(os.path.expanduser("~/.modal.toml"))

PROVIDERS: dict[str, ProviderInfo] = {
    "docker": ProviderInfo("docker", (), None, "sandboxes.docker_provider:DockerProvider", DEFAULT_CAPABILITIES["docker"]),
    "daytona": ProviderInfo("daytona", ("DAYTONA_API_KEY",), "sandboxes.backends.daytona_raw:DaytonaRaw",
                            "sandboxes.daytona_provider:DaytonaProvider", DEFAULT_CAPABILITIES["daytona"]),
    "e2b": ProviderInfo("e2b", ("E2B_API_KEY",), "sandboxes.backends.e2b_raw:E2BRaw",
                        "sandboxes.generic_provider:E2BProvider", DEFAULT_CAPABILITIES["e2b"]),
    "blaxel": ProviderInfo("blaxel", ("BL_API_KEY", "BL_WORKSPACE"), "sandboxes.backends.blaxel_raw:BlaxelRaw",
                           "sandboxes.generic_provider:BlaxelProvider", DEFAULT_CAPABILITIES["blaxel"]),
    "fly": ProviderInfo("fly", ("FLY_API_KEY",), "sandboxes.backends.fly_raw:FlyRaw", "sandboxes.generic_provider:FlyProvider", DEFAULT_CAPABILITIES["fly"]),
    "modal": ProviderInfo("modal", (), "sandboxes.backends.modal_raw:ModalRaw", "sandboxes.generic_provider:ModalProvider",
                          DEFAULT_CAPABILITIES["modal"], _modal_credentials),
}


def register_provider(info: ProviderInfo) -> None:
    """Add (or replace) a provider. A new provider needs only its env keys and a RawSandbox class."""
    PROVIDERS[info.name] = info


def load_class(path: str):
    mod, cls = path.split(":")
    return getattr(importlib.import_module(mod), cls)


def required_env(cfg: SandboxProviderConfig) -> tuple:
    if cfg.api_key_env is not None:
        return tuple(cfg.api_key_env)
    info = PROVIDERS.get((cfg.name or "").lower())
    return info.env_keys if info else ()


def usable(cfg: SandboxProviderConfig, env=None) -> tuple[bool, str]:
    """(True, "") when `cfg` can be used now, else (False, reason)."""
    env = os.environ if env is None else env
    name = (cfg.name or "").lower()
    if name not in PROVIDERS:
        return False, f"unknown provider {name!r} (known: {', '.join(sorted(PROVIDERS))})"
    if not cfg.enabled:
        return False, "disabled in config"
    missing = [k for k in required_env(cfg) if not env.get(k)]
    if missing:
        return False, f"{', '.join(missing)} not set"
    check = PROVIDERS[name].available
    if check is not None and cfg.api_key_env is None and not check():
        return False, "no credentials (set the provider's token env vars or log in with its CLI)"
    return True, ""


def capabilities(cfg: SandboxProviderConfig) -> dict:
    base = dict(PROVIDERS[cfg.name.lower()].capabilities) if cfg.name.lower() in PROVIDERS else {}
    base.update(cfg.capabilities or {})
    return base
