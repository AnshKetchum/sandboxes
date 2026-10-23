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
    extra_env: tuple = ()                # optional env vars worth loading from the shared env file (e.g. a region)
    available: Optional[object] = None   # optional callable() -> bool: credentials that are not plain env vars (e.g. a profile file)


# `dind`: can run a docker daemon with real storage inside the sandbox (what pr_task_synthesis needs).
# `image_ref`: opens a sandbox directly from a registry image reference.
# `builds_dockerfile`: RawSandbox builds a task's Dockerfile itself when the spec carries extra["dockerfile"] (+ ["context_dir"]).
# `requires_prebuilt_image`: the provider can only boot an image that is ALREADY in a registry; it cannot build a task's Dockerfile.
#   A consumer must not send it a task without a built image. Every provider declares this explicitly (tests enforce it).
# `build_timeout_s`: a build running longer than this is cancelled and fails (one E2B build hung for 35+ minutes holding a build slot).
# `max_concurrent_builds`: the provider limits how many image/template builds run at once (e2b: 20); the harbor environment gates its builds to this many, host-wide.
# `create_rate_per_s` / `create_burst`: the provider's published limit on creating sandboxes (see ratelimit.py), shared by all processes on a host:
#   modal  5/s       the API's own error ("Sandbox creation rate limit (5/s) exceeded"); its docs give no figure
#   e2b    5/s       Pro plan (Hobby 1/s)                         docs.e2b.dev/sandbox/rate-limits
#   fly    1/s, 3    create-machine: 1 req/s per action, short bursts to 3     fly.io/docs/machines/api/working-with-machines-api
#   daytona 5/s      tier 1: 300 sandbox creations/min (tiers 2-4: 400/500/600) www.daytona.io/docs/en/limits
#   blaxel -         no per-second creation limit published (concurrency tiers only)
DEFAULT_CAPABILITIES = {
    "daytona": {"create_rate_per_s": 5, "create_burst": 5, "dind": True, "image_ref": True, "requires_prebuilt_image": False, "persistent_disk_gb": 10},
    "e2b": {"create_rate_per_s": 5, "create_burst": 5, "max_concurrent_builds": 15, "build_timeout_s": 900, "dind": True, "image_ref": True, "requires_prebuilt_image": False, "persistent_disk_gb": 20},
    "blaxel": {"dind": False, "image_ref": True, "requires_prebuilt_image": False, "persistent_disk_gb": 0,
               "note": "root fs is RAM-backed (50% of memory); docker only fits in a small loop file"},
    "docker": {"dind": False, "image_ref": True, "requires_prebuilt_image": False, "persistent_disk_gb": 0, "local": True},
    "fly": {"create_rate_per_s": 1, "create_burst": 3, "dind": True, "image_ref": True, "requires_prebuilt_image": False, "builds_dockerfile": True, "persistent_disk_gb": 8, "note": "Firecracker microVM, real disk (8 GB rootfs by default)"},
    "modal": {"create_rate_per_s": 5, "create_burst": 5, "dind": True, "image_ref": True, "requires_prebuilt_image": False, "builds_dockerfile": True, "persistent_disk_gb": 500, "note": "runtime=vm (real VM, large root disk); gvisor cannot run a daemon"},
    #   gke    20/s, 40  our own cluster: bounded by the API server, not a vendor quota (tune per cluster size)
    "gke": {"create_rate_per_s": 20, "create_burst": 40, "dind": False, "image_ref": True, "requires_prebuilt_image": True, "persistent_disk_gb": 10,
            "note": "one pod per sandbox on our own GKE cluster; registry images only (build and push first); nodes billed via gke_meter"},
}


def _modal_credentials() -> bool:
    return bool(os.environ.get("MODAL_TOKEN_ID") and os.environ.get("MODAL_TOKEN_SECRET")) or os.path.exists(os.path.expanduser("~/.modal.toml"))


def _gke_credentials() -> bool:
    """A kubeconfig to reach the cluster (`gcloud container clusters get-credentials` writes one)."""
    paths = [p for p in os.environ.get("KUBECONFIG", "").split(os.pathsep) if p] or [os.path.expanduser("~/.kube/config")]
    return any(os.path.exists(p) for p in paths)

PROVIDERS: dict[str, ProviderInfo] = {
    "docker": ProviderInfo("docker", (), None, "sandboxes.docker_provider:DockerProvider", DEFAULT_CAPABILITIES["docker"]),
    "daytona": ProviderInfo("daytona", ("DAYTONA_API_KEY",), "sandboxes.backends.daytona_raw:DaytonaRaw",
                            "sandboxes.daytona_provider:DaytonaProvider", DEFAULT_CAPABILITIES["daytona"]),
    "e2b": ProviderInfo("e2b", ("E2B_API_KEY",), "sandboxes.backends.e2b_raw:E2BRaw",
                        "sandboxes.generic_provider:E2BProvider", DEFAULT_CAPABILITIES["e2b"]),
    "blaxel": ProviderInfo("blaxel", ("BL_API_KEY", "BL_WORKSPACE"), "sandboxes.backends.blaxel_raw:BlaxelRaw",
                           "sandboxes.generic_provider:BlaxelProvider", DEFAULT_CAPABILITIES["blaxel"], extra_env=("BL_REGION",)),
    "fly": ProviderInfo("fly", ("FLY_API_KEY",), "sandboxes.backends.fly_raw:FlyRaw", "sandboxes.generic_provider:FlyProvider", DEFAULT_CAPABILITIES["fly"]),
    "modal": ProviderInfo("modal", (), "sandboxes.backends.modal_raw:ModalRaw", "sandboxes.generic_provider:ModalProvider",
                          DEFAULT_CAPABILITIES["modal"], available=_modal_credentials),
    "gke": ProviderInfo("gke", (), "sandboxes.backends.gke_raw:GKERaw", "sandboxes.generic_provider:GKEProvider",
                        DEFAULT_CAPABILITIES["gke"], extra_env=("GKE_NAMESPACE", "GKE_KUBE_CONTEXT", "SANDBOX_GKE_VCPU_HOUR"),
                        available=_gke_credentials),
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


def requires_prebuilt_image(cfg: SandboxProviderConfig) -> bool:
    """True when `cfg`'s provider cannot build a task's Dockerfile and so needs a built image (a config's `capabilities:` may override).
    Undeclared counts as True: the failure mode is skipping a provider, not failing every task it is sent."""
    return bool(capabilities(cfg).get("requires_prebuilt_image", True))
