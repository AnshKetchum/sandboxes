"""Standard ownership labels: how every sandbox, on every provider, says who started it and for which run.

A consuming project sets its identity ONCE per process and never touches a provider:

    import sandboxes
    sandboxes.set_context(owner="euphoria", run="r-20261006-1")

From then on every sandbox the library opens (raw providers, the router, the sync providers, the harbor adapter, docker) carries
    sandboxes.managed=true   sandboxes.owner=<owner>   sandboxes.run=<run>
as labels/metadata/tags (whatever the provider calls them), and `sandboxes ls|stop --owner X --run R` finds them with no provider
logic. Sandboxes created before this scheme carry older markers; `owner_of()` maps the ones we know about so they stay stoppable.
Environment form of the same thing: SANDBOX_OWNER, SANDBOX_RUN_ID.
"""

import os
from typing import Optional

LABEL_MANAGED = "sandboxes.managed"
LABEL_OWNER = "sandboxes.owner"
LABEL_RUN = "sandboxes.run"
OWNER_ENV = "SANDBOX_OWNER"
RUN_ENV = "SANDBOX_RUN_ID"

# markers older code put on sandboxes -> the owner they imply
LEGACY_OWNER_LABELS = {"agentdist": "pr-task-synthesis", "euphoria.managed": "euphoria", "openenv.managed": "pr-task-synthesis"}
# docker container name prefixes older greenhouse/euphoria code used -> owner
LEGACY_DOCKER_PREFIXES = {"gh-": "greenhouse", "eg-gen-": "greenhouse", "gh-solve-": "greenhouse"}


def set_context(owner: Optional[str] = None, run: Optional[str] = None) -> None:
    """Declare who this process is (and which run); later sandboxes are labelled accordingly."""
    if owner:
        os.environ[OWNER_ENV] = owner
    if run:
        os.environ[RUN_ENV] = run


def standard_labels(owner: Optional[str] = None, run: Optional[str] = None, extra: Optional[dict] = None) -> dict:
    """The labels to put on a new sandbox: managed + owner + run (from the arguments, else the process context) + `extra`."""
    labels = {LABEL_MANAGED: "true"}
    owner = owner or os.environ.get(OWNER_ENV)
    run = run or os.environ.get(RUN_ENV)
    if owner:
        labels[LABEL_OWNER] = owner
    if run:
        labels[LABEL_RUN] = run
    labels.update({k: str(v) for k, v in (extra or {}).items()})
    return labels


def owner_of(labels: dict) -> Optional[str]:
    """Who started a sandbox, from its labels (the standard one, else a legacy marker)."""
    if labels.get(LABEL_OWNER):
        return labels[LABEL_OWNER]
    for k, owner in LEGACY_OWNER_LABELS.items():
        if k in labels:
            return owner
    return None


def run_of(labels: dict) -> Optional[str]:
    return labels.get(LABEL_RUN) or labels.get("run") or None


def is_ours(labels: dict) -> bool:
    """True for a sandbox this stack started (standard or legacy marker)."""
    return LABEL_MANAGED in labels or any(k in labels for k in LEGACY_OWNER_LABELS)


LEGACY_BACKEND_NAMES = ("daytona", "e2b", "blaxel", "modal", "fly")


def normalize_backend(name: str) -> str:
    """The pipeline's OPENENV_BACKEND with the old per-provider spellings folded into the one generic backend: `daytona` (or any
    provider name) means `sandboxes` on that provider. Sets SANDBOX_CONFIG_PATH to that provider's shared config when none is set."""
    if name in LEGACY_BACKEND_NAMES:
        os.environ.setdefault("SANDBOX_CONFIG_PATH", f"/ansh/sandboxes/configs/{name}.yaml")
        return "sandboxes"
    return name
