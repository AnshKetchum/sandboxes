"""Return types and config schema shared by every SandboxProvider.

These shapes are IDENTICAL across providers — a caller that switches
`sandbox_providers[0].name` from "docker" to "daytona" gets back the same
result types either way, differing only in the values inside them (e.g.
`get_total_runtime_cost()` returning 0.0 for docker, a real metered figure
for daytona).
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class CommandResult(BaseModel):
    """Result of `run_command`."""

    exit_code: Optional[int] = None
    output: str = ""          # combined stdout+stderr, tail-truncated
    timed_out: bool = False


class BuildResult(BaseModel):
    """Result of `build_sandbox_image`."""

    ok: bool
    image: str = ""            # the tag/ref that was built, if ok
    detail: str = ""
    seconds: float = 0.0


class PushResult(BaseModel):
    """Result of `push_sandbox_image`."""

    ok: bool
    image: str = ""            # the fully-qualified ref that was pushed, if ok
    detail: str = ""
    seconds: float = 0.0


class DependencyEntry(BaseModel):
    """One `add_dependency` ledger row — the append-only record consumed
    when reconciling a session's live-installed packages into a rebuilt,
    exportable image (see AddDependency's docstring on SandboxProvider)."""

    step: int
    command: str
    note: str = ""
    added_at: str = ""         # ISO-8601 UTC


class SandboxProviderConfig(BaseModel):
    """One entry of a `sandbox_providers:` YAML list — the SAME shape
    already used by euphoria/petunia configs, so existing config files need
    no translation to be consumed by this package.

    Every field beyond `name` is OPTIONAL: a bare `{name: docker}` is a
    complete, valid config. Fields with no meaning for a given backend are
    simply ignored by it (e.g. `usd_per_hour`/`auto_stop_minutes` are no-ops
    for docker, which is always free and always explicitly torn down).
    """

    name: str = "docker"                          # "docker" | "daytona" | "e2b" | "blaxel" (registry.PROVIDERS)
    enabled: bool = True                          # false: listed but never used
    api_key_env: Optional[list[str]] = None       # env vars that must be set for this provider to be
                                                   # usable; default: the provider's own (registry.py).
                                                   # A listed provider whose keys are absent is skipped.
    priority: int = 0                             # lower is tried first when capacity is equal
    weight: float = 1.0                           # share of new sandboxes when several have room
    pricing: dict = Field(default_factory=dict)   # overrides for pricing.CostModel fields
    capabilities: dict = Field(default_factory=dict)  # e.g. {dind: false} to keep docker-in-docker
                                                   # work off a provider; see registry.DEFAULT_CAPABILITIES
    image: Optional[str] = None                   # default base image to `open()` when
                                                   # none is given explicitly
    disk_gb: Optional[float] = None
    cpu: Optional[float] = None
    memory_mb: Optional[int] = None
    pids_limit: Optional[int] = None
    peak_concurrent_sandboxes: Optional[int] = None
    spend_cap_usd: Optional[float] = None
    auto_snapshot: Optional[bool] = None           # daytona only: whether `open()` snapshots
                                                   # the built image as a NEW persistent
                                                   # Daytona snapshot every call. None (default)
                                                   # preserves the provider's own historical
                                                   # default (snapshot unless the task ships a
                                                   # prebuilt image). A caller that opens many
                                                   # short-lived sandboxes from DIFFERENT/edited
                                                   # Dockerfiles (e.g. a repair loop rebuilding
                                                   # every iteration) should set this False —
                                                   # Daytona's snapshot quota is a small, shared,
                                                   # hard-capped org-wide resource (confirmed
                                                   # live: 30 max), and one snapshot per sandbox
                                                   # open burns through it fast with nothing ever
                                                   # reusing the snapshot anyway.
    auto_stop_minutes: int = 0
    auto_delete_minutes: int = 0
    usd_per_hour: float = 0.0                      # daytona: fallback estimate when the
                                                   # Analytics API is unreachable; 0 for docker
    registry: Optional[str] = None                 # default push target
    extra: dict = Field(default_factory=dict)
