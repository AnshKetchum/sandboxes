"""sandboxes — one API over docker, Daytona, E2B and Blaxel (any provider whose key is in the environment).

    from sandboxes import get_sandbox_provider

    provider = get_sandbox_provider()   # or config_path=..., or SANDBOX_CONFIG_PATH env
    with provider.session(image="python:3.11-bookworm"):
        provider.run_command("echo hi")
"""

from .base import SandboxProvider
from .config import SANDBOX_CONFIG_PATH_ENV, get_sandbox_provider, load_sandbox_providers
from .docker_provider import DockerProvider
from .errors import (
    BuildError,
    CapacityError,
    SandboxCreateError,
    ConfigError,
    PushError,
    SandboxError,
    SandboxNotOpenError,
    SandboxOpenError,
)
from .models import (
    BuildResult,
    CommandResult,
    DependencyEntry,
    PushResult,
    SandboxProviderConfig,
)
from .cost import CostLedger, SpendCapExceeded, get_ledger, summary as cost_summary
from .pricing import DEFAULT_MODELS, CostModel, cost_model_for
from . import fleet
from .raw import ExecResult, LiveSandbox, RawSandbox, SandboxSpec, StoredArtifact
from .registry import PROVIDERS, ProviderInfo, register_provider, usable
from .router import NoProviderAvailable, SandboxRouter
from .parallel import AsyncSandboxPool, Job, JobOutcome, ResourceLimits, RetryPolicy, SandboxPool, Stage

__all__ = [
    "CapacityError", "SandboxCreateError", "CostLedger", "SpendCapExceeded", "get_ledger", "cost_summary",
    "DEFAULT_MODELS", "CostModel", "cost_model_for", "ExecResult", "RawSandbox", "SandboxSpec", "LiveSandbox", "StoredArtifact", "fleet",
    "PROVIDERS", "ProviderInfo", "register_provider", "usable", "NoProviderAvailable", "SandboxRouter",
    "get_sandbox_provider",
    "load_sandbox_providers",
    "SANDBOX_CONFIG_PATH_ENV",
    "SandboxProvider",
    "SandboxProviderConfig",
    "DockerProvider",
    "CommandResult",
    "BuildResult",
    "PushResult",
    "DependencyEntry",
    "SandboxError",
    "ConfigError",
    "SandboxOpenError",
    "SandboxNotOpenError",
    "BuildError",
    "PushError",
    "SandboxPool",
    "AsyncSandboxPool",
    "Job",
    "JobOutcome",
    "Stage",
    "RetryPolicy",
    "ResourceLimits",
]

try:
    from .daytona_provider import DaytonaProvider, kill_sandboxes  # noqa: F401

    __all__ += ["DaytonaProvider", "kill_sandboxes"]
except ImportError:
    pass  # `sandboxes[daytona]` extra not installed
