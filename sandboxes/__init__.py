"""sandboxes — one API over docker and Daytona.

    from sandboxes import get_sandbox_provider

    provider = get_sandbox_provider()   # or config_path=..., or SANDBOX_CONFIG_PATH env
    with provider.session(image="python:3.11-bookworm"):
        provider.run_command("echo hi")
"""

from .base import SandboxProvider
from .config import SANDBOX_CONFIG_PATH_ENV, get_sandbox_provider
from .docker_provider import DockerProvider
from .errors import (
    BuildError,
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

__all__ = [
    "get_sandbox_provider",
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
]

try:
    from .daytona_provider import DaytonaProvider, kill_sandboxes  # noqa: F401

    __all__ += ["DaytonaProvider", "kill_sandboxes"]
except ImportError:
    pass  # `sandboxes[daytona]` extra not installed
