"""Exception hierarchy for the sandboxes package."""

from __future__ import annotations


class SandboxError(Exception):
    """Base class for every error this package raises."""


class ConfigError(SandboxError):
    """A sandbox_providers config file/entry couldn't be parsed or resolved."""


class SandboxNotOpenError(SandboxError):
    """A method that requires an open sandbox (put_file/get_file/run_command/
    add_dependency/close) was called before `.open()` or after `.close()`."""


class SandboxOpenError(SandboxError):
    """`.open()` itself failed (image not found, container/sandbox start
    failed, provider-side timeout, ...)."""


class BuildError(SandboxError):
    """`.build_sandbox_image()` failed."""


class PushError(SandboxError):
    """`.push_sandbox_image()` failed."""


class CapacityError(SandboxError):
    """A provider refused to start another sandbox (concurrency/quota/rate limit). The router treats
    this as "this provider is full right now" and sends the sandbox to another provider."""

    def __init__(self, provider: str, message: str = ""):
        super().__init__(f"{provider}: {message}")
        self.provider = provider


class SandboxCreateError(SandboxOpenError):
    """Every attempt to create a sandbox on a provider failed for a non-capacity reason."""


class ImageBuildError(SandboxError):
    """A provider could not build the sandbox's image from a Dockerfile. Not retried: the same build would fail the same way."""
