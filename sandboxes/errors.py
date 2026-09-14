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
