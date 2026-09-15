"""SandboxProvider — the single abstract interface every backend implements
with IDENTICAL method signatures, so callers can switch `sandbox_providers[0].
name` between "docker" and "daytona" without touching any calling code.

Design notes (from the standardization discussion this package came out of):

- `open`/`close` bookend every other call. `close` is not optional to call —
  it's what stops `get_total_runtime_cost`'s clock and releases the
  underlying resource (a leaked-open Daytona sandbox keeps billing
  indefinitely, bounded only by its own auto_stop/auto_delete TTL as a
  backstop, never a substitute for actually closing). `session()` below
  wraps `open`+`close` in a context manager so a caller can't forget.
- `build_sandbox_image`/`open` are DELIBERATELY not required to be as tightly
  coupled for every backend as they are for docker. Docker cleanly separates
  "build once" (cacheable, content-addressed) from "open many times" (cheap,
  no rebuild). Daytona's SDK does not offer that decoupling today — every
  Daytona sandbox is built-and-started together from a Dockerfile-bearing
  directory, every time `open()` is called. Rather than force a false
  symmetry, `image` means different things per backend: an already-built
  image tag for DockerProvider, an environment directory for DaytonaProvider.
  `build_sandbox_image`/`push_sandbox_image` still work for Daytona (via an
  ephemeral buildah-in-sandbox build, since Daytona sandboxes are
  unprivileged and can't run a docker daemon) — they just produce a portable,
  registry-pushed image for OTHER consumers (e.g. DockerProvider.open`ing it
  later), not something Daytona's own `open()` consumes directly.
- `run_solution`/`precheck`-style compound workflows are DELIBERATELY not
  primitives here — they compose cleanly from `put_file`/`run_command`/
  `get_file`, and each consuming codebase is expected to implement that
  composition itself for now.
- `add_dependency` installs live via `run_command` immediately (so an agent
  session sees the package right away, no rebuild-and-restart tax per call)
  AND appends to `self.dependencies` — an ordered, append-only ledger. That
  ledger is the input to a LATER, explicit `build_sandbox_image` call (with a
  Dockerfile re-rendered from it) if a caller needs the dependency to survive
  into a real, exportable image — reconciliation is the caller's job, this
  class only guarantees the ledger is accurate and ordered.
- Fleet-level ops (list/bulk-close-by-label, image/container pruning) are
  OUT OF SCOPE for this class on purpose — they operate across many
  sandboxes at once, not the one instance a SandboxProvider owns, and belong
  in a separate module layered on top of this one.
"""

from __future__ import annotations

import contextlib
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import ClassVar, Iterator, Optional, Union

from .models import BuildResult, CommandResult, DependencyEntry, PushResult, SandboxProviderConfig


class SandboxProvider(ABC):
    """One sandbox's lifecycle + operations. One instance = one sandbox.

    Usage:
        provider = get_sandbox_provider()          # config.py factory
        provider.open(image="python:3.11-bookworm")
        try:
            provider.run_command("echo hi")
        finally:
            provider.close()

        # or, equivalently, guaranteed-close:
        with provider.session(image="python:3.11-bookworm"):
            provider.run_command("echo hi")
    """

    name: ClassVar[str] = "base"

    def __init__(self, config: Optional[SandboxProviderConfig] = None):
        self.config = config or SandboxProviderConfig(name=self.name)
        self._dependencies: list[DependencyEntry] = []
        self._is_open: bool = False
        # wall-clock bookends for get_total_runtime_cost; a subclass's open()/
        # close() should set these via time.monotonic() (not wall time, so a
        # system clock adjustment mid-session can't skew a cost calculation).
        self._opened_at: Optional[float] = None
        self._closed_at: Optional[float] = None

    # -- lifecycle ------------------------------------------------------

    @abstractmethod
    def open(
        self,
        image: str,
        *,
        workdir: str = "/",
        network: str = "bridge",
        volumes: Optional[dict[str, str]] = None,
        name_prefix: Optional[str] = None,
    ) -> None:
        """Start the sandbox. `image` is an already-built image tag for
        DockerProvider; an environment directory (containing a Dockerfile)
        for DaytonaProvider — see the module docstring for why these differ.
        Must be safe to retry on a known-transient provider error (an
        implementation's job, not the caller's — see open_task_sandbox_retrying
        precedent this class's docstring cites).

        `volumes`: {host_path: "container_path[:mode]"} bind mounts (mode
        "rw" if omitted) — a DOCKER-ONLY capability, present in every
        provider's signature for parity but meaningful only for
        DockerProvider (a remote Daytona sandbox has no local host
        filesystem to mount); DaytonaProvider raises SandboxOpenError if
        given a non-empty `volumes`. Exists because a caller with a
        long-lived rw workspace (e.g. an agent writing many files over a
        session) wants direct host I/O, not a put_file/get_file round trip
        per write — the same reason greenhouse's own gen-stage sandbox
        bind-mounts its workspace instead of copying into it.

        `name_prefix`: an optional caller-supplied identifier (e.g. a run
        id) folded into the sandbox's own name — `<name_prefix>-<random>`
        for DockerProvider's container name, `<name_prefix>__<random>`
        prepended to DaytonaProvider's session id — so a caller can later
        find/kill everything belonging to one job via `docker ps`/`daytona
        list` without needing a separate label mechanism. None (the
        default) keeps each provider's original unprefixed naming."""

    @abstractmethod
    def close(self) -> None:
        """Tear the sandbox down. Idempotent — safe to call on an
        already-closed (or never-opened) provider."""

    @contextlib.contextmanager
    def session(self, image: str, **open_kwargs) -> Iterator["SandboxProvider"]:
        """`open()` then guarantee `close()` runs, even on an exception."""
        self.open(image, **open_kwargs)
        try:
            yield self
        finally:
            self.close()

    @property
    def is_open(self) -> bool:
        return self._is_open

    # -- files ------------------------------------------------------------

    @abstractmethod
    def put_file(
        self,
        local_path: Union[str, Path],
        remote_path: str,
        *,
        mode: str = "file",
        permissions: Optional[str] = None,
    ) -> None:
        """Copy `local_path` into the sandbox at `remote_path`.

        `mode`: "file" (default) copies a single file; "dir" recursively
        copies a directory tree — the metadata section this class's design
        discussion agreed put_file needed, rather than a second primitive.
        `permissions` (e.g. "755", "1777") is applied after the copy when
        given; backends that can't preserve ownership across the copy
        mechanism (docker cp always writes root:root) rely on this to make
        the result usable by a non-root sandbox user."""

    @abstractmethod
    def get_file(self, remote_path: str, local_path: Union[str, Path]) -> None:
        """Copy `remote_path` out of the sandbox to `local_path`, in full —
        never truncated the way a `run_command("cat ...")` capture is."""

    # -- exec ---------------------------------------------------------------

    @abstractmethod
    def run_command(
        self,
        command: str,
        *,
        timeout: float = 300.0,
        cwd: Optional[str] = None,
    ) -> CommandResult:
        """Run `command` as `bash -lc <command>` in the sandbox (explicit
        shell, never whatever a backend might default to, so `&&`/`;`/
        redirects behave identically across backends)."""

    # -- dependencies ---------------------------------------------------

    @abstractmethod
    def add_dependency(self, command: str, *, note: str = "") -> None:
        """Run `command` (e.g. "pip install pyyaml") live, right now, via
        `run_command` — AND append it to `self.dependencies`. Does not
        rebuild the sandbox image; see the module docstring for why, and
        `dependencies` for how a caller reconciles this into a durable image
        later."""

    @property
    def dependencies(self) -> list[DependencyEntry]:
        """The ordered, append-only ledger of every `add_dependency` call
        this session has made."""
        return list(self._dependencies)

    # -- image build / push ----------------------------------------------

    @abstractmethod
    def build_sandbox_image(
        self,
        context_dir: Union[str, Path],
        *,
        tag: str,
        timeout: float = 1800.0,
        dockerfile: Optional[str] = None,
    ) -> BuildResult:
        """Build `context_dir` (a directory containing a Dockerfile, or
        `dockerfile` naming one elsewhere in it) into an image called `tag`.
        Does not require an open sandbox — this operates on the build
        backend directly, independent of any `open()` call."""

    @abstractmethod
    def push_sandbox_image(
        self,
        tag: str,
        *,
        registry: Optional[str] = None,
        timeout: float = 600.0,
    ) -> PushResult:
        """Push a previously-`build_sandbox_image`'d `tag` to `registry`
        (falling back to `self.config.registry` when omitted)."""

    # -- cost -------------------------------------------------------------

    @abstractmethod
    def get_total_runtime_cost(self) -> float:
        """USD cost of this sandbox's runtime so far (0.0 for a backend with
        no metering, e.g. docker). Well-defined between `open()` and
        `close()`; keeps returning the final total after `close()`."""

    def get_elapsed_seconds(self) -> float:
        """Wall-clock time since `open()`, common to every backend (docker
        and daytona both set `_opened_at`/`_closed_at` identically, via
        `time.monotonic()`) — 0.0 before `open()` is ever called, frozen at
        its final value after `close()`, still counting up while open."""
        if self._opened_at is None:
            return 0.0
        end = self._closed_at if self._closed_at is not None else time.monotonic()
        return end - self._opened_at

    def stats(self) -> dict:
        """{"seconds": <elapsed wall time>, "cost_usd": <get_total_runtime_cost()>}
        — the one place this dict is assembled, so every provider reports it
        identically. A subclass never needs to build this itself: it only
        has to keep `_opened_at`/`_closed_at` current (already required for
        `get_total_runtime_cost`) and implement `get_total_runtime_cost` —
        `stats()` just proxies both onto whichever concrete provider it's
        called on."""
        return {
            "seconds": self.get_elapsed_seconds(),
            "cost_usd": self.get_total_runtime_cost(),
        }
