"""DaytonaProvider — remote, metered sandbox backend via harbor's
`DaytonaEnvironment`. Only importable when the `sandboxes[daytona]` extra is
installed (`daytona-sdk` + `harbor[daytona]`); `config.py` degrades to
docker-only when it isn't.

Requires `DAYTONA_API_KEY` (and any other daytona-sdk env vars) already set
in the process environment before `open()` is called — this package
deliberately does not read a `.env` file itself (that convention belongs to
whatever codebase embeds this one, not to a generic sandbox library).

Ported from the equivalent, production-proven logic in
`euphoria/api.py` (`_DaytonaTaskSandbox`, `_open_task_sandbox_daytona`,
`_daytona_sync`/`_daytona_loop_get`) and `euphoria/env.py` (`make_env`,
`start_env`, `is_retryable_start_error`) — same shared-background-event-loop
pattern (a Daytona/harbor coroutine run from an arbitrary caller thread via
`run_coroutine_threadsafe`, NOT `asyncio.run`/`run_until_complete`, which
breaks the moment two worker threads race it — confirmed live, not
theoretical), same non-root-upload-vs-root-owned-directory chmod handling,
same retryable-sandbox-start-error classification.

Accepted, documented asymmetry with DockerProvider (see base.py's module
docstring): `open(image=...)` here takes a harbor-style task directory
(containing `task.toml` + `environment/Dockerfile`), not an image tag —
Daytona's SDK builds-and-starts together from that directory every call,
with no separate "start from an already-built image" primitive today.
`build_sandbox_image`/`push_sandbox_image` still work (via an ephemeral
buildah-in-sandbox build — Daytona sandboxes are unprivileged, no docker
daemon to `docker build` against) but produce a portable, registry-pushed
image for OTHER consumers, not something this class's own `open()` reads.
"""

from __future__ import annotations

import asyncio
import atexit
import base64
import os
import posixpath
import shlex
import tarfile
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar, Optional, Union

from .base import SandboxProvider
from .errors import BuildError, PushError, SandboxNotOpenError, SandboxOpenError
from .models import BuildResult, CommandResult, DependencyEntry, PushResult, SandboxProviderConfig

# Bound how many sandbox builds+starts are in flight at once, process-wide.
# A pipeline wave launching dozens at the same instant made Daytona queue the
# builds behind each other until the slowest ones hit the start timeout
# (6 simultaneous timeouts at one wave start, all "just needed more time").
_OPEN_GATE = threading.BoundedSemaphore(
    max(1, int(os.environ.get("DAYTONA_MAX_CONCURRENT_OPENS", "24"))))
_START_TIMEOUT_S = int(os.environ.get("DAYTONA_START_TIMEOUT_S", "600"))
# Uploads of a prepared package/source tree into a fresh sandbox exceeded the
# old fixed 120s under org-wide load (several gen sessions lost per wave).
_UPLOAD_TIMEOUT_S = int(os.environ.get("DAYTONA_UPLOAD_TIMEOUT_S", "360"))

SANDBOX_LABELS = {"sandboxes.managed": "true"}
_SANDBOX_START_ATTEMPTS = 3
_OUTPUT_TAIL = 12000


def _tail(text: str, n: int = _OUTPUT_TAIL) -> str:
    return text if len(text) <= n else text[-n:]


# -- a persistent background event loop, shared by every DaytonaProvider in
# this process, exactly the pattern euphoria/api.py's _daytona_loop_get
# built to fix a real bug (two OS threads racing run_until_complete on the
# same loop raises "this event loop is already running") ------------------

_loop_lock = threading.Lock()
_loop: Optional[asyncio.AbstractEventLoop] = None


def _get_shared_loop() -> asyncio.AbstractEventLoop:
    global _loop
    with _loop_lock:
        if _loop is None:
            loop = asyncio.new_event_loop()

            def _run() -> None:
                asyncio.set_event_loop(loop)
                loop.run_forever()

            threading.Thread(target=_run, name="sandboxes-daytona-loop", daemon=True).start()
            _loop = loop
            atexit.register(_atexit_cleanup)
        return _loop


def _atexit_cleanup() -> None:
    try:
        from harbor.environments.daytona.environment import DaytonaClientManager

        async def _cleanup():
            mgr = await DaytonaClientManager.get_instance()
            await mgr._cleanup()

        _run_sync(_cleanup(), timeout=15)
    except Exception:
        pass  # best-effort — never let shutdown cleanup raise


def _run_sync(coro, timeout: Optional[float] = None):
    """Run a Daytona/harbor coroutine to completion from ANY thread, safely."""
    loop = _get_shared_loop()
    fut = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return fut.result(timeout=timeout)
    except TimeoutError as e:
        fut.cancel()  # best-effort; the coroutine may be past its last cancellation point
        # Bare TimeoutError's str() is empty — confirmed live to cause real
        # harm downstream: a repair agent seeing only "daytona sandbox start
        # failed: " with nothing after it has no way to distinguish "this
        # timed out" from any other failure, and was repeatedly misdiagnosing
        # it as "no network available" (a plausible-sounding but wrong guess)
        # instead of "this needed more time," stripping real dependencies
        # from Dockerfiles that would have built fine given longer. Give it
        # an unambiguous message instead.
        raise TimeoutError(
            f"operation exceeded its {timeout}s timeout without completing "
            "(this is a timeout, not a network/connectivity error — if this "
            "recurs for the same task, the build/operation likely just needs "
            "more time, not different dependencies)"
        ) from e


def _is_retryable_start_error(e: Exception) -> bool:
    from daytona import DaytonaError
    from harbor.environments.base import SandboxBuildFailedError

    if isinstance(e, SandboxBuildFailedError):
        return True
    if isinstance(e, DaytonaError):
        m = str(e).lower()
        return "entered error state" in m or "sandboxstate.error" in m or "build_failed" in m
    return False


async def _start_env(env, attempts: int = _SANDBOX_START_ATTEMPTS) -> None:
    for attempt in range(1, attempts + 1):
        try:
            await env.start(force_build=False)
            return
        except Exception as e:
            if not _is_retryable_start_error(e) or attempt == attempts:
                raise
            try:
                await env.stop(delete=True)
            except Exception:
                pass
            await asyncio.sleep(min(60, 15 * attempt))


async def _fetch_analytics_cost(sandbox, organization_id: str, sandbox_id: str,
                                 start: datetime, end: datetime) -> Optional[float]:
    """Sum the provider-computed `price` field of every usage period the
    Analytics API reports for `sandbox` over [start, end] — the real,
    metered cost, not an estimate. `sandbox` is the raw `daytona.AsyncSandbox`
    (harbor's `DaytonaEnvironment._sandbox`); this mirrors its own private
    `_get_analytics_api_url`/`_build_analytics_telemetry_api` pattern
    (see `AsyncSandbox.get_metrics`) but hits `UsageApi`'s
    per-sandbox-usage endpoint instead of `TelemetryApi`'s metrics one.
    Returns None (never raises) if the deployment has no separate Analytics
    API, or on any request failure — callers fall back to the usd_per_hour
    estimate in that case."""
    analytics_api_url = await sandbox._get_analytics_api_url()
    if not analytics_api_url:
        return None

    from daytona_analytics_api_client_async import ApiClient as AnalyticsApiClient
    from daytona_analytics_api_client_async import Configuration as AnalyticsConfiguration
    from daytona_analytics_api_client_async import UsageApi

    client = AnalyticsApiClient(AnalyticsConfiguration(host=analytics_api_url))
    client.default_headers["Authorization"] = sandbox._sandbox_api.api_client.default_headers["Authorization"]
    usage_api = UsageApi(client)
    try:
        periods = await usage_api.organization_organization_id_sandbox_sandbox_id_usage_get(
            organization_id, sandbox_id,
            var_from=start.isoformat(), to=end.isoformat(),
        )
    finally:
        await client.close()
    return round(sum(p.price for p in periods if p.price is not None), 6)


def _session_id(name_prefix: Optional[str], short_name: str) -> str:
    """DaytonaEnvironment's session_id, optionally prefixed with a caller
    id (e.g. a run id) — same intent as DockerProvider's container-name
    prefix. Extracted as its own function so the prefixing logic is
    testable without going through a real (or heavily mocked) open()."""
    prefix = f"{name_prefix}__" if name_prefix else ""
    return f"{prefix}{short_name}__{uuid.uuid4().hex[:10]}"


async def _list_sandboxes_async(name_contains: Optional[str] = None) -> list:
    from daytona import ListSandboxesQuery
    from harbor.environments.daytona.environment import DaytonaClientManager

    client = await (await DaytonaClientManager.get_instance()).get_client()
    out = []
    async for sb in client.list(ListSandboxesQuery(labels=dict(SANDBOX_LABELS))):
        # harbor sets this label itself (DaytonaEnvironment._sandbox_labels)
        # from the session_id we build in _session_id() — the same
        # `name_prefix` (e.g. a run id) DockerProvider folds into its
        # container name, so callers can find/kill one job's sandboxes the
        # same way on either backend, no separate label scheme needed.
        session_id = (sb.labels or {}).get("harbor.session_id", "")
        if name_contains is None or name_contains in session_id:
            out.append(sb)
    return out


def find_sandboxes(name_contains: Optional[str] = None) -> list:
    """Every currently-visible sandbox this package's providers tagged with
    SANDBOX_LABELS, optionally filtered to those whose session id (the
    harbor.session_id label) contains `name_contains` — e.g. a specific run
    id. Returns raw SDK sandbox objects (id/labels/state available). Explicitly
    NOT a SandboxProvider method — this acts across every sandbox, not the
    one instance a provider owns (see base.py's module docstring)."""
    return _run_sync(_list_sandboxes_async(name_contains), timeout=60)


def kill_sandboxes(*, name_contains: Optional[str] = None) -> int:
    """Fleet cleanup: delete every sandbox this package's providers tagged
    with SANDBOX_LABELS — optionally scoped to those whose session id
    contains `name_contains` (e.g. one run's id), rather than every sandbox
    ANY consumer of this package has ever opened (euphoria/petunia/
    greenhouse all share the same SANDBOX_LABELS). Explicitly NOT a
    SandboxProvider method — see base.py's module docstring on why fleet
    ops live outside this class."""
    from harbor.environments.daytona.environment import DaytonaClientManager

    async def _delete_matching() -> int:
        client = await (await DaytonaClientManager.get_instance()).get_client()
        n = 0
        for sb in await _list_sandboxes_async(name_contains):
            try:
                await client.delete(sb)
                n += 1
            except Exception:
                pass
        return n

    return _run_sync(_delete_matching(), timeout=180)


class DaytonaProvider(SandboxProvider):
    name: ClassVar[str] = "daytona"

    def __init__(self, config: Optional[SandboxProviderConfig] = None):
        super().__init__(config)
        self._env = None
        self._workdir: str = "/"
        # Wall-clock bookends alongside base.py's time.monotonic() ones —
        # the Analytics API's usage endpoint takes real RFC3339 timestamps,
        # not monotonic offsets.
        self._opened_at_wall: Optional[datetime] = None
        self._closed_at_wall: Optional[datetime] = None

    # -- lifecycle ------------------------------------------------------

    def open(
        self,
        image: str,
        *,
        workdir: str = "/",
        network: str = "bridge",
        volumes: Optional[dict[str, str]] = None,
        name_prefix: Optional[str] = None,
    ) -> None:
        """`image` is a harbor-style task directory (task.toml +
        environment/Dockerfile) — see the module docstring for why this
        differs from DockerProvider's already-built-tag contract.
        `volumes` is accepted for signature parity only — a remote Daytona
        sandbox has no local host filesystem to bind-mount; a non-empty
        value raises rather than silently doing nothing. `name_prefix`
        (e.g. a run id) is prepended to the Daytona session id, same intent
        as DockerProvider's container-name prefix — findable later via
        whatever the sandbox listing surfaces the session id through."""
        if self._is_open:
            raise SandboxOpenError("already open — call close() first")
        from .guard import check_admission
        check_admission("daytona", provider_cap_usd=self.config.spend_cap_usd)      # SpendCapExceeded before anything is created
        if volumes:
            raise SandboxOpenError(
                "DaytonaProvider does not support host bind mounts (volumes=) "
                "— a remote sandbox has no local host filesystem to mount; "
                "use put_file/get_file instead"
            )
        if not os.environ.get("DAYTONA_API_KEY"):
            raise SandboxOpenError(
                "DAYTONA_API_KEY is not set — the daytona SDK needs it in the "
                "process environment before open() is called (this package "
                "deliberately doesn't read a .env file itself; set it "
                "yourself first)"
            )

        try:
            from harbor.models.task.config import NetworkMode, NetworkPolicy
            from harbor.models.task.task import Task
            from harbor.models.trial.paths import TrialPaths
        except ImportError as e:
            raise SandboxOpenError(
                "daytona support not installed — `pip install sandboxes[daytona]`"
            ) from e

        task_dir = Path(image).resolve()
        try:
            task = Task(task_dir, disable_verification=True)
        except Exception as e:
            raise SandboxOpenError(
                f"{task_dir} is not a valid task directory "
                f"(need task.toml + environment/Dockerfile): {e}"
            ) from e

        trial_dir = Path(tempfile.mkdtemp(prefix="sbx-daytona-"))
        paths = TrialPaths(trial_dir=trial_dir)
        paths.mkdir()

        prebuilt = bool(task.config.environment.docker_image)
        cfg = self.config
        auto_snapshot = (not prebuilt) if cfg.auto_snapshot is None else cfg.auto_snapshot
        kw = dict(
            environment_dir=task.paths.environment_dir,
            environment_name=task.short_name,
            session_id=_session_id(name_prefix, task.short_name),
            trial_paths=paths,
            task_env_config=task.config.environment,
            override_storage_mb=int((cfg.disk_gb or 8) * 1024),
            auto_snapshot=auto_snapshot,
            labels=__import__("sandboxes.labels", fromlist=["standard_labels"]).standard_labels(
                run=name_prefix if name_prefix and not os.environ.get("SANDBOX_RUN_ID") else None, extra=SANDBOX_LABELS),
            auto_stop_interval_mins=cfg.auto_stop_minutes,
            auto_delete_interval_mins=cfg.auto_delete_minutes,
        )
        if cfg.cpu:
            kw["override_cpus"] = int(cfg.cpu)
        if cfg.memory_mb:
            kw["override_memory_mb"] = int(cfg.memory_mb)
        if network == "none":
            kw["network_policy"] = NetworkPolicy(network_mode=NetworkMode.NONE)

        from harbor.environments.daytona.environment import DaytonaEnvironment

        env = DaytonaEnvironment(**kw)
        try:
            # 600s -> 240s -> 400s. 600s let genuinely HUNG env.start() calls
            # (never responds at all) sit dead for a full 10 minutes before a
            # bare, empty-str() TimeoutError fired — cut to 240s to bound
            # that. But 240s then turned out too tight the OTHER direction:
            # confirmed live (greenhouse, 2026-09-28), MULTIPLE distinct
            # candidates with genuinely heavy dependency stacks (pandas,
            # scipy, sklearn, opencv, pydicom, pyarrow...) started timing out
            # mid-build — same empty-str() TimeoutError, indistinguishable
            # from a real hang, so the repair agent (seeing nothing but an
            # empty error) kept guessing "no network" and stripping real
            # dependencies instead of the true fix (more time). A real build
            # taking 3-4 minutes for a heavy scientific-Python stack is
            # normal, not a hang. 400s splits the difference: still a real
            # bound on truly-hung opens (33% cut from 600s), while giving
            # legitimate heavy builds enough room to actually finish.
            # 2026-09-29: back to 600s (env-overridable) now that starts are
            # gated by _OPEN_GATE — at 400s, 199/318 test iterations in one
            # run died as build timeouts, largely queueing behind each other.
            with _OPEN_GATE:
                _run_sync(_start_env(env), timeout=_START_TIMEOUT_S)
        except Exception as e:
            raise SandboxOpenError(f"daytona sandbox start failed: {e}") from e

        self._env = env
        self._workdir = workdir or "/"
        from .cost import get_ledger
        self._ledger_handle = get_ledger().open("daytona", cpu=float(cfg.cpu or 1), ram_gib=(cfg.memory_mb or 2048) / 1024.0,
                                                disk_gib=float(cfg.disk_gb or 8), labels={"prefix": name_prefix or ""})
        self._is_open = True
        self._opened_at = time.monotonic()
        self._closed_at = None
        self._opened_at_wall = datetime.now(timezone.utc)
        self._closed_at_wall = None

    def close(self) -> None:
        if not self._is_open:
            return
        try:
            _run_sync(self._env.stop(delete=True), timeout=60)
        except Exception:
            pass  # best-effort — a sandbox that fails to stop still hits its
                   # own auto_stop/auto_delete TTL as a backstop
        self._closed_at = time.monotonic()
        self._closed_at_wall = datetime.now(timezone.utc)
        handle, self._ledger_handle = getattr(self, "_ledger_handle", None), None
        if handle:
            from .cost import get_ledger
            from .pricing import cost_model_for
            get_ledger().close(handle, cost_model_for("daytona", self.config.pricing))
        self._is_open = False

    def _require_open(self):
        if not self._is_open or self._env is None:
            raise SandboxNotOpenError("sandbox is not open — call open() first")
        return self._env

    @property
    def raw_env(self):
        """The wrapped `harbor.environments.daytona.environment.
        DaytonaEnvironment` this provider opened — an escape hatch for a
        caller that needs harbor's own richer, async, environment-native
        surface (`ensure_dirs`, `scoped_exec_env`, `download_dir`, the
        `agent.run(env, ...)` contract harbor's own agent framework expects)
        that `run_command`/`put_file`/`get_file` deliberately don't cover.

        Exists specifically so a caller like euphoria — whose job body is
        fundamentally async and built on harbor's agent framework, not this
        package's own sync primitives — can still get its sandbox OPENED
        (with this package's own retry/backoff) and admitted/scheduled
        through `sandboxes.parallel`'s shared pool, without rewriting a
        single line of its actual agent-running code. Raises the same
        SandboxNotOpenError as every other accessor if called before open()
        or after close()."""
        return self._require_open()

    # -- files ------------------------------------------------------------

    def put_file(
        self,
        local_path: Union[str, Path],
        remote_path: str,
        *,
        mode: str = "file",
        permissions: Optional[str] = None,
    ) -> None:
        env = self._require_open()
        default_perm = "755" if mode == "file" else "777"
        perm = permissions or default_perm
        parent = remote_path if mode == "dir" else posixpath.dirname(remote_path)
        if parent:
            # our own exec runs as root (bash -lc); the SDK's upload_file/
            # upload_dir go through the sandbox's toolbox API as a non-root
            # user — a root-owned dir lets it traverse but not write into,
            # so the upload 400s with "permission denied" even though mkdir
            # itself just succeeded.
            self.run_command(f"mkdir -p {shlex.quote(parent)} && chmod {perm} {shlex.quote(parent)}")
        if mode == "dir":
            _run_sync(env.upload_dir(str(local_path), remote_path), timeout=_UPLOAD_TIMEOUT_S)
        else:
            _run_sync(env.upload_file(str(local_path), remote_path), timeout=_UPLOAD_TIMEOUT_S)
        self.run_command(f"chmod -R {perm} {shlex.quote(remote_path)}")

    def get_file(self, remote_path: str, local_path: Union[str, Path]) -> None:
        env = self._require_open()
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        _run_sync(env.download_file(remote_path, str(local_path)), timeout=90)

    # -- exec ---------------------------------------------------------------

    def run_command(
        self,
        command: str,
        *,
        timeout: float = 300.0,
        cwd: Optional[str] = None,
    ) -> CommandResult:
        env = self._require_open()
        workdir = cwd or self._workdir
        try:
            r = _run_sync(
                env.exec(f"bash -lc {shlex.quote(command)}", cwd=workdir, timeout_sec=timeout),
                timeout=timeout + 30,
            )
        except TimeoutError:
            return CommandResult(exit_code=None, output="", timed_out=True)
        return CommandResult(
            exit_code=getattr(r, "return_code", None),
            output=_tail((getattr(r, "stdout", "") or "") + (getattr(r, "stderr", "") or "")),
        )

    # -- dependencies ---------------------------------------------------

    def add_dependency(self, command: str, *, note: str = "") -> None:
        self.run_command(command)
        self._dependencies.append(DependencyEntry(
            step=len(self._dependencies),
            command=command,
            note=note,
            added_at=datetime.now(timezone.utc).isoformat(),
        ))

    # -- image build / push (buildah-in-sandbox; see module docstring) ---

    def build_sandbox_image(
        self,
        context_dir: Union[str, Path],
        *,
        tag: str,
        timeout: float = 1800.0,
        dockerfile: Optional[str] = None,
    ) -> BuildResult:
        """Build `context_dir` with buildah inside an ephemeral, unprivileged
        Daytona sandbox (there's no docker daemon to `docker build` against
        remotely) — same approach as greenhouse's `scripts/_daytona_builder.py`.
        Requires a builder image with buildah + CA certs preinstalled,
        named by `self.config.extra["builder_image"]`."""
        t0 = time.monotonic()
        builder_image = self.config.extra.get("builder_image")
        if not builder_image:
            return BuildResult(ok=False, image=tag,
                               detail="config.extra['builder_image'] is required "
                                      "(a task directory with buildah preinstalled) "
                                      "for daytona builds",
                               seconds=0.0)
        context_dir = Path(context_dir)
        df_name = dockerfile or "Dockerfile"

        with tempfile.TemporaryDirectory() as td:
            archive = Path(td) / "ctx.tar.gz"
            with tarfile.open(archive, "w:gz") as tf:
                tf.add(context_dir, arcname=".")

            builder = DaytonaProvider(self.config)
            try:
                builder.open(builder_image)
                builder.run_command("mkdir -p /build")
                builder.put_file(archive, "/build/ctx.tar.gz")
                extract = builder.run_command(
                    "cd /build && tar -xzf ctx.tar.gz && rm ctx.tar.gz", timeout=120
                )
                if extract.exit_code != 0:
                    return BuildResult(ok=False, image=tag, detail=extract.output,
                                       seconds=time.monotonic() - t0)
                build_cmd = (
                    f"cd /build && buildah bud --storage-driver vfs --isolation chroot "
                    f"-f {shlex.quote(df_name)} -t {shlex.quote(tag)} . "
                    f"> /build/build.log 2>&1"
                )
                r = builder.run_command(build_cmd, timeout=timeout)
                if r.exit_code != 0:
                    log = builder.run_command("tail -c 4000 /build/build.log")
                    return BuildResult(ok=False, image=tag,
                                       detail=_tail(log.output or r.output, 3000),
                                       seconds=time.monotonic() - t0)
                return BuildResult(ok=True, image=tag, detail="built (buildah)",
                                   seconds=time.monotonic() - t0)
            finally:
                builder.close()

    def push_sandbox_image(
        self,
        tag: str,
        *,
        registry: Optional[str] = None,
        timeout: float = 600.0,
    ) -> PushResult:
        """Push a `build_sandbox_image`d tag from the SAME builder sandbox
        (buildah's storage — vfs driver — is local to the sandbox that built
        it; pushing has to happen from there, not a fresh one)."""
        t0 = time.monotonic()
        builder_image = self.config.extra.get("builder_image")
        if not builder_image:
            return PushResult(ok=False, image=tag,
                              detail="config.extra['builder_image'] is required for daytona push",
                              seconds=0.0)
        reg = registry or self.config.registry
        ref = f"{reg}/{tag}" if reg and not tag.startswith(reg) else tag
        builder = DaytonaProvider(self.config)
        try:
            builder.open(builder_image)
            r = builder.run_command(
                f"buildah push --storage-driver vfs {shlex.quote(tag)} docker://{shlex.quote(ref)}",
                timeout=timeout,
            )
            ok = r.exit_code == 0
            return PushResult(ok=ok, image=ref if ok else "", detail=r.output,
                              seconds=time.monotonic() - t0)
        finally:
            builder.close()

    # -- cost -------------------------------------------------------------

    def _real_cost(self) -> Optional[float]:
        """Query Daytona's Analytics API for this sandbox's actual metered
        cost so far. Returns None (never raises) whenever that isn't
        possible — no sandbox opened yet, the SDK's internal shape changed,
        the deployment has no separate Analytics API, or the request
        failed — so `get_total_runtime_cost` can fall back to the
        usd_per_hour estimate."""
        env = self._env
        if env is None or self._opened_at_wall is None:
            return None
        sandbox = getattr(env, "_sandbox", None)
        if sandbox is None:
            return None
        organization_id = getattr(sandbox, "organization_id", None)
        sandbox_id = getattr(sandbox, "id", None)
        if not organization_id or not sandbox_id:
            return None
        end_wall = self._closed_at_wall or datetime.now(timezone.utc)
        try:
            return _run_sync(
                _fetch_analytics_cost(sandbox, organization_id, sandbox_id,
                                       self._opened_at_wall, end_wall),
                timeout=20,
            )
        except Exception:
            return None

    def get_total_runtime_cost(self) -> float:
        real = self._real_cost()
        if real is not None:
            return real
        rate = self.config.usd_per_hour or 0.0
        if rate <= 0 or self._opened_at is None:
            return 0.0
        hours = self.get_elapsed_seconds() / 3600.0
        return round(hours * rate, 6)
