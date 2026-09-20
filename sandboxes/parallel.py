"""SandboxPool — the shared "run N jobs safely against M sandboxes" shell
every consumer of this package (euphoria, greenhouse, petunia, recursive-
synth-replication, terminal-universe) was independently reimplementing a
piece of. Built by comparing all five: euphoria had the best sandbox-open
retry/backoff and spend-cap admission; greenhouse had the only host-
capacity (disk/mem) admission guard and the only two-tier concurrency split
protecting sandbox-bound work from a wide LLM/search-bound pool; terminal-
universe had the cleanest shape for that two-tier split (ThreadPoolExecutor
+ an inner Semaphore for the expensive resource); petunia and recursive-
synth-replication had none of their own — they free-ride on euphoria/
petunia's CLI today, and inherit this by depending on whichever of those
adopts it.

`SandboxPool` owns sandbox lifecycle (open, with retry/backoff; close,
always) and admission (host capacity, spend cap, sandbox-bound width). It
deliberately owns NOTHING about what a job DOES — that's `job_fn`, entirely
the caller's. A multi-stage pipeline (mine -> ideate -> gen -> test) is
still the caller's own chain; call `.run()` once per stage, not once for
the whole pipeline — see `force_wide` below for stages that don't need a
sandbox at all.

Threading, not multiprocessing: three of the four codebases surveyed
already use `ThreadPoolExecutor` successfully; euphoria's own process pool
was specifically about per-job RAM in ITS workload, not a universal
requirement, so that stays a caller concern (e.g. wrap job_fn's own
CPU/RAM-heavy piece in a process pool if a specific job type needs it) —
this module's job is the sandbox/admission shell, not a RAM ceiling.
"""

from __future__ import annotations

import asyncio
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from .base import SandboxProvider
from .config import get_sandbox_provider
from .errors import SandboxError, SandboxOpenError

# on_event(job_id, status, detail); status one of:
#   run():       admission_wait | start | sandbox_open | sandbox_retry | running | done | failed
#   run_chain(): admission_wait | sandbox_open | sandbox_retry |
#                stage_start | stage_done | stage_failed (detail = stage name,
#                or "stage: error" for stage_failed)
EventHook = Callable[[str, str, str], None]


def _noop_event(job_id: str, status: str, detail: str) -> None:
    pass


@dataclass
class Job:
    id: str
    payload: Any = None


@dataclass
class JobOutcome:
    job_id: str
    ok: bool
    result: Any = None
    error: Optional[str] = None
    sandbox_open_attempts: int = 1
    seconds: float = 0.0
    # run_chain() only: which stage broke the chain, if any (None for run()
    # and for a chain that completed every stage).
    failed_stage: Optional[str] = None
    # run_chain() only: {stage_name: result} for every stage that completed
    # before a failure (or all of them, on success).
    stage_results: dict = field(default_factory=dict)


@dataclass
class Stage:
    """One step of a `run_chain()` job. `sandboxed=True` opens (with retry)
    and closes its OWN sandbox for just this stage's `fn` call — not shared
    with any other stage in the chain, matching e.g. greenhouse's gen/test
    each getting a fresh sandbox while mine/ideate get none at all.
    `resource_class`, if set, additionally gates this stage behind a named
    semaphore from `SandboxPool(resource_classes=...)` — for a resource
    that needs its OWN width distinct from both the sandbox pool and the
    overall wide pool (e.g. greenhouse's DDGS-backed research stages,
    capped separately after a real thundering-herd incident: 500 concurrent
    tasks all hitting mine/ideate at once overwhelmed the shared web-search
    subsystem even though sandbox-bound `gen` was healthy and idle)."""

    name: str
    fn: Callable[[Optional[SandboxProvider], "Job"], Any]
    sandboxed: bool = False
    resource_class: Optional[str] = None


def _default_is_retryable(e: Exception) -> bool:
    """Failure signatures confirmed live, none caught by either provider's
    own internal retry (DaytonaProvider.open() already retries its own
    BUILD_FAILED-class errors; this is a DEFENSE-IN-DEPTH outer layer for
    everything neither provider's internal retry knows about):
      - Docker: "no available IPv4 addresses" — the default bridge network's
        address pool exhausted (a host-config ceiling, not a real capacity
        limit; retrying after a slot frees up is the correct response, not
        failing the job outright).
      - Daytona: an empty-message "sandbox start failed" — the signature of
        a transient account-level quota burst (observed: sandbox-CREATE
        rate limit during a concurrency ramp-up, stopped entirely once past
        the initial burst); also the same BUILD_FAILED/"entered error
        state" text DaytonaProvider already matches internally, kept here
        too so a caller with its OWN provider construction (bypassing this
        pool) still benefits.
    """
    msg = str(e)
    if "no available IPv4 addresses" in msg:
        return True
    if isinstance(e, SandboxOpenError) and not msg.strip():
        return True
    lowered = msg.lower()
    return any(s in lowered for s in ("entered error state", "sandboxstate.error", "build_failed"))


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    backoff_fn: Callable[[int], float] = field(
        default=lambda attempt: min(60.0, 15.0 * attempt)  # euphoria's own formula
    )
    is_retryable: Callable[[Exception], bool] = field(default=_default_is_retryable)


@dataclass
class ResourceLimits:
    """Host-capacity admission floors — lifted verbatim from greenhouse's
    run/rescue.py (built after a real incident: a concurrency=100 run hit
    ENOSPC across dozens of tasks within about a minute). Checked before
    EVERY job admission, sandbox-bound or not — the incident that motivated
    this was a non-sandbox (download-heavy) stage, not a sandbox one."""

    min_free_disk_gb: float = 20.0
    min_free_mem_gb: float = 8.0
    check_interval_s: float = 5.0
    max_wait_s: float = 900.0


def _free_disk_gb(path: Path) -> float:
    # disk_path may not exist yet (e.g. an output dir nothing has written to
    # yet) — shutil.disk_usage requires a real path, so walk up to the
    # nearest existing ancestor rather than crash the whole admission
    # check. Every path eventually reaches an ancestor that exists (at
    # worst "/"), so this always terminates.
    p = Path(path)
    while not p.exists():
        parent = p.parent
        if parent == p:
            break
        p = parent
    return shutil.disk_usage(p).free / (1024 ** 3)


def _free_mem_gb() -> Optional[float]:
    try:
        with open("/proc/meminfo", "r", encoding="ascii") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / (1024 ** 2)
    except (FileNotFoundError, PermissionError, ValueError, IndexError):
        return None
    return None


def _wait_for_capacity(
    limits: ResourceLimits, *, disk_path: Path, on_wait: Callable[[str], None],
) -> None:
    deadline = time.monotonic() + max(0.0, limits.max_wait_s)
    warned = False
    while True:
        free_disk = _free_disk_gb(disk_path)
        free_mem = _free_mem_gb()
        disk_low = free_disk < limits.min_free_disk_gb
        mem_low = free_mem is not None and free_mem < limits.min_free_mem_gb
        if not disk_low and not mem_low:
            return
        if time.monotonic() >= deadline:
            on_wait(
                f"giving up after {limits.max_wait_s:.0f}s wait (free disk {free_disk:.1f}GB, "
                f"free mem {'?' if free_mem is None else f'{free_mem:.1f}GB'}) — proceeding anyway"
            )
            return
        if not warned:
            reason = []
            if disk_low:
                reason.append(f"disk {free_disk:.1f}GB < {limits.min_free_disk_gb:.0f}GB")
            if mem_low:
                reason.append(f"mem {free_mem:.1f}GB < {limits.min_free_mem_gb:.0f}GB")
            on_wait(f"waiting on host capacity ({', '.join(reason)})")
            warned = True
        time.sleep(max(0.1, limits.check_interval_s))


async def _await_capacity(
    limits: ResourceLimits, *, disk_path: Path, on_wait: Callable[[str], None],
) -> None:
    """Same check as _wait_for_capacity, `await asyncio.sleep` instead of
    `time.sleep` so it never blocks the event loop — the disk/mem reads
    themselves are fast syscalls, fine to call directly without a thread."""
    deadline = time.monotonic() + max(0.0, limits.max_wait_s)
    warned = False
    while True:
        free_disk = _free_disk_gb(disk_path)
        free_mem = _free_mem_gb()
        disk_low = free_disk < limits.min_free_disk_gb
        mem_low = free_mem is not None and free_mem < limits.min_free_mem_gb
        if not disk_low and not mem_low:
            return
        if time.monotonic() >= deadline:
            on_wait(
                f"giving up after {limits.max_wait_s:.0f}s wait (free disk {free_disk:.1f}GB, "
                f"free mem {'?' if free_mem is None else f'{free_mem:.1f}GB'}) — proceeding anyway"
            )
            return
        if not warned:
            reason = []
            if disk_low:
                reason.append(f"disk {free_disk:.1f}GB < {limits.min_free_disk_gb:.0f}GB")
            if mem_low:
                reason.append(f"mem {free_mem:.1f}GB < {limits.min_free_mem_gb:.0f}GB")
            on_wait(f"waiting on host capacity ({', '.join(reason)})")
            warned = True
        await asyncio.sleep(max(0.1, limits.check_interval_s))


class SandboxPool:
    def __init__(
        self,
        config_path: str | Path | None,
        *,
        concurrency: Optional[int] = None,
        wide_concurrency: Optional[int] = None,
        resources: Optional[ResourceLimits] = None,
        spend_cap_usd: Optional[float] = None,
        retry: Optional[RetryPolicy] = None,
        disk_path: str | Path = ".",
        resource_classes: Optional[dict[str, int]] = None,
    ) -> None:
        self.config_path = config_path
        template = get_sandbox_provider(config_path)  # cheap: parses config, opens nothing
        self.concurrency = concurrency or template.config.peak_concurrent_sandboxes or 4
        self.wide_concurrency = wide_concurrency or self.concurrency * 8
        self.resources = resources or ResourceLimits()
        self.spend_cap_usd = (
            spend_cap_usd if spend_cap_usd is not None else template.config.spend_cap_usd
        )
        self.retry = retry or RetryPolicy()
        self.disk_path = Path(disk_path)
        self._spend_lock = threading.Lock()
        self._spend_total = 0.0
        # Named additional semaphores `run_chain()` stages can opt into via
        # `Stage(resource_class=...)` — e.g. {"search": 6} for a shared,
        # rate-limited API every stage's own work might hit, independent of
        # both the sandbox semaphore and the overall wide pool width.
        self._resource_sems = {
            name: threading.Semaphore(max(1, width))
            for name, width in (resource_classes or {}).items()
        }

    def _spend_tripped(self) -> bool:
        if not self.spend_cap_usd:
            return False
        with self._spend_lock:
            return self._spend_total >= self.spend_cap_usd

    def _record_spend(self, amount: float) -> None:
        if amount <= 0:
            return
        with self._spend_lock:
            self._spend_total += amount

    def _admission_ok(self, job: Job, *, on_event: EventHook) -> Optional[str]:
        """Blocks on host-capacity admission (self-throttling, see
        _wait_for_capacity); returns a rejection reason string if the spend
        cap is already tripped, else None (proceed). Shared by run() and
        run_chain() so both get identical admission behavior."""
        _wait_for_capacity(
            self.resources, disk_path=self.disk_path,
            on_wait=lambda msg: on_event(job.id, "admission_wait", msg),
        )
        if self.spend_cap_usd and self._spend_tripped():
            on_event(job.id, "admission_wait", f"spend cap ${self.spend_cap_usd} reached")
            return "spend cap reached"
        return None

    def _open_with_retry(self, image: str, *, job_id: str, on_event: EventHook) -> tuple[SandboxProvider, int]:
        provider = get_sandbox_provider(self.config_path)
        last_err: Optional[Exception] = None
        for attempt in range(1, self.retry.max_attempts + 1):
            try:
                provider.open(image, name_prefix=job_id)
                return provider, attempt
            except Exception as e:  # noqa: BLE001 — classified below, not swallowed
                last_err = e
                if attempt >= self.retry.max_attempts or not self.retry.is_retryable(e):
                    raise
                on_event(job_id, "sandbox_retry",
                         f"attempt {attempt}/{self.retry.max_attempts} failed: {e}")
                time.sleep(self.retry.backoff_fn(attempt))
        raise last_err or SandboxError("sandbox open failed with no recorded error")

    def run(
        self,
        jobs: list[Job],
        job_fn: Callable[[Optional[SandboxProvider], Job], Any],
        *,
        on_event: EventHook = _noop_event,
        force_wide: Optional[Callable[[Job], bool]] = None,
    ) -> dict[str, JobOutcome]:
        """Run `job_fn(provider, job)` for each job, `wide_concurrency`-wide
        overall, `concurrency`-wide for sandbox-bound work. `force_wide(job)`
        returning True skips the sandbox entirely for that job (job_fn gets
        `provider=None`) — for a mixed pipeline where only some stages need
        one. One job's exception never aborts the others (mark-and-continue);
        every job gets a `JobOutcome` back regardless of how it ended."""
        if not jobs:
            return {}
        sandbox_sem = threading.Semaphore(max(1, self.concurrency))
        template = get_sandbox_provider(self.config_path)
        image = template.config.image

        def needs_sandbox(job: Job) -> bool:
            return force_wide is None or not force_wide(job)

        def _run_one(job: Job) -> JobOutcome:
            t0 = time.monotonic()
            _wait_for_capacity(
                self.resources, disk_path=self.disk_path,
                on_wait=lambda msg: on_event(job.id, "admission_wait", msg),
            )
            if self.spend_cap_usd and self._spend_tripped():
                on_event(job.id, "admission_wait", f"spend cap ${self.spend_cap_usd} reached")
                return JobOutcome(job_id=job.id, ok=False, error="spend cap reached",
                                  seconds=time.monotonic() - t0)
            on_event(job.id, "start", "")
            use_sandbox = needs_sandbox(job)
            if not use_sandbox:
                try:
                    on_event(job.id, "running", "")
                    result = job_fn(None, job)
                    on_event(job.id, "done", "")
                    return JobOutcome(job_id=job.id, ok=True, result=result,
                                      seconds=time.monotonic() - t0)
                except Exception as e:  # noqa: BLE001 — isolate: one job's bug can't kill the pool
                    on_event(job.id, "failed", str(e))
                    return JobOutcome(job_id=job.id, ok=False, error=str(e),
                                      seconds=time.monotonic() - t0)
            with sandbox_sem:
                provider = None
                attempts = 1
                try:
                    on_event(job.id, "sandbox_open", "")
                    if not image:
                        raise SandboxError(
                            f"sandbox config ({self.config_path!r}) has no `image` set"
                        )
                    provider, attempts = self._open_with_retry(image, job_id=job.id, on_event=on_event)
                    on_event(job.id, "running", "")
                    result = job_fn(provider, job)
                    on_event(job.id, "done", "")
                    return JobOutcome(job_id=job.id, ok=True, result=result,
                                      sandbox_open_attempts=attempts, seconds=time.monotonic() - t0)
                except Exception as e:  # noqa: BLE001 — isolate: one job's bug can't kill the pool
                    on_event(job.id, "failed", str(e))
                    return JobOutcome(job_id=job.id, ok=False, error=str(e),
                                      sandbox_open_attempts=attempts, seconds=time.monotonic() - t0)
                finally:
                    if provider is not None:
                        self._record_spend(provider.get_total_runtime_cost())
                        try:
                            provider.close()
                        except Exception:
                            pass

        outcomes: dict[str, JobOutcome] = {}
        workers = min(max(1, self.wide_concurrency), len(jobs))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sbx-pool") as ex:
            futures = {ex.submit(_run_one, job): job.id for job in jobs}
            for fut in as_completed(futures):
                job_id = futures[fut]
                try:
                    outcomes[job_id] = fut.result()
                except Exception as e:  # noqa: BLE001 — future itself must never propagate
                    outcomes[job_id] = JobOutcome(job_id=job_id, ok=False, error=str(e))
        return outcomes

    def run_chain(
        self,
        jobs: list[Job],
        chain: list[Stage],
        *,
        on_event: EventHook = _noop_event,
    ) -> dict[str, JobOutcome]:
        """Run every stage of `chain`, in order, for each job — all in ONE
        thread per job, so a fast job's later stages are NEVER held up by a
        slow sibling still on an earlier stage (no barrier between stages,
        unlike calling `run()` once per stage). This is the model a per-task
        pipeline like mine->ideate->gen->test needs: some stages sandboxed,
        some not, each job progressing through all of them independently.

        Still `wide_concurrency`-wide overall and `concurrency`-wide for
        whichever stages set `sandboxed=True` (a stage opens its OWN fresh
        sandbox — with the same retry/backoff as `run()` — for just its own
        `fn` call, then closes it before the next stage starts; sandboxes
        are never held across stages). A stage with `resource_class` set
        additionally gates behind that named semaphore (see `Stage`'s own
        docstring). Mark-and-continue at the STAGE level: the first stage
        to raise stops that job's remaining stages (recorded in
        `JobOutcome.failed_stage`) without affecting any other job."""
        if not jobs:
            return {}
        sandbox_sem = threading.Semaphore(max(1, self.concurrency))
        template = get_sandbox_provider(self.config_path)
        image = template.config.image

        def _run_stage(stage: Stage, job: Job) -> Any:
            if not stage.sandboxed:
                return stage.fn(None, job)
            if not image:
                raise SandboxError(f"sandbox config ({self.config_path!r}) has no `image` set")
            with sandbox_sem:
                on_event(job.id, "sandbox_open", stage.name)
                provider, _attempts = self._open_with_retry(image, job_id=job.id, on_event=on_event)
                try:
                    return stage.fn(provider, job)
                finally:
                    self._record_spend(provider.get_total_runtime_cost())
                    try:
                        provider.close()
                    except Exception:
                        pass

        def _run_one(job: Job) -> JobOutcome:
            t0 = time.monotonic()
            stage_results: dict = {}
            for stage in chain:
                # Checked before EVERY stage, not just once at job entry —
                # the incident this guards against (greenhouse: a long
                # run's disk filling up hours in, discovered while OTHER
                # tasks were already deep into their own chains) means a
                # job admitted early can still need to be throttled before
                # a LATER stage starts real new work (a download, a fresh
                # sandbox).
                rejected = self._admission_ok(job, on_event=on_event)
                if rejected:
                    return JobOutcome(job_id=job.id, ok=False, error=rejected, failed_stage=stage.name,
                                      stage_results=stage_results, seconds=time.monotonic() - t0)
                gate = self._resource_sems.get(stage.resource_class) if stage.resource_class else None
                # stage_start/stage_done/stage_failed carry the stage name
                # as `detail` — a caller reconstructing per-stage progress
                # UI (e.g. greenhouse's own "▶ t-0001 mine …" / "✓ ... done
                # 5s" lines) keys off (job_id, stage_name) the same way
                # orchestrator.py's old _echo_event already did.
                on_event(job.id, "stage_start", stage.name)
                try:
                    if gate is not None:
                        with gate:
                            stage_results[stage.name] = _run_stage(stage, job)
                    else:
                        stage_results[stage.name] = _run_stage(stage, job)
                except Exception as e:  # noqa: BLE001 — isolate: one job's bug can't kill the pool
                    on_event(job.id, "stage_failed", f"{stage.name}: {e}")
                    return JobOutcome(job_id=job.id, ok=False, error=str(e), failed_stage=stage.name,
                                      stage_results=stage_results, seconds=time.monotonic() - t0)
                on_event(job.id, "stage_done", stage.name)
            return JobOutcome(job_id=job.id, ok=True, result=stage_results, stage_results=stage_results,
                              seconds=time.monotonic() - t0)

        outcomes: dict[str, JobOutcome] = {}
        workers = min(max(1, self.wide_concurrency), len(jobs))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sbx-chain") as ex:
            futures = {ex.submit(_run_one, job): job.id for job in jobs}
            for fut in as_completed(futures):
                job_id = futures[fut]
                try:
                    outcomes[job_id] = fut.result()
                except Exception as e:  # noqa: BLE001 — future itself must never propagate
                    outcomes[job_id] = JobOutcome(job_id=job_id, ok=False, error=str(e))
        return outcomes


class AsyncSandboxPool:
    """Async counterpart to SandboxPool, for a job body that's fundamentally
    async and needs a sandbox's own native, richer surface rather than the
    sync run_command/put_file/get_file primitives — the shape euphoria's
    evaluate.py needs (harbor's agent framework calls `agent.run(env, ...)`
    directly against a raw environment object, not through this package's
    own sync methods). Get that object via `provider.raw_env` inside
    `job_fn` (DaytonaProvider/DockerProvider both expose it — see their own
    docstrings) instead of calling run_command/put_file/get_file yourself;
    this pool still owns open()/close()/retry/admission/concurrency exactly
    like SandboxPool does, via `asyncio.to_thread` bridging the existing
    sync provider calls (nothing in DockerProvider/DaytonaProvider needed
    duplicating in an async form for this).

    Concurrency shape matches euphoria's own proven pattern (see evaluate.
    py's pull_jobs/jobq), not a thread-per-job wrapper: `wide_concurrency`
    coroutines pull off ONE shared asyncio.Queue (work-stealing — a job
    that finishes fast doesn't wait on a slower sibling's chunk), each
    additionally bounded by the `concurrency`-wide sandbox semaphore for
    sandbox-bound work. One process, not euphoria's own outer multiprocess
    tier — that split was specifically about euphoria's own per-job RAM
    profile, a caller concern this pool doesn't need an opinion on; wrap
    N of these (or N `run()`-driving processes) in your own process pool
    if a specific workload needs it."""

    def __init__(
        self,
        config_path: str | Path | None,
        *,
        concurrency: Optional[int] = None,
        wide_concurrency: Optional[int] = None,
        resources: Optional[ResourceLimits] = None,
        spend_cap_usd: Optional[float] = None,
        retry: Optional[RetryPolicy] = None,
        disk_path: str | Path = ".",
    ) -> None:
        self.config_path = config_path
        template = get_sandbox_provider(config_path)
        self.concurrency = concurrency or template.config.peak_concurrent_sandboxes or 4
        self.wide_concurrency = wide_concurrency or self.concurrency * 8
        self.resources = resources or ResourceLimits()
        self.spend_cap_usd = (
            spend_cap_usd if spend_cap_usd is not None else template.config.spend_cap_usd
        )
        self.retry = retry or RetryPolicy()
        self.disk_path = Path(disk_path)
        self.image = template.config.image
        self._spend_total = 0.0
        # Constructed lazily (asyncio.Semaphore/Lock bind to whichever loop
        # is running on first real `await` in older Pythons) rather than in
        # __init__, which may run before any loop exists.
        self._sandbox_sem: Optional[asyncio.Semaphore] = None
        self._spend_lock: Optional[asyncio.Lock] = None

    def _primitives(self) -> tuple[asyncio.Semaphore, asyncio.Lock]:
        if self._sandbox_sem is None:
            self._sandbox_sem = asyncio.Semaphore(max(1, self.concurrency))
            self._spend_lock = asyncio.Lock()
        return self._sandbox_sem, self._spend_lock

    async def _open_with_retry(
        self, image: str, *, job_id: str, on_event: EventHook,
    ) -> tuple[SandboxProvider, int]:
        provider = get_sandbox_provider(self.config_path)
        last_err: Optional[Exception] = None
        for attempt in range(1, self.retry.max_attempts + 1):
            try:
                await asyncio.to_thread(provider.open, image, name_prefix=job_id)
                return provider, attempt
            except Exception as e:  # noqa: BLE001 — classified below, not swallowed
                last_err = e
                if attempt >= self.retry.max_attempts or not self.retry.is_retryable(e):
                    raise
                on_event(job_id, "sandbox_retry",
                         f"attempt {attempt}/{self.retry.max_attempts} failed: {e}")
                await asyncio.sleep(self.retry.backoff_fn(attempt))
        raise last_err or SandboxError("sandbox open failed with no recorded error")

    async def _run_one(
        self,
        job: Job,
        job_fn: Callable[[Optional[SandboxProvider], Job], Awaitable[Any]],
        *,
        on_event: EventHook,
        force_wide: Optional[Callable[[Job], bool]],
    ) -> JobOutcome:
        """One job's full admission -> (optional sandbox) -> job_fn ->
        cleanup lifecycle. Shared by run() and run_from_queue() so the two
        entry points can never drift apart on this — only how jobs REACH
        this method (a pre-built list vs. a live external queue) differs
        between them."""
        sandbox_sem, spend_lock = self._primitives()
        t0 = time.monotonic()
        await _await_capacity(
            self.resources, disk_path=self.disk_path,
            on_wait=lambda msg: on_event(job.id, "admission_wait", msg),
        )
        if self.spend_cap_usd:
            async with spend_lock:
                tripped = self._spend_total >= self.spend_cap_usd
            if tripped:
                on_event(job.id, "admission_wait", f"spend cap ${self.spend_cap_usd} reached")
                return JobOutcome(job_id=job.id, ok=False, error="spend cap reached",
                                  seconds=time.monotonic() - t0)
        on_event(job.id, "start", "")
        if force_wide is not None and force_wide(job):
            try:
                on_event(job.id, "running", "")
                result = await job_fn(None, job)
                on_event(job.id, "done", "")
                return JobOutcome(job_id=job.id, ok=True, result=result,
                                  seconds=time.monotonic() - t0)
            except Exception as e:  # noqa: BLE001 — isolate: one job's bug can't kill the pool
                on_event(job.id, "failed", str(e))
                return JobOutcome(job_id=job.id, ok=False, error=str(e),
                                  seconds=time.monotonic() - t0)
        async with sandbox_sem:
            provider = None
            attempts = 1
            try:
                on_event(job.id, "sandbox_open", "")
                if not self.image:
                    raise SandboxError(
                        f"sandbox config ({self.config_path!r}) has no `image` set"
                    )
                provider, attempts = await self._open_with_retry(
                    self.image, job_id=job.id, on_event=on_event)
                on_event(job.id, "running", "")
                result = await job_fn(provider, job)
                on_event(job.id, "done", "")
                return JobOutcome(job_id=job.id, ok=True, result=result,
                                  sandbox_open_attempts=attempts, seconds=time.monotonic() - t0)
            except Exception as e:  # noqa: BLE001 — isolate: one job's bug can't kill the pool
                on_event(job.id, "failed", str(e))
                return JobOutcome(job_id=job.id, ok=False, error=str(e),
                                  sandbox_open_attempts=attempts, seconds=time.monotonic() - t0)
            finally:
                if provider is not None:
                    cost = await asyncio.to_thread(provider.get_total_runtime_cost)
                    if cost > 0:
                        async with spend_lock:
                            self._spend_total += cost
                    try:
                        await asyncio.to_thread(provider.close)
                    except Exception:
                        pass

    async def run(
        self,
        jobs: list[Job],
        job_fn: Callable[[Optional[SandboxProvider], Job], Awaitable[Any]],
        *,
        on_event: EventHook = _noop_event,
        force_wide: Optional[Callable[[Job], bool]] = None,
    ) -> dict[str, JobOutcome]:
        """Await `job_fn(provider, job)` for each job — `wide_concurrency`
        coroutines pulling off a shared queue, `concurrency`-wide for
        sandbox-bound work. Same semantics as SandboxPool.run() otherwise
        (force_wide, mark-and-continue, admission/spend-cap gating,
        retry/backoff on open) — see its own docstring."""
        if not jobs:
            return {}
        queue: asyncio.Queue = asyncio.Queue()
        for job in jobs:
            queue.put_nowait(job)
        outcomes: dict[str, JobOutcome] = {}

        async def _puller() -> None:
            while True:
                try:
                    job = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                outcomes[job.id] = await self._run_one(
                    job, job_fn, on_event=on_event, force_wide=force_wide)

        workers = min(max(1, self.wide_concurrency), len(jobs))
        await asyncio.gather(*(_puller() for _ in range(workers)))
        return outcomes

    async def run_from_queue(
        self,
        next_job: Callable[[], Optional[Job]],
        job_fn: Callable[[Optional[SandboxProvider], Job], Awaitable[Any]],
        *,
        on_result: Callable[[Job, JobOutcome], None],
        on_event: EventHook = _noop_event,
        force_wide: Optional[Callable[[Job], bool]] = None,
        workers: Optional[int] = None,
    ) -> None:
        """Like run(), but pulls jobs one at a time from `next_job()` (a
        blocking callable returning the next Job, or None when there's no
        more work — e.g. a `multiprocessing.Queue.get` wrapped to return
        None on that queue's own sentinel) instead of a pre-materialized
        list, and reports each JobOutcome to `on_result` as soon as it
        finishes instead of returning a batch dict at the end.

        Built for a cross-process work-stealing model like euphoria's own
        (see evaluate.py's pull_jobs/jobq): several OS processes each run
        this against the SAME shared queue, so a process whose job finishes
        early pulls the next one immediately rather than waiting on its
        process-mates — draining `next_job()` into a list up front and
        calling `run()` instead would break exactly that property, since a
        fast-starting process could grab far more than its fair share
        before slower processes even start pulling.

        `next_job`/`on_result` are plain blocking callables (not
        coroutines) — both are bridged via `asyncio.to_thread`, so a real
        `multiprocessing.Queue.get`/`.put` (which DOES block) is exactly
        what's expected here, no adapter needed."""
        async def _puller() -> None:
            while True:
                job = await asyncio.to_thread(next_job)
                if job is None:
                    return
                outcome = await self._run_one(
                    job, job_fn, on_event=on_event, force_wide=force_wide)
                await asyncio.to_thread(on_result, job, outcome)

        n = max(1, workers or self.wide_concurrency)
        await asyncio.gather(*(_puller() for _ in range(n)))
