"""Harbor environments behind the sandboxes interface, for projects that run harbor tasks (euphoria, petunia, greenhouse).

A harbor environment is a task's own Dockerfile built into a sandbox, so it cannot go through RawSandbox, but a project should
still not know which provider it got. Everything provider-specific about harbor lives here: which environment class and which
keyword arguments each provider needs, the standard owner/run labels, finite lifetimes, the admission check against the spend caps,
cost-ledger recording, which start errors are worth retrying, and shutting down harbor's provider clients.

    env = harbor_adapter.make_env("e2b", task=task, trial_paths=paths, session_id="t__1", size=SIZE, owner="euphoria")
    await harbor_adapter.start_env(env)            # retries what is retryable; counts the sandbox in the cost ledger
    ...
    await env.stop(delete=True)                    # closes its ledger entry and releases it
    harbor_adapter.stop_live()                     # at shutdown: stop the sandboxes this process still holds
"""

from __future__ import annotations

import asyncio
import importlib
from typing import Callable, Optional

from .labels import standard_labels
from .lifetime import DEFAULT_IDLE_STOP_MIN, clamp_lifetime_min

HARBOR_ENVS = {
    "daytona": ("harbor.environments.daytona.environment", "DaytonaEnvironment"),
    "e2b": ("harbor.environments.e2b", "E2BEnvironment"),
    "blaxel": ("harbor.environments.blaxel", "BlaxelEnvironment"),
}
_live: dict = {}


def harbor_providers() -> tuple:
    """Providers a harbor environment exists for."""
    return tuple(HARBOR_ENVS)


def _sandbox_id(env, provider: str) -> str:
    sb = getattr(env, "_sandbox", None)
    for attr in ("id", "sandbox_id"):
        if sb is not None and getattr(sb, attr, None):
            return str(getattr(sb, attr))
    return str(getattr(env, "_sandbox_name", "") or "")


def make_env(provider: str, *, task, trial_paths, session_id: str, size: dict, network_mode=None, auto_snapshot: bool = False,
             owner: Optional[str] = None, run: Optional[str] = None, on_release: Optional[Callable[[], None]] = None):
    """A harbor environment on `provider`, labelled, lifetime-limited, cap-checked and tracked in the cost ledger.
    `size`: {cpu, memory_mb, disk_gb, auto_stop_minutes, auto_delete_minutes, pricing}."""
    from .guard import check_admission
    check_admission(provider)
    mod, cls = HARBOR_ENVS[provider]
    kw = dict(environment_dir=task.paths.environment_dir, environment_name=task.short_name, session_id=session_id, trial_paths=trial_paths,
              task_env_config=task.config.environment, override_storage_mb=int((size.get("disk_gb") or 8) * 1024))
    if network_mode is not None:
        from harbor.models.task.config import NetworkMode, NetworkPolicy
        kw["network_policy"] = NetworkPolicy(network_mode=NetworkMode(network_mode))
    life = clamp_lifetime_min(size.get("auto_delete_minutes"))
    if provider == "daytona":
        kw.update(auto_snapshot=auto_snapshot and not bool(task.config.environment.docker_image), labels=standard_labels(owner, run),
                  auto_stop_interval_mins=int(size.get("auto_stop_minutes") or DEFAULT_IDLE_STOP_MIN), auto_delete_interval_mins=life)
    elif provider == "blaxel":
        kw["ttl"] = f"{life}m"                                   # harbor's default is 24 h
    if size.get("cpu"):
        kw["override_cpus"] = int(size["cpu"])
    if size.get("memory_mb"):
        kw["override_memory_mb"] = int(size["memory_mb"])
    env = getattr(importlib.import_module(mod), cls)(**kw)
    _track(env, provider, size, standard_labels(owner, run), on_release)
    return env


def _track(env, provider: str, size: dict, labels: dict, on_release) -> None:
    from .cost import get_ledger
    from .pricing import cost_model_for
    state = {"released": False, "handle": None}
    orig_start, orig_stop = env.start, env.stop

    def release():
        if not state["released"]:
            state["released"] = True
            _live.pop(id(env), None)
            if on_release:
                on_release()

    async def start(*a, **k):
        try:
            r = await orig_start(*a, **k)
        except BaseException:
            release()
            raise
        state["handle"] = get_ledger().open(provider, cpu=float(size.get("cpu") or 1), ram_gib=(size.get("memory_mb") or 2048) / 1024.0,
                                            disk_gib=float(size.get("disk_gb") or 8), labels=labels, sandbox_id=_sandbox_id(env, provider))
        return r

    async def stop(*a, **k):
        try:
            return await orig_stop(*a, **k)
        finally:
            if state["handle"]:
                get_ledger().close(state["handle"], cost_model_for(provider, size.get("pricing")))
                state["handle"] = None
            release()

    env.start, env.stop = start, stop
    env.sandbox_provider = provider
    _live[id(env)] = env


def is_retryable_start_error(e: Exception) -> bool:
    """Start failures worth another attempt on a fresh sandbox (a build or provider-side flake, not a bad task)."""
    try:
        from harbor.environments.base import SandboxBuildFailedError
        if isinstance(e, SandboxBuildFailedError):
            return True
    except ImportError:
        pass
    m = str(e).lower()
    return type(e).__name__ == "DaytonaError" and ("entered error state" in m or "sandboxstate.error" in m or "build_failed" in m)


async def start_env(env, attempts: int = 3, label: str = "env") -> None:
    for attempt in range(1, attempts + 1):
        try:
            await env.start(force_build=False)
            return
        except Exception as e:  # noqa: BLE001
            if not is_retryable_start_error(e) or attempt == attempts:
                raise
            wait = min(60, 15 * attempt)
            print(f"{label}: sandbox start failed (attempt {attempt}/{attempts}), retrying in {wait}s: {e}")
            try:
                await env.stop(delete=True)
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(wait)


async def stop_live() -> int:
    """Stop every harbor sandbox this process still holds (after an abort), whichever provider it is on."""
    n = 0
    for env in list(_live.values()):
        try:
            await env.stop(delete=True)
            n += 1
        except Exception as e:  # noqa: BLE001
            print(f"stop {getattr(env, 'session_id', '?')} failed: {e!r}")
    return n


async def close_clients() -> None:
    """Shut down the provider clients harbor keeps open (best effort)."""
    try:
        from harbor.environments.daytona.environment import DaytonaClientManager
        await (await DaytonaClientManager.get_instance())._cleanup()
    except Exception:  # noqa: BLE001
        pass
