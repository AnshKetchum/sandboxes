"""Generic fleet logic over the provider interface (raw.RawSandbox): no provider names, no provider SDK calls.

Everything here works for any provider that implements `list_live` / `kill` / `stored_artifacts` / `cost_model`, so a
new provider gets cost tracking, spend caps, ledger reconciliation and the janitor by subclassing and registering.

    live()               what exists now, per provider
    sweep()              kill sandboxes older than a limit (the janitor)
    reconcile()          close ledger entries whose sandbox is gone (stops phantom cost)
    accrue_storage()     put stored images/snapshots/volumes into the ledger as `storage` events
"""

from __future__ import annotations

import json
import time
from typing import Optional

from .registry import PROVIDERS, load_class, usable
from .models import SandboxProviderConfig


def providers_available() -> list[str]:
    """Providers with a raw backend whose API keys are in the environment."""
    return [n for n, info in PROVIDERS.items() if info.raw and usable(SandboxProviderConfig(name=n))[0]]


def raw_class(name: str):
    return load_class(PROVIDERS[name].raw)


async def live(providers=None) -> dict:
    """{provider: [LiveSandbox, ...]}, or None for a provider that could not be listed (callers must then leave its entries alone)."""
    out = {}
    for n in providers or providers_available():
        try:
            out[n] = await raw_class(n).list_live()
        except Exception:  # noqa: BLE001
            out[n] = None
    return out


async def sweep(max_age_hours: float = 6.0, providers=None, dry_run: bool = False) -> list[tuple[str, float]]:
    """Kill every sandbox of ours older than `max_age_hours`. Returns [("provider:id", age_hours)]."""
    found, now = [], time.time()
    for n, items in (await live(providers)).items():
        for sb in items or []:
            age = (now - sb.created_at) / 3600.0 if sb.created_at else None
            if age is not None and age > max_age_hours:
                found.append((f"{n}:{sb.id}", age))
                if not dry_run:
                    try:
                        await raw_class(n).kill(sb.id)
                    except Exception:  # noqa: BLE001 — "state change in progress": the next pass gets it
                        pass
    return found


def _reconcile_core(path, live_now: dict, now: Optional[float] = None) -> int:
    """Close ledger entries whose sandbox is not in `live_now` ({provider: set of ids, or None if that provider could not be listed})."""
    from .cost import get_ledger
    led = get_ledger(); path = path or led.path
    now = now or time.time()
    opens, closed = {}, set()
    try:
        for line in open(path):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("ev") == "open" and r.get("sid"):
                opens[r["id"]] = r
            elif r.get("ev") == "close":
                closed.add(r["id"])
    except OSError:
        return 0
    n = 0
    for h, o in opens.items():
        ids = live_now.get(o["provider"])
        if h in closed or ids is None or o["sid"] in ids:
            continue
        sec = max(0.0, now - o["t"])
        usd = raw_class(o["provider"]).cost_model().sandbox_usd(o["cpu"], o["ram_gib"], o["disk_gib"], sec) if o["provider"] in PROVIDERS else 0.0
        led._append({"t": now, "ev": "close", "id": h, "provider": o["provider"], "run": o.get("run", ""), "seconds": round(sec, 2),
                     "usd": round(usd, 6), "note": "reconciled: sandbox no longer exists"})
        n += 1
    return n


def close_dead_entries(max_age_s: float = 3 * 3600.0, path=None, now: Optional[float] = None) -> int:
    """Close ledger entries still open after `max_age_s`. reconcile() only closes entries that carry a sandbox id, so a killed process's
    entries (no id yet, or an old record) would bill up to the stale ceiling and count against the spend cap for hours. A sandbox never
    outlives its own lifetime (<= 3 h in every config here), so one open longer than that is dead; it is billed at `max_age_s`."""
    from .cost import CostLedger, get_ledger
    led = CostLedger(path) if path else get_ledger()
    path = path or led.path
    cut = (now or time.time()) - max_age_s
    opens, closed = {}, set()
    try:
        for line in open(path):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("ev") == "open":
                opens[r["id"]] = r
            elif r.get("ev") == "close":
                closed.add(r["id"])
    except OSError:
        return 0
    n = 0
    for h, o in opens.items():
        if h in closed or o["t"] > cut:
            continue
        usd = raw_class(o["provider"]).cost_model().sandbox_usd(o["cpu"], o["ram_gib"], o["disk_gib"], max_age_s) if o["provider"] in PROVIDERS else 0.0
        led._append({"t": o["t"] + max_age_s, "ev": "close", "id": h, "provider": o["provider"], "run": o.get("run", ""), "seconds": max_age_s,
                     "usd": round(usd, 6), "note": "closed: open past the lifetime ceiling (dead process)"})
        n += 1
    return n


async def areconcile(path=None, live_now: Optional[dict] = None, now: Optional[float] = None, gone=()) -> int:
    """Close ledger entries whose sandbox no longer exists on its provider (killed by a stop script, a janitor, the provider's own
    lifetime, a crashed process). Closed at `now`, so run it often. Entries without a sandbox id are left to the stale cutoff.
    Use this from async code; `reconcile()` is the same for plain scripts."""
    if live_now is None:
        live_now = {n: ({x.id for x in v} if v is not None else None) for n, v in (await live()).items()}
    if gone:                                   # ids just killed: providers take a few seconds to stop listing them
        live_now = {n: (ids - set(gone) if ids is not None else None) for n, ids in live_now.items()}
    return _reconcile_core(path, live_now, now)


def reconcile(path=None, live_now: Optional[dict] = None, now: Optional[float] = None) -> int:
    """Synchronous `areconcile` (it starts its own event loop: do not call it from async code)."""
    import asyncio
    if live_now is not None:
        return _reconcile_core(path, live_now, now)
    return asyncio.run(areconcile(path, None, now))


async def accrue_storage(seconds: float, providers=None) -> float:
    """Record `seconds` of storage cost for whatever each provider holds now (images, snapshots, volumes). Returns USD added.
    Call it on a fixed cadence (the guard does, every cycle) with the time since the previous call."""
    from .cost import get_ledger
    led, total = get_ledger(), 0.0
    for n in providers or providers_available():
        try:
            items = await raw_class(n).stored_artifacts()
        except Exception:  # noqa: BLE001
            continue
        model = raw_class(n).cost_model()
        for a in items:
            total += led.storage(n, a.kind, a.gib, seconds, label=a.id, model=model)
    return total


# ---------------------------------------------------------------------------------------------------------------------
# One command set for every project: ls / stop by owner, run and age. Nothing here names a provider, except the local docker
# daemon, which is not a RawSandbox and is listed through its own small adapter below.
# ---------------------------------------------------------------------------------------------------------------------
import asyncio as _asyncio
import json as _json
import shutil as _shutil
import subprocess as _subprocess

from .labels import LEGACY_DOCKER_PREFIXES, is_ours, owner_of, run_of
from .raw import LiveSandbox


def _docker_ok() -> bool:
    if not _shutil.which("docker"):
        return False
    try:
        return _subprocess.run(["docker", "version", "--format", "{{.Server.Version}}"], capture_output=True, timeout=8).returncode == 0
    except Exception:  # noqa: BLE001
        return False


def _docker_live_sync() -> list:
    r = _subprocess.run(["docker", "ps", "-a", "--no-trunc", "--format", "{{json .}}"], capture_output=True, text=True, timeout=60)
    out = []
    for line in r.stdout.splitlines():
        try:
            c = _json.loads(line)
        except ValueError:
            continue
        labels = dict(kv.split("=", 1) for kv in (c.get("Labels") or "").split(",") if "=" in kv)
        name, image = c.get("Names", ""), c.get("Image", "")
        if not is_ours(labels):                                   # containers older than the standard labels: known name/image conventions
            owner = next((o for pre, o in LEGACY_DOCKER_PREFIXES.items() if name.startswith(pre)), None) or ("greenhouse" if image.startswith("greenhouse-precheck:") else None)
            if not owner:
                continue
            labels = {**labels, "sandboxes.owner": owner}
        labels["name"] = name
        try:
            created = _dt_epoch(c.get("CreatedAt", ""))
        except ValueError:
            created = None
        out.append(LiveSandbox("docker", c["ID"], created, c.get("State", ""), labels))
    return out


def _dt_epoch(s: str):
    import datetime as dt
    return dt.datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc).timestamp() if s else None


async def ls(owner=None, run=None, older_than_hours=None, providers=None) -> list:
    """Sandboxes of ours alive now, optionally narrowed by owner, run and age. Includes local docker containers."""
    now, out = time.time(), []
    live_by = await live([p for p in (providers or providers_available()) if p != "docker"])
    if (providers is None or "docker" in providers) and _docker_ok():
        live_by["docker"] = await _asyncio.get_running_loop().run_in_executor(None, _docker_live_sync)
    for items in live_by.values():
        for sb in items or []:
            if owner and owner_of(sb.labels) != owner:
                continue
            if run and not (run_of(sb.labels) == run or run in sb.labels.get("name", "")):
                continue
            if older_than_hours is not None and not (sb.created_at and (now - sb.created_at) / 3600.0 > older_than_hours):
                continue
            out.append(sb)
    return out


async def stop(owner=None, run=None, older_than_hours=None, providers=None, all_ours: bool = False, dry_run: bool = False,
               batch: int = 0, pause_s: float = 0.0, on_result=None) -> list:
    """End sandboxes of ours: by owner (a project), by run, by age, or `all_ours`. A scope is required so nothing is stopped by
    accident. Kills through each provider's own `kill`, then closes the cost-ledger entries of the ones that were deleted.
    Returns the sandboxes that were deleted (with `dry_run`: the ones that would be). A kill that fails is left alone (the next stop
    gets it), is not returned and is not closed in the ledger. `on_result(sandbox, error_or_None)` is called as each is handled."""
    if not (owner or run or older_than_hours is not None or all_ours):
        raise ValueError("stop needs a scope: owner=, run=, older_than_hours= or all_ours=True")
    found = await ls(owner, run, older_than_hours, providers)
    if dry_run:
        for sb in found:
            if on_result:
                on_result(sb, None)
        return found
    deleted = []
    for i, sb in enumerate(found):
        if batch and i and i % batch == 0 and pause_s:
            await _asyncio.sleep(pause_s)                  # staggered deletes: a provider with a deep destroy backlog stays healthy
        err = None
        try:
            if sb.provider == "docker":
                _subprocess.run(["docker", "rm", "-f", sb.id], capture_output=True, timeout=60)
            else:
                await raw_class(sb.provider).kill(sb.id)
        except Exception as e:  # noqa: BLE001 — "state change in progress": the next stop gets it
            err = e
        if err is None:
            deleted.append(sb)
        if on_result:
            on_result(sb, err)
    await areconcile(gone={sb.id for sb in deleted})
    return deleted


def classify_state(state: str) -> str:
    """Normalize a provider's sandbox state string to error | creating | stopped | running (unknown states are running)."""
    t = (state or "").lower()
    if "error" in t or "failed" in t:
        return "error"
    if any(k in t for k in ("creat", "build", "pending", "deploy", "schedul", "pull")):
        return "creating"
    if any(k in t for k in ("stop", "archiv", "destroy", "terminat", "exit")):
        return "stopped"
    return "running"


async def reap(owner=None, run=None, creating_older_than_min: float = 8.0, providers=None, dry_run: bool = False) -> list:
    """Delete our sandboxes that are broken: in an error state, or still `creating` after `creating_older_than_min` (the ghosts of
    timed-out create calls hold quota). Returns [(LiveSandbox, why)]."""
    now, out = time.time(), []
    for sb in await ls(owner, run, None, providers):
        st = classify_state(sb.state)
        age_min = (now - sb.created_at) / 60.0 if sb.created_at else 0.0
        why = "error state" if st == "error" else (f"creating for {age_min:.0f} min" if st == "creating" and age_min > creating_older_than_min else None)
        if why:
            out.append((sb, why))
    if not dry_run:
        for sb, _ in out:
            try:
                if sb.provider == "docker":
                    _subprocess.run(["docker", "rm", "-f", sb.id], capture_output=True, timeout=60)
                else:
                    await raw_class(sb.provider).kill(sb.id)
            except Exception:  # noqa: BLE001
                pass
        if out:
            await areconcile(gone={sb.id for sb, _ in out})
    return out
