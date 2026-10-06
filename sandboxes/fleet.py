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


def reconcile(path=None, live_now: Optional[dict] = None, now: Optional[float] = None) -> int:
    """Close ledger entries whose sandbox no longer exists on its provider (killed by a stop script, a janitor, the provider's own
    lifetime, a crashed process). Closed at `now`, so run it often. Entries without a sandbox id are left to the stale cutoff."""
    import asyncio
    from .cost import get_ledger
    led = get_ledger(); path = path or led.path
    if live_now is None:
        live_now = {n: ({x.id for x in v} if v is not None else None) for n, v in asyncio.run(live()).items()}
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
