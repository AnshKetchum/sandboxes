"""Delete sandboxes older than a maximum age, on every provider whose API key is set.

The backstop under the per-sandbox lifetime (see lifetime.py): Daytona's auto-stop is inactivity based, harbor's E2B
environment lives 24 h, and a killed process leaves its sandboxes behind. Run it by hand or let a watchdog call it.

    python -m sandboxes.janitor --dry-run                      # list what is past 6 h
    python -m sandboxes.janitor --max-age-hours 6 --watch 300  # delete, every 5 minutes
    python -m sandboxes.janitor --providers e2b,blaxel

Scope: Daytona only sandboxes carrying one of our labels (agentdist, sandboxes.managed, euphoria.managed) because that
org is shared with other projects; E2B and Blaxel sandboxes of the account (it is ours alone).
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import os
import sys
import time

DAYTONA_LABELS = ("agentdist", "sandboxes.managed", "euphoria.managed")      # not harbor.* : other projects in the org use those


def _age_h(ts) -> float | None:
    if ts is None:
        return None
    if isinstance(ts, str):
        try:
            ts = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return (dt.datetime.now(dt.timezone.utc) - ts).total_seconds() / 3600.0


async def _daytona(max_h: float, dry: bool) -> list[tuple[str, float]]:
    if not os.environ.get("DAYTONA_API_KEY"):
        return []
    from daytona import AsyncDaytona
    c = AsyncDaytona()
    out = []
    try:
        items = []
        res = c.list()
        if hasattr(res, "__aiter__"):                        # the harbor-pinned sdk yields sandboxes
            async for sb in res:
                items.append(sb)
        else:                                                # newer sdks return pages
            r = await res
            items = list(r.items)
        for sb in items:
            labels = getattr(sb, "labels", None) or {}
            if not any(k in labels for k in DAYTONA_LABELS):
                continue
            age = _age_h(getattr(sb, "created_at", None))
            if age is not None and age > max_h:
                out.append((f"daytona:{sb.id}", age))
                if not dry:
                    try:
                        await c.delete(sb)
                    except Exception:  # noqa: BLE001 — "state change in progress": next pass gets it
                        pass
    finally:
        await c.close()
    return out


async def _e2b(max_h: float, dry: bool) -> list[tuple[str, float]]:
    if not os.environ.get("E2B_API_KEY"):
        return []
    from e2b import AsyncSandbox
    pag, items, out = AsyncSandbox.list(), [], []
    while True:
        items += await pag.next_items()
        if not pag.has_next:
            break
    for s in items:
        age = _age_h(getattr(s, "started_at", None))
        if age is not None and age > max_h:
            out.append((f"e2b:{s.sandbox_id}", age))
            if not dry:
                try:
                    await AsyncSandbox.kill(s.sandbox_id)
                except Exception:  # noqa: BLE001
                    pass
    return out


async def _blaxel(max_h: float, dry: bool) -> list[tuple[str, float]]:
    if not os.environ.get("BL_API_KEY"):
        return []
    from .compat import ensure_blaxel_importable
    ensure_blaxel_importable()
    from blaxel.core import SandboxInstance
    out, items = [], []
    page = await SandboxInstance.list(limit=100)            # one page is at most `limit`: walk them all
    async for s in page.auto_paging_iter():
        items.append(s)
    for s in items:
        if str(s.status).split(".")[-1] in ("TERMINATED", "DELETING"):
            continue
        age = _age_h(getattr(s.metadata, "created_at", None))
        if age is not None and age > max_h:
            out.append((f"blaxel:{s.metadata.name}", age))
            if not dry:
                try:
                    await SandboxInstance.delete(s.metadata.name)
                except Exception:  # noqa: BLE001
                    pass
    return out


async def sweep(max_age_hours: float = 6.0, providers=("daytona", "e2b", "blaxel"), dry_run: bool = False) -> list[tuple[str, float]]:
    """One pass. Returns [(provider:id, age_hours)] of the sandboxes that were past the limit."""
    fns = {"daytona": _daytona, "e2b": _e2b, "blaxel": _blaxel}
    found = []
    for p in providers:
        try:
            found += await fns[p](max_age_hours, dry_run)
        except Exception as e:  # noqa: BLE001 — one provider's outage must not stop the others
            print(f"janitor: {p} failed: {type(e).__name__}: {str(e)[:120]}", file=sys.stderr, flush=True)
    return found


def _load_keys(env_file: str = "/ansh/euphoria/env/.env") -> None:
    """Provider keys from the shared env file when the calling process (a watchdog, cron) did not export them."""
    from .registry import PROVIDERS
    wanted = {k for info in PROVIDERS.values() for k in info.env_keys}
    try:
        for line in open(os.environ.get("SANDBOX_ENV_FILE", env_file)):
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                if k in wanted:
                    os.environ.setdefault(k, v.strip().strip('"').strip("'"))
    except OSError:
        pass


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--max-age-hours", type=float, default=float(os.environ.get("SANDBOX_JANITOR_MAX_AGE_H", 6)))
    ap.add_argument("--providers", default="daytona,e2b,blaxel"); ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--watch", type=float, default=0, help="repeat every N seconds")
    a = ap.parse_args(argv)
    _load_keys()
    while True:
        found = asyncio.run(sweep(a.max_age_hours, [p for p in a.providers.split(",") if p], a.dry_run))
        if not a.dry_run:
            from .guard import reconcile
            reconcile()                                  # the sandboxes just deleted stop accruing in the cost ledger
        verb = "would delete" if a.dry_run else "deleted"
        print(f"{time.strftime('%FT%TZ', time.gmtime())} janitor: {verb} {len(found)} sandbox(es) older than {a.max_age_hours:g} h"
              + ("".join(f"\n  {n} ({age:.1f} h)" for n, age in found[:20])), flush=True)
        if not a.watch:
            return 0
        time.sleep(a.watch)


if __name__ == "__main__":
    sys.exit(main())
