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

def _age_h(ts) -> float | None:
    """Age in hours of an ISO string / datetime / epoch (kept for callers that parse provider timestamps)."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return (time.time() - ts) / 3600.0
    if isinstance(ts, str):
        try:
            ts = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return (dt.datetime.now(dt.timezone.utc) - ts).total_seconds() / 3600.0


async def sweep(max_age_hours: float = 6.0, providers=None, dry_run: bool = False) -> list[tuple[str, float]]:
    """One pass over every available provider (or `providers`): kill sandboxes of ours older than the limit. Provider specifics
    (what is "ours", how to list and kill) live in each provider's RawSandbox subclass; see fleet.py."""
    from . import fleet
    return await fleet.sweep(max_age_hours, providers, dry_run)


def load_provider_keys(env_file: str = "/ansh/euphoria/env/.env") -> None:
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


_load_keys = load_provider_keys      # old name


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--max-age-hours", type=float, default=float(os.environ.get("SANDBOX_JANITOR_MAX_AGE_H", 6)))
    ap.add_argument("--providers", default="", help="comma list; default: every provider whose key is set"); ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--watch", type=float, default=0, help="repeat every N seconds")
    a = ap.parse_args(argv)
    load_provider_keys()
    while True:
        found = asyncio.run(sweep(a.max_age_hours, [p for p in a.providers.split(",") if p] or None, a.dry_run))
        if not a.dry_run:
            from .fleet import reconcile
            reconcile()                                  # the sandboxes just deleted stop accruing in the cost ledger
        verb = "would delete" if a.dry_run else "deleted"
        print(f"{time.strftime('%FT%TZ', time.gmtime())} janitor: {verb} {len(found)} sandbox(es) older than {a.max_age_hours:g} h"
              + ("".join(f"\n  {n} ({age:.1f} h)" for n, age in found[:20])), flush=True)
        if not a.watch:
            return 0
        time.sleep(a.watch)


if __name__ == "__main__":
    sys.exit(main())
