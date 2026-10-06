"""`sandboxes`: the one command set for every project (euphoria, petunia, greenhouse, pr_task_synthesis, ...).

    sandboxes ls    [--owner X] [--run R] [--older-than H] [--providers a,b]    what is alive now
    sandboxes stop  --owner X | --run R | --older-than H | --all [--dry-run]    end sandboxes (any provider, local docker too)
    sandboxes cost  [--run R] [--watch N] [--cap USD]                            spend from the uniform cost ledger
    sandboxes guard [--watch N] [--kill]                                         caps, burn rate, kill when a cap trips
    sandboxes janitor [--max-age-hours H] [--dry-run]                            delete anything past a maximum age
    sandboxes reap  [--owner X] [--run R] [--creating-older-than MIN] [--dry-run] delete broken sandboxes (error state, stuck creating)
    sandboxes providers                                                          what is usable and what a 2 vCPU/4 GiB node costs
    sandboxes audit PATH...                                                      find provider-specific code in a project

A project's own stop/clean command is a thin wrapper over `sandboxes stop --owner <project>` (see sandboxes.cli.stop_for).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time


def _fmt_age(sb) -> str:
    return "-" if not sb.created_at else f"{(time.time() - sb.created_at) / 3600:.1f}h"


def cmd_ls(a) -> int:
    from . import fleet
    found = asyncio.run(fleet.ls(a.owner, a.run, a.older_than, a.providers.split(",") if a.providers else None))
    from .labels import owner_of, run_of
    for sb in found:
        print(f"{sb.provider:<8} {sb.id[:24]:<26} {_fmt_age(sb):>7}  owner={owner_of(sb.labels) or '-':<18} run={run_of(sb.labels) or '-':<24} {sb.state}")
    print(f"{len(found)} sandbox(es)")
    return 0


def stop_for(owner: str, run: str | None = None, dry_run: bool = False, quiet: bool = False) -> int:
    """What a project's own stop/clean command calls: stop everything `owner` started (optionally one run). Returns the count."""
    from . import fleet
    found = asyncio.run(fleet.stop(owner=owner, run=run, dry_run=dry_run))
    if not quiet:
        print(f"{'would stop' if dry_run else 'stopped'} {len(found)} {owner} sandbox(es)" + (f" of run {run}" if run else ""))
    return len(found)


def cmd_stop(a) -> int:
    from . import fleet
    if not (a.owner or a.run or a.older_than is not None or a.all):
        print("stop needs a scope: --owner X, --run R, --older-than H or --all", file=sys.stderr)
        return 2
    found = asyncio.run(fleet.stop(a.owner, a.run, a.older_than, a.providers.split(",") if a.providers else None, a.all, a.dry_run, a.batch, a.pause))
    from collections import Counter
    c = Counter(sb.provider for sb in found)
    print(f"{'would stop' if a.dry_run else 'stopped'} {len(found)} sandbox(es): {dict(c)}")
    return 0


def cmd_reap(a) -> int:
    from . import fleet
    out = asyncio.run(fleet.reap(a.owner, a.run, a.creating_older_than, a.providers.split(",") if a.providers else None, a.dry_run))
    for sb, why in out:
        print(f"{sb.provider} {sb.id[:24]} {why}")
    print(f"{'would reap' if a.dry_run else 'reaped'} {len(out)} sandbox(es)")
    return 0


def cmd_providers(a) -> int:
    from . import fleet
    from .models import SandboxProviderConfig
    from .registry import PROVIDERS, usable
    print(f"{'provider':<9}{'usable':<8}{'hard lifetime':<15}{'docker-in-docker':<18}{'node $/h':>9}  note")
    for n, info in PROVIDERS.items():
        ok, why = usable(SandboxProviderConfig(name=n))
        raw = fleet.raw_class(n) if info.raw else None
        usd = f"{raw.cost_model().hourly_usd(2, 4, 10):.3f}" if raw else "free"
        print(f"{n:<9}{'yes' if ok else 'no':<8}{str(raw.hard_lifetime) if raw else '-':<15}{str(info.capabilities.get('dind', False)):<18}{usd:>9}  {'' if ok else why}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="sandboxes", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("ls", "stop"):
        p = sub.add_parser(name)
        p.add_argument("--owner"); p.add_argument("--run"); p.add_argument("--older-than", type=float, help="hours")
        p.add_argument("--providers", default="", help="comma list (default: all available + local docker)")
        if name == "stop":
            p.add_argument("--all", action="store_true", help="every sandbox of ours, any owner"); p.add_argument("--dry-run", action="store_true")
            p.add_argument("--batch", type=int, default=0, help="delete N at a time ..."); p.add_argument("--pause", type=float, default=0, help="... then wait this many seconds")
    r = sub.add_parser("reap")
    r.add_argument("--owner"); r.add_argument("--run"); r.add_argument("--creating-older-than", type=float, default=8.0, help="minutes")
    r.add_argument("--providers", default=""); r.add_argument("--dry-run", action="store_true")
    sub.add_parser("providers")
    for name in ("cost", "guard", "janitor", "audit"):
        sub.add_parser(name, add_help=False)
    a, rest = ap.parse_known_args(argv)
    from .janitor import _load_keys
    _load_keys()                                    # keys from the shared env file when the shell did not export them
    if a.cmd == "ls":
        return cmd_ls(a)
    if a.cmd == "stop":
        return cmd_stop(a)
    if a.cmd == "reap":
        return cmd_reap(a)
    if a.cmd == "providers":
        return cmd_providers(a)
    if a.cmd == "cost":
        from .cost import main as m
        return m(rest)
    if a.cmd == "guard":
        from .guard import main as m
        return m(rest)
    if a.cmd == "janitor":
        from .janitor import main as m
        return m(rest)
    from .audit import main as m
    return m(rest)


if __name__ == "__main__":
    sys.exit(main())
