#!/usr/bin/env python3
"""Concurrency and cost benchmark for sandbox providers, through the library's own provider interface.

For each provider and each concurrency level N it starts N sandboxes AT ONCE, checks they all run a command, burns CPU on all of
them together, then deletes them, and records: how many started and why the rest failed, start latency (p50/p90/max), CPU
throughput, and what it cost. Every level also tells you the provider's ceiling when starts begin to fail (the number that did
start before the first quota error). The $/hour of one node of the requested size comes from the provider's own cost model.

    python scripts/benchmark_concurrency.py --provider modal --concurrencies 1 10 100
    python scripts/benchmark_concurrency.py --provider e2b blaxel modal --concurrencies 1 10 100 --hold 30
    python scripts/benchmark_concurrency.py --provider modal --extra runtime=gvisor --cpu 2 --mem-gb 4 --disk-gb 10

A level is FEASIBLE when at least --threshold (default 95%) of its sandboxes start and run. Escalation stops at the first level
that is not feasible (use --no-stop-on-fail to keep going). Safety: every sandbox gets a short lifetime (hold + 10 min) so a
crashed run cannot leave anything billing for long, the library's spend caps apply, and --budget-usd (default 5) refuses a level
whose estimated cost would push the run past it.

Results: a table on stdout, JSON in scripts/benchmark_results/<run>.json, and a one-line-per-level summary appended to
scripts/benchmark_results/summary.md.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sandboxes import SpendCapExceeded, fleet                 # noqa: E402
from sandboxes.errors import CapacityError                    # noqa: E402
from sandboxes.models import SandboxProviderConfig            # noqa: E402
from sandboxes.raw import SandboxSpec                         # noqa: E402
from sandboxes.registry import usable                         # noqa: E402

RESULTS = Path(__file__).resolve().parent / "benchmark_results"
BURN = ("python3 - <<'EOF'\nimport time, multiprocessing as m\n"
        "def f(_):\n    e = time.time() + {hold}; n = 0\n    while time.time() < e: n += 1\n    return n\n"
        "print(sum(m.Pool({cpu}).map(f, range({cpu}))))\nEOF")


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p))] if v else None


def classify(e: BaseException) -> str:
    if isinstance(e, CapacityError):
        return "capacity/quota: " + str(e)[:90]
    if isinstance(e, SpendCapExceeded):
        return "spend cap"
    name = type(e).__name__
    return f"{name}: {str(e)[:90]}"


async def one_level(provider: str, n: int, args, extra: dict) -> dict:
    cls = fleet.raw_class(provider)
    model = cls.cost_model()
    spec = lambda: SandboxSpec(image=args.image, cpu=args.cpu, mem_gb=args.mem_gb, disk_gb=args.disk_gb,
                               lifetime_s=int(args.hold + 600), create_timeout=args.create_timeout, create_tries=1,
                               extra=dict(extra), labels={"bench": os.environ.get("SANDBOX_RUN_ID", "")})
    boxes, fails, create_s, ready_s, ttr_s, burn_iters, cost = [], collections.Counter(), [], [], [], [], 0.0
    t_level = time.time()

    async def start(i):
        sbx = cls(spec(), model)
        t = time.time()
        try:
            await sbx.start()
        except BaseException as e:  # noqa: BLE001
            fails[classify(e)] += 1
            return
        sbx._create_s = time.time() - t; create_s.append(sbx._create_s); boxes.append(sbx)

    await asyncio.gather(*(start(i) for i in range(n)))
    started = len(boxes)

    async def run(sbx):
        try:
            t = time.time()
            r = await sbx.exec("echo ok", timeout=60)
            if r.rc != 0 or "ok" not in r.stdout:
                fails["exec: " + (r.stdout or "")[:60]] += 1
                return
            ready_s.append(time.time() - t); ttr_s.append(sbx._create_s + ready_s[-1])
            if args.hold > 0:
                b = await sbx.exec(BURN.format(hold=args.hold, cpu=args.cpu), timeout=args.hold + 90)
                if b.rc == 0 and b.stdout.strip().splitlines():
                    burn_iters.append(int(b.stdout.strip().splitlines()[-1]))
                else:
                    fails["burn: " + (b.stdout or "")[:60]] += 1
        except BaseException as e:  # noqa: BLE001
            fails["run: " + classify(e)] += 1

    await asyncio.gather(*(run(b) for b in boxes))
    peak_live = len(boxes)
    t_del = time.time()

    async def stop(sbx):
        try:
            return await sbx.stop()
        except BaseException:  # noqa: BLE001
            return 0.0

    cost = sum(await asyncio.gather(*(stop(b) for b in boxes)))      # summed after the gather: `cost += await` loses updates under concurrency
    ok = len(burn_iters) if args.hold > 0 else len(ready_s)
    return {"provider": provider, "n": n, "started": started, "ok": ok, "failed": n - ok, "success_rate": ok / n,
            "failures": dict(fails), "create_p50": pct(create_s, .5), "create_p90": pct(create_s, .9), "create_max": max(create_s, default=None),
            "ready_p50": pct(ready_s, .5), "ttr_p50": pct(ttr_s, .5), "ttr_p90": pct(ttr_s, .9), "ttr_max": max(ttr_s, default=None), "burn_p50": pct(burn_iters, .5), "burn_min": min(burn_iters, default=None),
            "burn_max": max(burn_iters, default=None), "delete_s": time.time() - t_del, "peak_live": peak_live,
            "level_seconds": time.time() - t_level, "test_cost_usd": cost}


async def warmup(provider: str, args, extra: dict) -> float:
    """One sandbox first, so one-time image/template builds do not count against the measured levels."""
    cls = fleet.raw_class(provider); t = time.time()
    sbx = cls(SandboxSpec(image=args.image, cpu=args.cpu, mem_gb=args.mem_gb, disk_gb=args.disk_gb, lifetime_s=900,
                          create_timeout=900, create_tries=2, extra=dict(extra)), cls.cost_model())
    try:
        await sbx.start(); await sbx.exec("true", timeout=60)
    finally:
        await sbx.stop()
    return time.time() - t


def fmt(x, f="{:.1f}"):
    return "-" if x is None else f.format(x)


def table(rows):
    h = f"{'provider':<8}{'N':>6}{'started':>8}{'ok':>6}{'rate':>6}{'create p50/p90/max (s)':>26}{'first-cmd p50/p90/max (s)':>28}{'burn p50 (M)':>13}{'burn min..max (M)':>20}{'test $':>9}"
    out = [h, "-" * len(h)]
    for r in rows:
        out.append(f"{r['provider']:<8}{r['n']:>6}{r['started']:>8}{r['ok']:>6}{100 * r['success_rate']:>5.0f}%"
                   f"{fmt(r['create_p50']) + '/' + fmt(r['create_p90']) + '/' + fmt(r['create_max']):>26}{fmt(r['ttr_p50']) + '/' + fmt(r['ttr_p90']) + '/' + fmt(r['ttr_max']):>28}"
                   f"{fmt(r['burn_p50'] and r['burn_p50'] / 1e6):>13}{(fmt(r['burn_min'] and r['burn_min'] / 1e6) + '..' + fmt(r['burn_max'] and r['burn_max'] / 1e6)):>20}{r['test_cost_usd']:>9.3f}")
        for k, v in r["failures"].items():
            out.append(f"{'':<8}   {v}x {k}")
    return "\n".join(out)


async def main_async(args):
    from sandboxes.janitor import load_provider_keys
    load_provider_keys()                      # provider keys from the shared env file when the shell did not export them
    extra = dict(kv.split("=", 1) for kv in args.extra)
    os.environ.setdefault("SANDBOX_RUN_ID", "bench-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S"))
    run_id = os.environ["SANDBOX_RUN_ID"]
    from sandboxes import set_context
    set_context(owner="benchmark", run=run_id)          # `sandboxes stop --owner benchmark` ends anything a crashed run left
    rows, summary, spent_est = [], {}, 0.0
    for prov in args.provider:
        ok, why = usable(SandboxProviderConfig(name=prov))
        if not ok:
            print(f"[{prov}] skipped: {why}"); continue
        cls = fleet.raw_class(prov); model = cls.cost_model()
        node_h = model.hourly_usd(args.cpu, args.mem_gb, args.disk_gb)
        print(f"\n== {prov}: node {args.cpu} vCPU / {args.mem_gb} GiB / {args.disk_gb} GB = ${node_h:.3f}/h  (hard lifetime: {cls.hard_lifetime})", flush=True)
        try:
            print(f"   warm-up (image/template build, not measured): {await warmup(prov, args, extra):.0f}s", flush=True)
        except BaseException as e:  # noqa: BLE001
            print(f"   warm-up failed: {classify(e)}"); summary[prov] = {"node_usd_per_hour": node_h, "max_feasible": 0, "error": classify(e)}; continue
        best, ceiling = 0, None
        for n in args.concurrencies:
            est = n * (args.hold + 90) / 3600 * node_h
            if spent_est + est > args.budget_usd:
                print(f"   N={n}: skipped, estimated ${est:.2f} would exceed the ${args.budget_usd:.2f} budget (spent ~${spent_est:.2f})"); break
            r = await one_level(prov, n, args, extra); rows.append(r); spent_est += max(est, r["test_cost_usd"])
            print(f"   N={n:<5} started {r['started']:>4}/{n}  ok {r['ok']:>4}  first-cmd p50 {fmt(r['ttr_p50'])}s  max {fmt(r['ttr_max'])}s  cost ${r['test_cost_usd']:.3f}"
                  + (f"  FAILURES {r['failures']}" if r["failures"] else ""), flush=True)
            if r["success_rate"] >= args.threshold:
                best = n
            else:
                ceiling = r["started"]
                if args.stop_on_fail:
                    break
        summary[prov] = {"node_usd_per_hour": node_h, "max_feasible": best, "ceiling_seen": ceiling, "usd_per_hour_at_max_feasible": best * node_h,
                         "plan_limit_in_cost_model": model.max_concurrent}
    print("\n" + table(rows)); print()
    for p, s in summary.items():
        print(f"{p:<8} node ${s['node_usd_per_hour']:.3f}/h | max feasible tested: {s['max_feasible']}"
              + (f" | ceiling hit at ~{s['ceiling_seen']} sandboxes" if s.get("ceiling_seen") is not None else "")
              + f" | fleet of {s['max_feasible']}: ${s.get('usd_per_hour_at_max_feasible', 0):.2f}/h" + (f" | {s['error']}" if s.get("error") else ""))
    RESULTS.mkdir(exist_ok=True)
    out = RESULTS / f"{run_id}.json"
    out.write_text(json.dumps({"run": run_id, "args": vars(args), "levels": rows, "summary": summary}, indent=1))
    with open(RESULTS / "summary.md", "a") as f:
        for r in rows:
            f.write(f"- {run_id} {r['provider']} N={r['n']}: {r['ok']}/{r['n']} ok, create p50 {fmt(r['create_p50'])}s, first command p50 {fmt(r['ttr_p50'])}s max {fmt(r['ttr_max'])}s, ${r['test_cost_usd']:.3f}"
                    f" ({args.cpu} vCPU/{args.mem_gb} GiB/{args.disk_gb} GB, image {args.image})\n")
    print(f"\nwrote {out}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--provider", nargs="+", required=True, help="daytona e2b blaxel modal ...")
    ap.add_argument("--concurrencies", type=int, nargs="+", default=[1, 10, 100])
    ap.add_argument("--image", default="python:3.12-slim"); ap.add_argument("--cpu", type=int, default=2)
    ap.add_argument("--mem-gb", type=int, default=4); ap.add_argument("--disk-gb", type=int, default=10)
    ap.add_argument("--hold", type=float, default=30, help="seconds of CPU burn per level while all sandboxes are up (0 = skip)")
    ap.add_argument("--threshold", type=float, default=0.95); ap.add_argument("--create-timeout", type=int, default=300)
    ap.add_argument("--budget-usd", type=float, default=5.0)
    ap.add_argument("--extra", nargs="*", default=[], help="provider options key=value (e.g. runtime=vm for modal)")
    ap.add_argument("--no-stop-on-fail", dest="stop_on_fail", action="store_false")
    asyncio.run(main_async(ap.parse_args()))


if __name__ == "__main__":
    main()
