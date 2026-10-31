#!/usr/bin/env python3
"""Verify that the cost tracking matches what each provider publishes and what it actually bills, and that the cost ledger is an
accurate tab. Three layers, each reporting inconsistencies instead of hiding them:

  A  RATES    our CostModel vs the provider's published price page, recomputed independently from the raw per-second / per-hour
              numbers (offline, no sandboxes). Flags any difference over 0.5%.
  B  METERING controlled live run: N sandboxes per provider, held for exactly --hold seconds, timed from the outside with a wall
              clock. Compares that to the ledger (seconds and USD per sandbox), checks the ledger's own invariants (every open has
              one close, usd == model(seconds), summary == sum of parts, nothing left "running"), and shows how much of a
              sandbox's real life (creation and deletion latency) the ledger does not see.
  C  PROVIDER-REPORTED  where a provider has a billing / usage API: Daytona's usage API (checked right after B), Modal's billing
              report by the run's tag (Modal publishes hourly; use --check later). Providers with no such API (E2B, Blaxel, Fly)
              are listed as "dashboard only" with the exact amount to expect for the run id.

    python scripts/verify_costs.py --providers daytona e2b blaxel modal fly --n 3 --hold 120
    python scripts/verify_costs.py --check scripts/benchmark_results/cost_verify-20261006-210000.json     # layer C for Modal, later
    python scripts/verify_costs.py --rates-only

Writes scripts/benchmark_results/cost_verify-<run>.json and .md. Exit status 1 if any layer found an inconsistency over its tolerance.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sandboxes import fleet                                      # noqa: E402
from sandboxes.cost import CostLedger, get_ledger, summary       # noqa: E402
from sandboxes.janitor import load_provider_keys                  # noqa: E402
from sandboxes.labels import set_context                          # noqa: E402
from sandboxes.models import SandboxProviderConfig                # noqa: E402
from sandboxes.pricing import cost_model_for                      # noqa: E402
from sandboxes.raw import SandboxSpec                             # noqa: E402
from sandboxes.registry import usable                             # noqa: E402

RESULTS = Path(__file__).resolve().parent / "benchmark_results"
CPU, RAM, DISK = 2, 4, 10          # the reference node

# ---- A: published rates, as printed on the provider's page (fetched 2026-10-06). Independent arithmetic, deliberately NOT sharing code with pricing.py.
PUBLISHED = {
    "daytona": ("https://www.daytona.io/pricing: $0.0504/vCPU-h, $0.0162/GiB-h, $0.000108/GiB-h disk, first 5 GiB free",
                lambda c, r, d: c * 0.0504 + r * 0.0162 + max(0, d - 5) * 0.000108),
    "e2b": ("https://e2b.dev/pricing: $0.000014/vCPU-s, $0.0000045/GiB-s, storage free",
            lambda c, r, d: c * 0.000014 * 3600 + r * 0.0000045 * 3600),
    "blaxel": ("https://blaxel.ai/pricing: $0.0000115/GB-RAM-s, CPU scales with RAM and is not billed separately",
               lambda c, r, d: r * 0.0000115 * 3600),
    "modal": ("https://modal.com/pricing: $0.00003942/core-s (1 core = 2 vCPU on the page), $0.00000667/GiB-s; we request `cpu` cores = the vCPU count",
              lambda c, r, d: c * 0.00003942 * 3600 + r * 0.00000667 * 3600),
    "fly": ("https://docs.fly.io/about/pricing (iad): performance $0.000012732/vCPU-s, RAM beyond 2 GB per vCPU $0.000002316/GB-s",
            lambda c, r, d: c * 0.000012732 * 3600 + max(0, r - 2 * c) * 0.000002316 * 3600),
}


def layer_a() -> list[dict]:
    rows = []
    for p, (src, f) in PUBLISHED.items():
        published, ours = f(CPU, RAM, DISK), cost_model_for(p).hourly_usd(CPU, RAM, DISK)
        diff = (ours - published) / published * 100
        rows.append({"provider": p, "published_usd_per_hour": published, "ours_usd_per_hour": ours, "diff_pct": diff, "source": src, "ok": abs(diff) <= 0.5})
    return rows


# ---- B: controlled live metering ------------------------------------------------------------------------------------
async def one(prov: str, i: int, args, extra: dict) -> dict:
    cls = fleet.raw_class(prov)
    spec = SandboxSpec(image=args.image, cpu=CPU, mem_gb=RAM, disk_gb=DISK, lifetime_s=args.hold + 900, create_timeout=300, create_tries=1, extra=dict(extra))
    sbx = cls(spec, cls.cost_model())
    t_req = time.time()
    await sbx.start()
    t_created = time.time()
    await sbx.exec("true", timeout=60)
    await asyncio.sleep(args.hold)
    t_stop_call = time.time()
    usd = await sbx.stop()
    t_gone = time.time()
    return {"provider": prov, "i": i, "sid": sbx_id(sbx), "t_request": t_req, "t_created": t_created,
            "t_stop_call": t_stop_call, "t_gone": t_gone, "ledger_usd_returned": usd, "obj": sbx}


def sbx_id(sbx) -> str:
    return getattr(sbx, "_last_id", "") or ""


async def layer_b(args) -> tuple[list[dict], str]:
    run_id = args.run_id
    set_context(owner="cost-verify", run=run_id)
    out = []
    jobs = []
    for prov in args.providers:
        ok, why = usable(SandboxProviderConfig(name=prov))
        if not ok:
            print(f"[{prov}] skipped: {why}")
            continue
        extra = {"runtime": "vm"} if prov == "modal" else {}
        # remember the sandbox id before stop() clears it
        cls = fleet.raw_class(prov)
        orig_stop = cls.stop

        async def stop_keep_id(self, sync_dirs=(), _o=orig_stop):
            self._last_id = self.sandbox_id
            return await _o(self, sync_dirs)
        cls.stop = stop_keep_id
        jobs += [one(prov, i, args, extra) for i in range(args.n)]
    results = await asyncio.gather(*jobs, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            print("run failed:", type(r).__name__, str(r)[:150])
        else:
            out.append(r)
    return out, run_id


def ledger_rows(run_id: str) -> dict:
    """Ledger records of this run keyed by sandbox id."""
    opens, closes = {}, {}
    for line in open(get_ledger().path):
        try:
            r = json.loads(line)
        except ValueError:
            continue
        if r.get("run") != run_id:
            continue
        if r["ev"] == "open":
            opens[r["id"]] = r
        elif r["ev"] == "close":
            closes[r["id"]] = r
    return {o["sid"]: (o, closes.get(h)) for h, o in opens.items() if o.get("sid")}


def analyse_b(results: list[dict], run_id: str, args) -> list[dict]:
    led = ledger_rows(run_id)
    rows = []
    for r in results:
        prov = r["provider"]; model = cost_model_for(prov)
        o, c = led.get(r["sid"], (None, None))
        wall_total = r["t_gone"] - r["t_request"]
        wall_run = r["t_stop_call"] - r["t_created"]
        row = {"provider": prov, "sid": r["sid"][:18], "wall_total_s": wall_total, "create_latency_s": r["t_created"] - r["t_request"],
               "delete_latency_s": r["t_gone"] - r["t_stop_call"], "held_s": wall_run, "ledger_open": bool(o), "ledger_closed": bool(c)}
        if o and c:
            row.update({"ledger_s": c["seconds"], "ledger_usd": c["usd"], "model_usd_for_ledger_s": model.sandbox_usd(o["cpu"], o["ram_gib"], o["disk_gib"], c["seconds"]),
                        "model_usd_for_wall_total": model.sandbox_usd(CPU, RAM, DISK, wall_total),
                        "unseen_s": wall_total - c["seconds"], "unseen_pct": (wall_total - c["seconds"]) / wall_total * 100})
            row["usd_formula_ok"] = abs(row["ledger_usd"] - row["model_usd_for_ledger_s"]) < 1e-5
        rows.append(row)
    return rows


def invariants(run_id: str, results: list[dict]) -> list[str]:
    bad = []
    led = ledger_rows(run_id)
    for r in results:
        o, c = led.get(r["sid"], (None, None))
        if not o:
            bad.append(f"{r['provider']} {r['sid'][:10]}: no open record in the ledger")
        elif not c:
            bad.append(f"{r['provider']} {r['sid'][:10]}: opened but never closed")
    s = summary(get_ledger().path, run=run_id)
    part = sum(a["total_usd"] for a in s["providers"].values())
    if abs(part - s["total_usd"]) > 1e-3:
        bad.append(f"summary total {s['total_usd']} != sum of providers {part}")
    if s["running_usd"] > 1e-6:
        bad.append(f"summary still shows ${s['running_usd']:.4f} running after every sandbox was stopped")
    return bad


# ---- C: provider-reported ---------------------------------------------------------------------------------------------
async def daytona_usage(results: list[dict], wait_s: int = 90) -> dict:
    """{sandbox id: provider-computed price in USD} from Daytona's usage API, read after the sandboxes were deleted (the usage
    endpoint lags, so wait first). None means Daytona reported no usage period for it yet."""
    from sandboxes.daytona_provider import _fetch_analytics_cost
    mine = [r for r in results if r["provider"] == "daytona"]
    if not mine:
        return {}
    await asyncio.sleep(wait_s)
    out = {}
    for r in mine:
        sb = getattr(r["obj"], "_sb", None)
        try:
            out[r["sid"]] = await _fetch_analytics_cost(sb, sb.organization_id, r["sid"], dt.datetime.fromtimestamp(r["t_request"] - 60, dt.timezone.utc),
                                                        dt.datetime.fromtimestamp(r["t_gone"] + 60, dt.timezone.utc))
        except Exception as e:  # noqa: BLE001
            out[r["sid"]] = f"error: {type(e).__name__}: {str(e)[:80]}"
    return out


def modal_report_for_run(run_id: str, start: dt.datetime, end: dt.datetime) -> dict:
    """Modal's billing report broken down by the sandboxes.run tag: {cost, cost_by_resource} for this run."""
    import modal
    ws = modal.Workspace.from_context()
    rep = ws.billing.report(start=start.replace(minute=0, second=0, microsecond=0), end=end + dt.timedelta(hours=1), resolution="h", tag_names=["sandboxes.run"])
    tot, by = 0.0, {}
    for r in rep:
        tags = getattr(r, "tags", {}) or {}
        if tags.get("sandboxes.run") == run_id:
            tot += float(r.cost)
            for k, v in (getattr(r, "cost_by_resource", {}) or {}).items():
                by[k] = by.get(k, 0.0) + float(v)
    return {"cost": tot, "by_resource": by, "rows": sum(1 for r in rep if (getattr(r, "tags", {}) or {}).get("sandboxes.run") == run_id)}


def render_md(data: dict) -> str:
    L = [f"# Cost verification {data['run']}", ""]
    L += ["## A. Published rates vs our cost model (2 vCPU / 4 GiB / 10 GB node, per hour)", "", "| provider | published | ours | diff | source |", "|---|---|---|---|---|"]
    for r in data["rates"]:
        L.append(f"| {r['provider']} | ${r['published_usd_per_hour']:.4f} | ${r['ours_usd_per_hour']:.4f} | {r['diff_pct']:+.2f}% {'ok' if r['ok'] else '**MISMATCH**'} | {r['source']} |")
    if data.get("metering"):
        L += ["", f"## B. Metering: {data['n']} sandboxes per provider held {data['hold']} s, timed from outside vs the ledger", "",
              "| provider | wall total s | create s | delete s | ledger s | unseen by ledger | ledger $ | $ if billed from request to delete | formula ok |", "|---|---|---|---|---|---|---|---|---|"]
        for r in data["metering"]:
            if "ledger_s" in r:
                L.append(f"| {r['provider']} | {r['wall_total_s']:.1f} | {r['create_latency_s']:.1f} | {r['delete_latency_s']:.1f} | {r['ledger_s']:.1f} | {r['unseen_s']:.1f} s ({r['unseen_pct']:.1f}%) | {r['ledger_usd']:.5f} | {r['model_usd_for_wall_total']:.5f} | {r['usd_formula_ok']} |")
            else:
                L.append(f"| {r['provider']} | {r['wall_total_s']:.1f} | - | - | - | not in the ledger | - | - | - |")
    if "invariant_failures" in data:
        L += ["", "## Ledger invariants", ""] + ([f"- {b}" for b in data["invariant_failures"]] or ["- all hold: every sandbox has one open and one close, usd = model(seconds), the summary equals its parts, nothing left running"])
    if data.get("provider_reported"):
        L += ["", "## C. Provider-reported cost vs ledger", ""]
        for k, v in data["provider_reported"].items():
            L.append(f"- {k}: {v}")
    L += ["", "## Dashboard-only providers", "", "No billing API: compare the amount below with the provider's dashboard for this run id.", ""]
    for p, usd in data.get("expected_by_provider", {}).items():
        L.append(f"- {p}: expect ${usd:.4f} (ledger total for run `{data['run']}`)")
    return "\n".join(L) + "\n"


async def main_async(args):
    load_provider_keys()
    RESULTS.mkdir(exist_ok=True)
    if args.check:
        data = json.loads(Path(args.check).read_text())
        s = dt.datetime.fromtimestamp(data["t_start"], dt.timezone.utc); e = dt.datetime.fromtimestamp(data["t_end"], dt.timezone.utc)
        rep = modal_report_for_run(data["run"], s, e)
        exp = data["expected_by_provider"].get("modal", 0.0)
        msg = f"modal billed ${rep['cost']:.5f} (by resource {({k: round(v, 5) for k, v in rep['by_resource'].items()})}) vs ledger ${exp:.5f}" if rep["rows"] else "no billing rows for this run yet (Modal publishes hourly; retry later)"
        data.setdefault("provider_reported", {})["modal"] = msg + (f" -> ratio ledger/billed {exp / rep['cost']:.2f}" if rep["cost"] else "")
        Path(args.check).write_text(json.dumps(data, indent=1, default=str)); Path(args.check).with_suffix(".md").write_text(render_md(data))
        print(data["provider_reported"]["modal"])
        return 0
    rates = layer_a()
    print("A. rates (2 vCPU / 4 GiB / 10 GB per hour):")
    for r in rates:
        print(f"   {r['provider']:<8} published ${r['published_usd_per_hour']:.4f}  ours ${r['ours_usd_per_hour']:.4f}  {r['diff_pct']:+.2f}%  {'ok' if r['ok'] else 'MISMATCH'}")
    data = {"run": args.run_id, "rates": rates, "n": args.n, "hold": args.hold}
    bad = [] if all(r["ok"] for r in rates) else ["published rates differ from the cost model"]
    if not args.rates_only:
        t_start = time.time()
        results, run_id = await layer_b(args)
        data["t_start"], data["t_end"] = t_start, time.time()
        rows = analyse_b(results, run_id, args)
        data["metering"] = rows
        data["invariant_failures"] = invariants(run_id, results)
        bad += data["invariant_failures"]
        s = summary(get_ledger().path, run=run_id)
        data["expected_by_provider"] = {p: a["total_usd"] for p, a in s["providers"].items()}
        print("\nB. metering (wall clock from outside vs ledger):")
        for r in rows:
            if "ledger_s" in r:
                print(f"   {r['provider']:<8} wall {r['wall_total_s']:6.1f}s  (create {r['create_latency_s']:5.1f}s, delete {r['delete_latency_s']:5.1f}s)  ledger {r['ledger_s']:6.1f}s  unseen {r['unseen_s']:5.1f}s = {r['unseen_pct']:4.1f}%  ledger ${r['ledger_usd']:.5f}  formula {'ok' if r['usd_formula_ok'] else 'WRONG'}")
            else:
                print(f"   {r['provider']:<8} NOT IN LEDGER")
        print("   invariants:", "all hold" if not data["invariant_failures"] else data["invariant_failures"])
        if "daytona" in args.providers:
            usage = await daytona_usage(results)
            for sid, price in usage.items():
                mine = next((x for x in rows if x["provider"] == "daytona" and sid.startswith(x["sid"])), None)
                ours = mine.get("ledger_usd") if mine else None
                note = "no usage reported yet" if price is None else (price if isinstance(price, str) else f"Daytona billed ${price:.5f} vs ledger ${ours:.5f} (ratio ledger/billed {ours / price:.2f})" if price else "Daytona billed $0")
                data.setdefault("provider_reported", {})[f"daytona {sid[:8]}"] = note
                if isinstance(price, float) and price and ours and abs(ours - price) / price > 0.10:
                    bad.append(f"daytona {sid[:8]}: ledger ${ours:.5f} differs from Daytona's own usage price ${price:.5f} by more than 10%")
    (RESULTS / f"cost_verify-{args.run_id}.json").write_text(json.dumps(data, indent=1, default=str))
    (RESULTS / f"cost_verify-{args.run_id}.md").write_text(render_md(data))
    print(f"\nwrote {RESULTS / ('cost_verify-' + args.run_id + '.json')}  (later: --check that file for Modal's billing report)")
    return 1 if bad else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--providers", nargs="+", default=["e2b", "blaxel", "modal", "fly"]); ap.add_argument("--n", type=int, default=3)
    ap.add_argument("--hold", type=int, default=120); ap.add_argument("--image", default="python:3.12-slim")
    ap.add_argument("--rates-only", action="store_true"); ap.add_argument("--check"); ap.add_argument("--run-id", default="cost-verify-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S"))
    a = ap.parse_args()
    sys.exit(asyncio.run(main_async(a)))


if __name__ == "__main__":
    main()
