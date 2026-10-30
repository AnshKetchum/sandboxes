#!/usr/bin/env python3
"""GKE sandbox benchmark, through the backend's own create/exec/stop path: what one sandbox gets on each node pool (disk, CPU,
exec round trip, file transfer), and how a fleet of N behaves (time to ready, exec latency and disk under load, nodes added).

    python scripts/gke/bench.py probe --pool c3d-30-lssd-spot c3d-30-spot           # one sandbox per pool, probes in turn
    python scripts/gke/bench.py probe --pool c3d-30-lssd-spot --count 25 --load     # 25 on one pool, every probe at once
    python scripts/gke/bench.py ramp --count 1000 --hold 120                        # placement picks the pools (catalog)
    python scripts/gke/bench.py ramp --count 200 --pool c3d-90-lssd-spot --load     # one pool, disk probe on all 200 at once

probe: per sandbox, client side: open time, exec round trip (`true` x10), upload 1 / 16 MiB, download 16 MiB; in the sandbox
(one python run, --rounds times): nproc and the cgroup limits, a 10^7-step python loop, 256 MB sequential write + fsync, 3,000
small files written + synced + stat'd + read (the field notes' probes), 200 x 4 KiB append + fsync latency, and a venv with pip
(a pip-install-like burst of ~1,000 files). With --load every sandbox runs its probes at the same moment (a node's disk is
shared: what a busy node gives each sandbox), else one sandbox at a time.

ramp: N sandboxes opened at once (the library's create rate limit applies), then `true` on all at once, then the probe on all
(--load) or none, --warm-opens sandboxes opened one at a time onto the warm nodes (open latency with nodes up and the image
cached), a hold, and stop. Records each open's time and failure, the nodes per pool before and after, and the exec
latency spread. Use it for the 1K target: it shows where the fleet stops (quota, IPs, API server, client threads).

Every sandbox is labelled owner=benchmark and run=<run id> (`sandboxes stop --run <id>` ends anything a crashed run left) and
has a short lifetime (hold + 30 min). Results: JSON in scripts/benchmark_results/gke-<run>.json and a line per pool/level in
scripts/benchmark_results/gke-summary.md. Needs gcloud on PATH and the cluster's kubeconfig; SANDBOX_GKE_CONFIG picks the
catalog (default configs/gke.yaml).
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import datetime as dt
import json
import os
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

RESULTS = Path(__file__).resolve().parents[1] / "benchmark_results"
IMAGE = "mirror.gcr.io/library/python:3.12-slim"

PROBE = r'''
import json, os, subprocess, sys, time
out = {"nproc": os.cpu_count(), "affinity": len(os.sched_getaffinity(0))}
for k, f in (("cpu_max", "/sys/fs/cgroup/cpu.max"), ("mem_max", "/sys/fs/cgroup/memory.max")):
    try:
        out[k] = open(f).read().strip()
    except OSError:
        out[k] = None
os.makedirs("/work/bench", exist_ok=True)
os.chdir("/work/bench")
t = time.time(); sum(i * i for i in range(10 ** 7)); out["cpu_loop_s"] = time.time() - t
t = time.time()
with open("seq.bin", "wb") as f:
    for _ in range(256):
        f.write(bytes(1 << 20))
    f.flush(); os.fsync(f.fileno())
out["seq_write_fsync_s"] = time.time() - t
os.remove("seq.bin")
os.makedirs("small", exist_ok=True)
t = time.time()
for i in range(3000):
    with open(f"small/f{i}", "w") as f:
        f.write(str(i))
os.sync()
for i in range(3000):
    os.stat(f"small/f{i}"); open(f"small/f{i}").read()
out["small_files_s"] = time.time() - t
for i in range(3000):
    os.remove(f"small/f{i}")
lat = []
with open("fsync.bin", "ab") as f:
    for _ in range(200):
        t = time.time(); f.write(b"x" * 4096); f.flush(); os.fsync(f.fileno()); lat.append(time.time() - t)
os.remove("fsync.bin")
lat.sort()
out["fsync_ms_p50"], out["fsync_ms_p99"] = 1000 * lat[100], 1000 * lat[197]
t = time.time()
r = subprocess.run([sys.executable, "-m", "venv", "/work/bench/venv"], capture_output=True)
out["venv_s"] = (time.time() - t) if r.returncode == 0 else None
subprocess.run(["rm", "-rf", "/work/bench/venv"])
print("PROBE " + json.dumps(out))
'''

PROBE_KEYS = ("cpu_loop_s", "seq_write_fsync_s", "small_files_s", "fsync_ms_p50", "fsync_ms_p99", "venv_s")


def pct(v, p):
    v = sorted(x for x in v if x is not None)
    return v[min(len(v) - 1, int(len(v) * p))] if v else None


def med(v):
    v = [x for x in v if x is not None]
    return statistics.median(v) if v else None


def fmt(x, f="{:.2f}"):
    return "-" if x is None else f.format(x)


def classify(e: BaseException) -> str:
    return f"{type(e).__name__}: {str(e)[:120]}"


def nodes_per_pool(api) -> dict:
    from sandboxes.gke_place import POOL_LABEL
    out: dict = collections.Counter()
    for n in api.list_node(label_selector=POOL_LABEL).items or []:
        out[(n.metadata.labels or {}).get(POOL_LABEL)] += 1
    return dict(out)


def make_spec(args, pool):
    from sandboxes.raw import SandboxSpec
    extra = {"cpu_request": args.cpu_request, "spot": "prefer", "ready_timeout_s": args.ready_timeout}
    extra.update(dict(kv.split("=", 1) for kv in args.extra))
    for k in ("cpu_request", "ephemeral_request_gb", "capacity_wait_s", "ready_timeout_s"):
        if k in extra:
            extra[k] = float(extra[k])
    if pool:
        extra["pool"] = pool
    return SandboxSpec(image=args.image, cpu=args.cpu, mem_gb=args.mem_gb, disk_gb=args.disk_gb,
                       lifetime_s=int(args.hold + 1800), create_timeout=int(args.ready_timeout), create_tries=1, extra=extra)


async def open_one(args, pool, rec: dict):
    from sandboxes.backends.gke_raw import GKERaw
    sbx = GKERaw(make_spec(args, pool))
    t = time.time()
    try:
        await sbx.start()
    except BaseException as e:  # noqa: BLE001
        rec["error"] = classify(e)
        rec["open_s"] = time.time() - t
        return None
    rec["open_s"] = time.time() - t
    rec["id"], rec["pool"] = sbx.sandbox_id, sbx.pool
    return sbx


async def client_probe(sbx, rec: dict) -> None:
    try:
        await _client_probe(sbx, rec)
    except Exception as e:  # noqa: BLE001 — one sandbox's failure is a result, not the end of the run
        rec.setdefault("errors", []).append("client probe: " + classify(e))


async def _client_probe(sbx, rec: dict) -> None:
    rtt = []
    for _ in range(10):
        t = time.time()
        r = await sbx.exec("true", timeout=60)
        rtt.append(time.time() - t)
        if r.rc != 0:
            rec.setdefault("errors", []).append(f"exec true rc={r.rc}: {r.stdout[-200:]}")
    rec["exec_rtt_p50"], rec["exec_rtt_max"] = med(rtt), max(rtt)
    for mib in (1, 16):
        data = os.urandom(mib << 20)
        t = time.time()
        await sbx._upload(data, f"/tmp/up{mib}.bin")
        rec[f"upload_{mib}mib_s"] = time.time() - t
    t = time.time()
    got = await sbx._download("/tmp/up16.bin")
    rec["download_16mib_s"] = time.time() - t
    if len(got) != 16 << 20:
        rec.setdefault("errors", []).append(f"download returned {len(got)} bytes")


async def sandbox_probe(sbx, rec: dict, rounds: int) -> None:
    try:
        await _sandbox_probe(sbx, rec, rounds)
    except Exception as e:  # noqa: BLE001
        rec.setdefault("errors", []).append("sandbox probe: " + classify(e))


async def _sandbox_probe(sbx, rec: dict, rounds: int) -> None:
    await sbx._upload(PROBE.encode(), "/tmp/probe.py")
    for _ in range(rounds):
        r = await sbx.exec("python3 /tmp/probe.py", timeout=900)
        line = next((l for l in r.stdout.splitlines() if l.startswith("PROBE ")), None)
        if r.rc != 0 or not line:
            rec.setdefault("errors", []).append(f"probe rc={r.rc}: {r.stdout[-300:]}")
            continue
        rec.setdefault("probes", []).append(json.loads(line[6:]))


def summarize(recs: list) -> dict:
    ok = [r for r in recs if "id" in r]
    s = {"n": len(recs), "opened": len(ok), "open_failures": dict(collections.Counter(r["error"] for r in recs if "error" in r)),
         "open_p50": pct([r["open_s"] for r in ok], .5), "open_p90": pct([r["open_s"] for r in ok], .9),
         "open_max": max((r["open_s"] for r in ok), default=None)}
    for k in ("exec_rtt_p50", "upload_1mib_s", "upload_16mib_s", "download_16mib_s"):
        s[k] = med([r.get(k) for r in ok])
    probes = [p for r in ok for p in r.get("probes", [])]
    for k in PROBE_KEYS:
        s[k] = med([p.get(k) for p in probes])
        s[k + "_p90"] = pct([p.get(k) for p in probes], .9)
    if probes:
        s["nproc"], s["cpu_max"] = probes[0].get("nproc"), probes[0].get("cpu_max")
    s["probe_errors"] = sum(len(r.get("errors", [])) for r in ok)
    return s


async def stop_all(boxes) -> float:
    async def stop(b):
        try:
            return await b.stop()
        except BaseException:  # noqa: BLE001
            return 0.0
    return sum(await asyncio.gather(*(stop(b) for b in boxes if b is not None)))


async def run_probe(args, api) -> list:
    rows = []
    for pool in args.pool:
        print(f"\n== {pool}: {args.count} sandbox(es), {'all at once' if args.load else 'one at a time'}", flush=True)
        recs = [{"i": i} for i in range(args.count)]
        boxes = await asyncio.gather(*(open_one(args, pool, r) for r in recs))
        try:
            live = [(b, r) for b, r in zip(boxes, recs) if b is not None]
            if args.load:
                await asyncio.gather(*(client_probe(b, r) for b, r in live))
                await asyncio.gather(*(sandbox_probe(b, r, args.rounds) for b, r in live))
            else:
                for b, r in live:
                    await client_probe(b, r)
                    await sandbox_probe(b, r, args.rounds)
        finally:
            cost = await stop_all(boxes)
        s = {"pool": pool, "mode": "load" if args.load else "isolated", **summarize(recs), "pod_cost_usd": cost}
        rows.append({"summary": s, "sandboxes": recs})
        print(probe_line(s), flush=True)
    return rows


def probe_line(s: dict) -> str:
    return (f"  {s['pool']:<20} {s['mode']:<8} {s['opened']}/{s['n']} open p50 {fmt(s['open_p50'], '{:.1f}')}s | exec "
            f"{fmt(s['exec_rtt_p50'], '{:.3f}')}s | up16 {fmt(s['upload_16mib_s'])}s dn16 {fmt(s['download_16mib_s'])}s | cpu "
            f"{fmt(s['cpu_loop_s'])}s | seq+fsync {fmt(s['seq_write_fsync_s'])}s (p90 {fmt(s['seq_write_fsync_s_p90'])}) | "
            f"3k files {fmt(s['small_files_s'])}s (p90 {fmt(s['small_files_s_p90'])}) | fsync {fmt(s['fsync_ms_p50'])}/"
            f"{fmt(s['fsync_ms_p99'])} ms | venv {fmt(s['venv_s'])}s | nproc {s.get('nproc')}"
            + (f" | FAILED {s['open_failures']}" if s["open_failures"] else "") + (f" | {s['probe_errors']} probe errors" if s["probe_errors"] else ""))


async def run_ramp(args, api) -> list:
    pool = args.pool[0] if args.pool else None
    before = nodes_per_pool(api)
    print(f"\n== ramp: {args.count} sandboxes on {pool or 'the catalog (placement)'}; nodes before {before}", flush=True)
    recs = [{"i": i} for i in range(args.count)]
    t0 = time.time()
    progress = {"done": 0}

    async def one(r):
        b = await open_one(args, pool, r)
        r["ready_at"] = time.time() - t0
        progress["done"] += 1
        if progress["done"] % max(1, args.count // 20) == 0:
            print(f"  {progress['done']}/{args.count} settled at {time.time() - t0:.0f}s", flush=True)
        return b

    boxes = await asyncio.gather(*(one(r) for r in recs))
    t_open = time.time() - t0
    try:
        live = [(b, r) for b, r in zip(boxes, recs) if b is not None]
        lat = []

        async def ping(b, r):
            t = time.time()
            res = await b.exec("true", timeout=120)
            r["exec_under_load_s"] = time.time() - t
            if res.rc != 0:
                r.setdefault("errors", []).append(f"exec rc={res.rc}: {res.stdout[-200:]}")
            lat.append(r["exec_under_load_s"])
        t = time.time()
        await asyncio.gather(*(ping(b, r) for b, r in live))
        t_ping = time.time() - t
        if args.load:
            await asyncio.gather(*(sandbox_probe(b, r, 1) for b, r in live))
        during = nodes_per_pool(api)
        warm = []
        for _ in range(args.warm_opens):            # one at a time, onto nodes already up with the image cached
            r = {}
            b = await open_one(args, pool, r)
            if b is not None:
                t = time.time()
                await b.exec("true", timeout=60)
                r["first_exec_s"] = time.time() - t
                await b.stop()
            warm.append(r)
        if args.hold:
            print(f"  holding {args.hold:.0f}s", flush=True)
            await asyncio.sleep(args.hold)
    finally:
        t = time.time()
        cost = await stop_all(boxes)
        t_stop = time.time() - t
    s = {"pool": pool or "placement", "mode": "ramp", **summarize(recs), "all_settled_s": t_open, "exec_all_s": t_ping,
         "exec_under_load_p50": pct(lat, .5), "exec_under_load_p90": pct(lat, .9), "exec_under_load_max": max(lat, default=None),
         "stop_all_s": t_stop, "nodes_before": before, "nodes_during": during,
         "warm_open_p50": pct([r["open_s"] for r in warm if "id" in r], .5),
         "warm_open_max": max((r["open_s"] for r in warm if "id" in r), default=None),
         "warm_first_exec_p50": pct([r.get("first_exec_s") for r in warm if "id" in r], .5),
         "per_pool": dict(collections.Counter(r.get("pool") for r in recs if "id" in r)), "pod_cost_usd": cost}
    print(f"  opened {s['opened']}/{s['n']} in {t_open:.0f}s (open p50 {fmt(s['open_p50'], '{:.1f}')}s p90 "
          f"{fmt(s['open_p90'], '{:.1f}')}s max {fmt(s['open_max'], '{:.1f}')}s); exec on all at once {t_ping:.1f}s (p50 "
          f"{fmt(s['exec_under_load_p50'], '{:.2f}')}s p90 {fmt(s['exec_under_load_p90'], '{:.2f}')}s); stop all {t_stop:.1f}s; "
          f"per pool {s['per_pool']}; nodes during {during}; warm open p50 {fmt(s['warm_open_p50'])}s max "
          f"{fmt(s['warm_open_max'])}s (first exec {fmt(s['warm_first_exec_p50'], '{:.3f}')}s)", flush=True)
    if s["open_failures"]:
        for k, v in s["open_failures"].items():
            print(f"    {v}x {k}")
    if args.load:
        print(probe_line(s))
    return [{"summary": s, "sandboxes": recs}]


async def main_async(args) -> int:
    from sandboxes import set_context
    from sandboxes.backends.gke_raw import _core
    run_id = args.run or "gke-bench-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    os.environ["SANDBOX_RUN_ID"] = run_id
    set_context(owner="benchmark", run=run_id)
    api = _core()
    est = args.count * len(args.pool or [None]) * (args.hold + 600) / 3600 * 0.03
    if est > args.budget_usd:
        print(f"estimated pod cost ${est:.2f} is over --budget-usd {args.budget_usd:g}; raise it to run this", file=sys.stderr)
        return 2
    print(f"run {run_id}: image {args.image}, {args.cpu} vCPU (request x{args.cpu_request:g}) / {args.mem_gb} GiB / "
          f"{args.disk_gb} GB, extra {args.extra}", flush=True)
    rows = await (run_probe(args, api) if args.cmd == "probe" else run_ramp(args, api))
    RESULTS.mkdir(exist_ok=True)
    out = RESULTS / f"gke-{run_id}.json"
    out.write_text(json.dumps({"run": run_id, "args": vars(args), "at": time.time(), "rows": rows}, indent=1, default=str))
    with open(RESULTS / "gke-summary.md", "a") as f:
        for row in rows:
            s = row["summary"]
            f.write(f"- {run_id} {args.cmd} {s['pool']} {s['mode']} n={s['n']}: {json.dumps({k: (round(v, 4) if isinstance(v, float) else v) for k, v in s.items() if k not in ('pool', 'mode', 'n')}, default=str)}\n")
    print(f"\nwrote {out}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["probe", "ramp"])
    ap.add_argument("--pool", nargs="*", default=[], help="catalog pool(s); ramp without one lets placement choose")
    ap.add_argument("--count", type=int, default=1)
    ap.add_argument("--load", action="store_true", help="run the in-sandbox probe on every sandbox at once")
    ap.add_argument("--rounds", type=int, default=2, help="in-sandbox probe repetitions (probe mode)")
    ap.add_argument("--hold", type=float, default=0.0, help="ramp: seconds to keep the fleet up before stopping it")
    ap.add_argument("--warm-opens", type=int, default=10, help="ramp: sandboxes opened one at a time once the fleet is up")
    ap.add_argument("--image", default=IMAGE)
    ap.add_argument("--cpu", type=int, default=1)
    ap.add_argument("--cpu-request", type=float, default=1.0, help="fraction of --cpu requested (1.0: Guaranteed, pinned)")
    ap.add_argument("--mem-gb", type=int, default=4)
    ap.add_argument("--disk-gb", type=int, default=10)
    ap.add_argument("--ready-timeout", type=float, default=1200)
    ap.add_argument("--extra", nargs="*", default=[], help="provider extra key=value (e.g. ephemeral_request_gb=2)")
    ap.add_argument("--budget-usd", type=float, default=10.0, help="refuse a run whose pod-hours estimate is above this")
    ap.add_argument("--run", default=None)
    args = ap.parse_args(argv)
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
