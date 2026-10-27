#!/usr/bin/env python3
"""Live check of GKE sandboxes on the real cluster: N sandboxes of one harbor task, built from its Dockerfile, each checked end
to end; then whether the ledger charged what the nodes really cost, and (with --down) whether anything is left that bills.

    python scripts/gke/live_check.py --task <harbor task dir> --count 1 --run gke-live-1
    python scripts/gke/live_check.py --task <harbor task dir> --count 8 --run gke-live-2 --down

Per sandbox: start (timed), pool and recorded rate, the VM it really runs on (machine type, spot, boot disk), commands (cwd,
env, exit codes, timeout), a 1 MiB byte-exact file round trip, network (DNS and the internet reachable; the metadata server
and cluster addresses not), the task's verifier run on its reference solution (reward 1), stop.
Costs: an observer polls the cluster's nodes every 10 s for the whole check; each node's observed life x its catalog price
(+ boot disk), the cluster fee and Cloud Build's own build durations give the expected bill, compared with every gke record the
ledgers hold for the window. The meter may over-charge by up to one pass per node (never under). With --down: `gke_ops down`,
the observer runs until the last node is gone, then `gke_ops residue` must show nothing but the control plane.
Writes <run>.json in the job's working directory (--out).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def gcloud_json(*args):
    p = subprocess.run(["gcloud", *args, "--format=json"], capture_output=True, text=True, timeout=300)
    return json.loads(p.stdout or "null") if p.returncode == 0 else None


class NodeObserver(threading.Thread):
    """Every node the cluster shows, from first to last sighting (creation time from the API)."""

    def __init__(self, api, every: float = 10.0):
        super().__init__(daemon=True)
        self.api, self.every, self.nodes, self.stop_at, self.lock = api, every, {}, None, threading.Lock()

    def run(self):
        from sandboxes.backends.gke_raw import epoch
        while self.stop_at is None or time.time() < self.stop_at:
            try:
                items = self.api.list_node().items or []
            except Exception as e:  # noqa: BLE001
                print(f"  observer: {e!r}", flush=True)
                time.sleep(self.every)
                continue
            now = time.time()
            with self.lock:
                for n in items:
                    lab = n.metadata.labels or {}
                    rec = self.nodes.setdefault(n.metadata.name, {"created": epoch(n.metadata.creation_timestamp), "first": now,
                                                                  "pool": lab.get("cloud.google.com/gke-nodepool"),
                                                                  "machine": lab.get("node.kubernetes.io/instance-type"),
                                                                  "spot": lab.get("cloud.google.com/gke-spot") == "true"})
                    rec["last"] = now
            time.sleep(self.every)


CHECKS = {
    "dns": "python3 -c \"import socket; socket.gethostbyname('pypi.org')\"",
    "internet": "python3 -c \"import urllib.request as u; u.urlopen('https://pypi.org/simple/', timeout=15)\"",
    "metadata_blocked": "python3 -c \"import urllib.request as u; u.urlopen(u.Request('http://169.254.169.254/computeMetadata/v1/"
                        "instance/service-accounts/default/token', headers={'Metadata-Flavor': 'Google'}), timeout=5)\"",
    "cluster_blocked": "python3 -c \"import socket; s=socket.create_connection(('kubernetes.default.svc.cluster.local', 443), 5)\"",
}


async def check_one(sb, task: Path, idx: int) -> dict:
    out = {"id": sb.sandbox_id, "pool": sb.pool, "rate_h": sb.hourly_rate()}
    r = await sb.exec("nproc; free -g | awk '/Mem/{print $2}'; df -BG --output=size / | tail -1", timeout=60)
    out["inside"] = r.stdout.split()
    r = await sb.exec('pwd && echo "$FOO" && exit 3', cwd="/tmp", env={"FOO": f"bar{idx}"}, timeout=60)
    out["cwd_env_rc"] = (r.stdout.split(), r.rc) == (["/tmp", f"bar{idx}"], 3)
    r = await sb.exec("sleep 30", timeout=3)
    out["timeout"] = r.rc != 0 and "timed out" in r.stdout.lower()
    blob = os.urandom(1 << 20)
    src = Path(f"/tmp/live-{os.getpid()}-{idx}.bin"); src.write_bytes(blob)
    await sb.put_file(src, "/tmp/blob.bin")
    back = Path(f"/tmp/live-{os.getpid()}-{idx}.back")
    await sb.get_file("/tmp/blob.bin", back)
    out["file_roundtrip"] = back.read_bytes() == blob
    src.unlink(); back.unlink()
    for name, cmd in CHECKS.items():
        r = await sb.exec(cmd, timeout=60)
        out[name] = (r.rc != 0) if name.endswith("_blocked") else (r.rc == 0)
    await sb.put_file(task / "tests", "/tests")
    await sb.put_file(task / "solution", "/solution")
    r = await sb.exec("mkdir -p /logs/verifier /outputs && bash /solution/solve.sh && cd /app && bash /tests/test.sh "
                      "&& cat /logs/verifier/reward.txt", timeout=600)
    out["verifier_reward"] = (r.stdout.strip().splitlines() or [""])[-1]
    return out


def pod_timeline(api, pod_name: str, ns: str) -> dict:
    """Seconds from the pod's creation to each step of its start, from its events: scheduled (a node had room: the wait for a
    node scale-up ends here), image pulled, container started."""
    from sandboxes.backends.gke_raw import epoch
    try:
        pod = api.read_namespaced_pod(pod_name, ns)
        evs = api.list_namespaced_event(ns, field_selector=f"involvedObject.name={pod_name}").items or []
    except Exception as e:  # noqa: BLE001
        return {"error": repr(e)[:200]}
    t0 = epoch(pod.metadata.creation_timestamp)
    out = {}
    for ev in evs:
        t = epoch(getattr(ev, "event_time", None)) or epoch(ev.last_timestamp) or epoch(ev.first_timestamp)
        key = {"Scheduled": "scheduled", "TriggeredScaleUp": "scale_up_triggered", "Pulling": "pull_start", "Pulled": "pulled",
               "Started": "started"}.get(ev.reason)
        if key and t and t0:
            out[key] = round(t - t0, 1)
            if ev.reason == "Pulled":
                out["pull_message"] = (ev.message or "")[:160]
    return out


def vm_facts(node: str, zone_hint: str) -> dict:
    """What GCE says the node is: machine type, provisioning model, boot disk type and size."""
    for z in [zone_hint] + [f"us-central1-{s}" for s in "abcf"]:
        d = gcloud_json("compute", "instances", "describe", node, "--zone", z)
        if d:
            disk = gcloud_json("compute", "disks", "describe", d["disks"][0]["source"].rsplit("/", 1)[-1], "--zone", z) or {}
            return {"zone": z, "machine": d["machineType"].rsplit("/", 1)[-1],
                    "model": (d.get("scheduling") or {}).get("provisioningModel", "STANDARD"),
                    "disk_gb": int(disk.get("sizeGb", 0)), "disk_type": disk.get("type", "").rsplit("/", 1)[-1]}
    return {}


def expected_bill(observed: dict, since: float, until: float, mcfg, fee_h: float, builds: list, fee_from: float = None) -> dict:
    """What the cluster cost over what the ledger covers: each observed node from max(since, its creation) to its last
    sighting, the fee from `fee_from` (the cluster's creation when the meter had never run, else `since`)."""
    from sandboxes.gke_meter import Node, node_price_hour
    nodes, total = {}, 0.0
    for name, o in observed.items():
        start = max(since, o["created"] or o["first"])
        end = min(until, o["last"])
        price = node_price_hour(Node(name, start, o["machine"] or "?", o["spot"], o["pool"] or "", 0), mcfg)
        usd = max(0.0, end - start) * price / 3600
        nodes[name] = {"pool": o["pool"], "hours": round((end - start) / 3600, 4), "price_h": round(price, 5), "usd": round(usd, 5)}
        total += usd
    fee = (until - (fee_from or since)) * fee_h / 3600
    build_usd = sum(b["usd"] for b in builds)
    return {"nodes": nodes, "nodes_usd": total, "cluster_fee_usd": fee, "builds_usd": build_usd, "cluster_usd": total + fee,
            "total": total + fee + build_usd}


def cloud_builds(since: float, rate_min: float) -> list:
    import math
    out = []
    for b in gcloud_json("builds", "list", "--region", "us-central1", "--limit", "50") or []:
        from sandboxes.gke_build import _epoch
        s, f = _epoch(b.get("startTime")), _epoch(b.get("finishTime"))
        if s and s >= since - 60 and f:
            out.append({"id": b["id"], "status": b["status"], "minutes": round((f - s) / 60, 2),
                        "usd": math.ceil((f - s) / 60) * rate_min})
    return out


async def main_async(a) -> dict:
    os.environ["SANDBOX_RUN_ID"] = a.run
    from sandboxes import gke_meter, gke_ops
    from sandboxes.backends import gke_raw
    from sandboxes.backends.gke_raw import GKERaw
    from sandboxes.gke_build import BuildConfig
    from sandboxes.raw import SandboxSpec
    api = gke_raw._core()
    cfg, bcfg, mcfg = gke_ops.OpsConfig.load(), BuildConfig.load(), gke_meter.MeterConfig.load()
    task = Path(a.task).resolve()
    env = task / "environment"
    report: dict = {"run": a.run, "count": a.count, "task": str(task), "since": time.time()}
    fresh = not gke_meter.load_state().get("last_pass")             # the meter's first pass backfills from the cluster's creation
    report["meter_baseline"] = gke_meter.cluster_created_from_api(api) if fresh else report["since"]
    obs = NodeObserver(api)
    obs.start()
    gke_meter.meter_once(api, force=True)

    def spec():
        return SandboxSpec(image="", cpu=1, mem_gb=4, disk_gb=10, lifetime_s=3600, create_tries=1,
                           labels={"sandboxes.run": a.run},
                           extra={"dockerfile": str(env / "Dockerfile"), "context_dir": str(env), "cpu_request": 0.95})
    sbs = [GKERaw(spec()) for _ in range(a.count)]
    t0 = time.time()

    async def start(sb):
        t = time.time()
        await sb.start()
        return time.time() - t
    starts = await asyncio.gather(*(start(sb) for sb in sbs), return_exceptions=True)
    report["start_s"] = [round(s, 1) if isinstance(s, float) else repr(s)[:300] for s in starts]
    live = [sb for sb, s in zip(sbs, starts) if isinstance(s, float)]
    print(f"started {len(live)}/{a.count} in {time.time() - t0:.0f}s: {report['start_s']}", flush=True)
    report["timelines"] = {sb.sandbox_id: pod_timeline(api, sb.sandbox_id, gke_raw.namespace()) for sb in live}
    print(f"timelines (s after pod creation): {report['timelines']}", flush=True)
    if live:                                                         # a warm start: same task, a node already up
        warm = GKERaw(spec())
        t = time.time()
        try:
            await warm.start()
            report["warm_start_s"] = round(time.time() - t, 1)
            report["warm_timeline"] = pod_timeline(api, warm.sandbox_id, gke_raw.namespace())
            live.append(warm)
        except Exception as e:  # noqa: BLE001
            report["warm_start_s"] = repr(e)[:300]
        print(f"warm start: {report['warm_start_s']}s {report.get('warm_timeline')}", flush=True)
    try:
        report["sandboxes"] = await asyncio.gather(*(check_one(sb, task, i) for i, sb in enumerate(live)), return_exceptions=True)
        report["sandboxes"] = [s if isinstance(s, dict) else {"error": repr(s)[:400]} for s in report["sandboxes"]]
        pods = {p.metadata.name: p for p in gke_raw.list_managed_pods(api, gke_raw.namespace())}
        report["vms"] = {}
        for sb in live:
            node = pods[sb.sandbox_id].spec.node_name if sb.sandbox_id in pods else None
            if node and node not in report["vms"]:
                report["vms"][node] = vm_facts(node, "us-central1-a")
    finally:
        await asyncio.gather(*(sb.stop() for sb in live), return_exceptions=True)
        Path(a.out, f"{a.run}.partial.json").write_text(json.dumps(report, indent=1, default=str))   # whatever happens next
    report["housekeep"] = {k: v for k, v in gke_ops.housekeep(api, cfg).items() if v}
    if a.down:
        report["down"] = {k: v for k, v in gke_ops.down(api, cfg).items() if k != "residue"}
        report["residue"] = gke_ops.residue(cfg)
    else:
        print("waiting for the sandbox nodes to scale down (idle_scale_down_min / autoscaler)...", flush=True)
        while any(o["pool"] in cfg.sandbox_pools and time.time() - o["last"] < 30 for o in obs.nodes.values()):
            await asyncio.sleep(30)
            gke_meter.meter_once(api)
    gke_meter.meter_once(api, force=True)
    obs.stop_at = time.time() + 15
    obs.join()
    until = time.time()
    report["until"] = until
    report["builds"] = cloud_builds(report["since"], bcfg.usd_per_minute)
    base = report["meter_baseline"]
    report["expected"] = expected_bill(obs.nodes, base, until, mcfg, mcfg.cluster_fee_hour, report["builds"], fee_from=base)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from verify_costs_gke import ledger_side
    report["ledger"] = ledger_side(report["since"] - 1, until + 1)
    return report


def summarize(r: dict) -> None:
    print(f"\n== {r['run']}: {r['count']} sandbox(es) of {Path(r['task']).name}")
    for s in r.get("sandboxes", []):
        if "error" in s:
            print(f"  ERROR {s['error']}")
            continue
        flags = {k: v for k, v in s.items() if isinstance(v, bool)}
        bad = [k for k, v in flags.items() if not v]
        print(f"  {s['id']} pool={s['pool']} rate=${s['rate_h']:.5f}/h inside(nproc,GiB,disk)={s['inside']} "
              f"reward={s['verifier_reward']} {'ALL CHECKS OK' if not bad and s['verifier_reward'] == '1' else 'FAILED: ' + str(bad)}")
    for node, v in r.get("vms", {}).items():
        print(f"  node {node}: {v}")
    print(f"  start (s): cold {r.get('start_s')}  warm {r.get('warm_start_s')}")
    for sid, tl in (r.get("timelines") or {}).items():
        print(f"    {sid}: {tl}")
    if r.get("warm_timeline"):
        print(f"    warm: {r['warm_timeline']}")
    for b in r.get("builds", []):
        print(f"  build {b['id']}: {b['status']} {b['minutes']} min -> ${b['usd']:.4f}")
    e, l = r["expected"], r["ledger"]
    print(f"  expected (observed nodes x catalog + fee + builds): nodes ${e['nodes_usd']:.4f} + fee ${e['cluster_fee_usd']:.4f} "
          f"+ builds ${e['builds_usd']:.4f} = ${e['total']:.4f}")
    for n, v in e["nodes"].items():
        print(f"    {n}: {v}")
    print(f"  ledger: pods ${l['pods']:.4f} + nodes ${l['nodes']:.4f} + fee ${l['cluster_fee']:.4f} = ${l['total']:.4f}"
          f"   builds ${l['builds']:.4f}  image storage ${l['image_storage']:.6f}   (gaps {l['gap_max_usd']:.4f}, open {l['still_open']})")
    cl, ex = l["total"], e["cluster_usd"]
    print(f"  cluster: ledger ${cl:.4f} vs observed ${ex:.4f} -> {cl / ex:.3f}x" if ex else "  cluster: nothing observed")
    print(f"  builds:  ledger ${l['builds']:.4f} vs Cloud Build ${e['builds_usd']:.4f}")
    if "residue" in r:
        from sandboxes.gke_ops import format_residue
        print("  left in the project after down:\n" + format_residue(r["residue"]))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--task", required=True)
    ap.add_argument("--count", type=int, default=1)
    ap.add_argument("--run", default=f"gke-live-{random.randint(1000, 9999)}")
    ap.add_argument("--down", action="store_true")
    ap.add_argument("--out", default=".")
    a = ap.parse_args(argv)
    r = asyncio.run(main_async(a))
    path = Path(a.out) / f"{a.run}.json"
    path.write_text(json.dumps(r, indent=1, default=str))
    summarize(r)
    print(f"\nreport: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
