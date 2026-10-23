"""GKE housekeeping: the deletion and cost controls that run outside any one sandbox's process.

A sandbox deletes its own pod (gke_raw), and the pod's activeDeadlineSeconds ends it at its lifetime. This module is the
layer under those, for what a dead process or a failed API call leaves behind, and for the node pools: Google bills nodes,
and a node outlives its pods until the autoscaler removes it.

    python -m sandboxes.gke_ops status                     # nodes per pool, pods per phase and run, node burn $/h, gke spend
    python -m sandboxes.gke_ops sweep [--dry-run]          # one housekeeping pass
    python -m sandboxes.gke_ops watch --every 60           # a pass a minute: run it beside any GKE workload
    python -m sandboxes.gke_ops teardown [--run R]         # delete our pods (all, or one run's), then meter
    python -m sandboxes.gke_ops teardown --pools           # ... and resize the sandbox node pools to 0, metering until they are
    python -m sandboxes.gke_ops teardown --delete-cluster --yes

A pass (`housekeep`); a step that fails is reported and the others still run:
  1. meter the node bill (gke_meter), so the ledger is current before anything reads it
  2. retry the deletes that failed in a sandbox's own process (gke_pending_deletes.jsonl)
  3. delete our pods that finished (deadline, eviction, preemption, crash), outran their deadline, stayed Pending past their
     creator's wait (a pod never scheduled never starts its deadline clock, and keeps the autoscaler adding nodes), or are
     stuck terminating on a node that died
  4. close the ledger entries of pods that no longer exist
  5. resize the sandbox node pools to 0 once none of our pods has existed for `idle_scale_down_min` and nodes are still up
     (the autoscaler normally gets there first; this is for when something keeps it from scaling down)
The janitor runs the same pass for GKE (RawSandbox.housekeep), so `python -m sandboxes.janitor --watch 300` covers it too.

Configuration: configs/gke.yaml `gke:` (or the file in SANDBOX_GKE_CONFIG):
    project, location, cluster       for gcloud (resize, delete); env GKE_PROJECT, GKE_LOCATION, GKE_CLUSTER override them
    sandbox_pools                    node pools that only sandboxes use (default ["sandbox-spot"])
    idle_scale_down_min              default 15
    orphan_pending_min               a Pending pod's age limit when it carries no ready-within annotation (default 30)
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from . import gke_meter
from .backends.gke_raw import (READY_ANNOTATION, _pending_path, delete_pod_sync, epoch, labels_of, list_managed_pods, namespace,
                               take_pending_deletes)
from .cost import cache_dir
from .lifetime import MAX_LIFETIME_MIN

STATE_FILE = "gke_ops_state.json"
LOCK_FILE = "gke_ops.lock"
FINISHED = ("Succeeded", "Failed")
OVERDUE_SLACK_S = 600.0         # running this long past its deadline: the kubelet missed it
ORPHAN_SLACK_S = 300.0          # Pending this long past its creator's wait: the creator is gone (it deletes its pod on giving up)
STUCK_DELETING_S = 120.0        # still there this long after a delete: its node is gone; drop the object (grace 0 is a force delete)
NODE_GONE_WAIT_S = 1200.0       # teardown --pools: how long to keep metering while the nodes drain


@dataclass
class OpsConfig:
    project: str = ""
    location: str = ""
    cluster: str = ""
    sandbox_pools: list = field(default_factory=lambda: ["sandbox-spot"])
    idle_scale_down_min: float = 15.0
    orphan_pending_min: float = 30.0

    @classmethod
    def load(cls, path: Optional[os.PathLike] = None) -> "OpsConfig":
        p = Path(path or os.environ.get("SANDBOX_GKE_CONFIG") or gke_meter.DEFAULT_CONFIG)
        raw = (yaml.safe_load(p.read_text()) or {}).get("gke", {}) if p.exists() else {}
        c = cls(**{k: v for k, v in (raw or {}).items() if k in cls.__dataclass_fields__})
        c.project = os.environ.get("GKE_PROJECT") or c.project
        c.location = os.environ.get("GKE_LOCATION") or c.location
        c.cluster = os.environ.get("GKE_CLUSTER") or c.cluster
        return c

    def cluster_args(self) -> list:
        if not (self.project and self.location and self.cluster):
            raise RuntimeError("gke: set gke.project, gke.location and gke.cluster in configs/gke.yaml (or GKE_PROJECT, "
                               "GKE_LOCATION, GKE_CLUSTER) for node pool and cluster operations")
        return [self.cluster, "--location", self.location, "--project", self.project]


def gcloud(*args: str, timeout: float = 1800) -> str:
    """Run gcloud; its stdout, or RuntimeError with its stderr. The one place this module shells out (tests replace it)."""
    p = subprocess.run(["gcloud", *args], capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError(f"gcloud {' '.join(args[:3])} ... failed ({p.returncode}): {p.stderr.strip()[-500:]}")
    return p.stdout


def resize_pool(cfg: OpsConfig, pool: str, nodes: int = 0, wait: bool = False) -> None:
    gcloud("container", "clusters", "resize", *cfg.cluster_args(), "--node-pool", pool, "--num-nodes", str(nodes), "--quiet",
           *([] if wait else ["--async"]))


# -- state -----------------------------------------------------------------------------------------------------------------------
def _state_path() -> Path:
    return cache_dir() / STATE_FILE


def _load_state() -> dict:
    try:
        s = json.loads(_state_path().read_text())
        return s if isinstance(s, dict) else {}
    except (OSError, ValueError):
        return {}


def _save_state(state: dict) -> None:
    p = _state_path()
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, sort_keys=True))
    os.replace(tmp, p)


# -- what to delete --------------------------------------------------------------------------------------------------------------
def why_delete(pod, now: float, cfg: OpsConfig) -> Optional[str]:
    """Why one of our pods should be deleted now, or None to leave it alone."""
    md, st = pod.metadata, pod.status
    created = epoch(md.creation_timestamp) or now
    phase = getattr(st, "phase", None) or "Pending"
    if getattr(md, "deletion_timestamp", None):
        since = epoch(md.deletion_timestamp) or now
        return f"stuck terminating for {now - since:.0f}s" if now - since > STUCK_DELETING_S else None
    if phase in FINISHED:
        return f"finished ({phase}{':' + st.reason if getattr(st, 'reason', None) else ''})"
    if phase == "Pending":
        try:
            wait = float((md.annotations or {}).get(READY_ANNOTATION)) + ORPHAN_SLACK_S
        except (TypeError, ValueError):
            wait = cfg.orphan_pending_min * 60.0
        return f"pending for {(now - created) / 60:.0f} min, past its creator's wait" if now - created > wait else None
    deadline = getattr(pod.spec, "active_deadline_seconds", None) or MAX_LIFETIME_MIN * 60
    started = epoch(getattr(st, "start_time", None)) or created
    over = now - (started + float(deadline))
    return f"{over / 60:.0f} min past its deadline" if over > OVERDUE_SLACK_S else None


def queued_deletes() -> int:
    """Deletes waiting for the next pass (read only: the queue is not taken)."""
    p = _pending_path()
    files = [p, *sorted(p.parent.glob(p.name + ".*.taken"))] if p.parent.exists() else []
    return sum(len(f.read_text().splitlines()) for f in files if f.exists())


def foreign_pods_on(api, node_names) -> list:
    """Pods on these nodes that are neither ours nor a DaemonSet's nor static: what a pool resize would kill."""
    out = []
    for n in node_names:
        for p in api.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={n}").items or []:
            md = p.metadata
            if (md.labels or {}).get("sandboxes.managed") == "true" or (getattr(p.status, "phase", None) in FINISHED):
                continue
            if any(getattr(o, "kind", "") == "DaemonSet" for o in (md.owner_references or [])):
                continue
            if "kubernetes.io/config.mirror" in (md.annotations or {}):
                continue
            out.append(f"{md.namespace}/{md.name}")
    return out


# -- one pass --------------------------------------------------------------------------------------------------------------------
def retry_pending_deletes(api) -> tuple[int, int]:
    """(deleted or already gone, still failing: queued again by delete_pod_sync)."""
    entries, files = take_pending_deletes()
    done = failed = 0
    for key in sorted({(e.get("namespace") or namespace(), e["name"]) for e in entries}):
        if delete_pod_sync(api, key[1], key[0]):
            done += 1
        else:
            failed += 1
    for f in files:
        f.unlink(missing_ok=True)
    return done, failed


def scale_down_idle(api, cfg: OpsConfig, state: dict, now: float, active: int) -> list:
    """Resize the sandbox pools that still have nodes to 0 when none of our pods has existed for idle_scale_down_min. Returns
    the pools resized."""
    if active:
        state["last_busy"] = now
        return []
    state.setdefault("last_busy", now)                       # no memory of the last pod: the idle clock starts now
    idle_s = cfg.idle_scale_down_min * 60.0
    if now - float(state["last_busy"]) < idle_s or now - float(state.get("last_resize", 0)) < idle_s:
        return []
    nodes = [n for n in gke_meter.nodes_from_api(api) if n.pool in cfg.sandbox_pools]
    if not nodes:
        return []
    blockers = foreign_pods_on(api, [n.name for n in nodes])
    if blockers:
        print(f"[sandboxes] gke_ops: sandbox pools idle but not resized, other pods are on their nodes: {blockers[:5]}",
              file=sys.stderr, flush=True)
        return []
    pools = sorted({n.pool for n in nodes})
    for pool in pools:
        resize_pool(cfg, pool, 0)
    state["last_resize"] = now
    return pools


def housekeep(api, cfg: Optional[OpsConfig] = None, *, now: Optional[float] = None, dry_run: bool = False) -> dict:
    """One pass (see the module docstring). Returns a report; {"skipped": ...} when another process is doing a pass."""
    cache_dir().mkdir(parents=True, exist_ok=True)
    with open(cache_dir() / LOCK_FILE, "a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"skipped": "another housekeeping pass is running"}
        cfg = cfg or OpsConfig.load()
        rep: dict = {"metered_usd": 0.0, "pending_deleted": 0, "pending_failed": 0, "deleted": [], "delete_failed": [],
                     "reconciled": 0, "scaled_down": [], "errors": []}

        def step(name, fn):
            try:
                fn()
            except Exception as e:  # noqa: BLE001 — one failing step must not stop the others
                rep["errors"].append(f"{name}: {type(e).__name__}: {e}"[:400])

        def meter():
            rep["metered_usd"] = gke_meter.meter_once(api, now=now)
        if not dry_run:
            step("meter", meter)

        def pending():
            rep["pending_deleted"], rep["pending_failed"] = retry_pending_deletes(api)
        if not dry_run:
            step("pending deletes", pending)

        ns = namespace()
        listed: dict = {}

        def pods():
            listed["at"] = time.time()
            listed["pods"] = list_managed_pods(api, ns)
            t = now or time.time()
            for p in listed["pods"]:
                why = why_delete(p, t, cfg)
                if not why:
                    continue
                if dry_run or delete_pod_sync(api, p.metadata.name, ns):
                    rep["deleted"].append((p.metadata.name, why))
                else:
                    rep["delete_failed"].append(p.metadata.name)
        step("pods", pods)

        def reconcile():
            from .fleet import reconcile as fleet_reconcile
            gone = {n for n, _ in rep["deleted"]}
            alive = {p.metadata.name for p in listed["pods"]} - gone
            rep["reconciled"] = fleet_reconcile(live_now={"gke": alive}, listed_at=listed["at"])
        if "pods" in listed and not dry_run:              # never close entries from a listing that failed
            step("reconcile", reconcile)

        state = _load_state()

        def scale():
            deleted = {n for n, _ in rep["deleted"]}
            active = sum(1 for p in listed["pods"] if p.metadata.name not in deleted
                         and (getattr(p.status, "phase", None) or "Pending") not in FINISHED)
            rep["scaled_down"] = [] if dry_run else scale_down_idle(api, cfg, state, now or time.time(), active)
        if "pods" in listed:                              # no listing, no idea whether pods are running: never resize
            step("scale down", scale)
            if not dry_run:
                _save_state(state)
        return rep


def format_report(rep: dict) -> str:
    if rep.get("skipped"):
        return f"gke_ops: skipped ({rep['skipped']})"
    parts = [f"metered ${rep['metered_usd']:.4f}", f"deleted {len(rep['deleted'])}"]
    if rep["pending_deleted"] or rep["pending_failed"]:
        parts.append(f"pending deletes {rep['pending_deleted']} done / {rep['pending_failed']} failing")
    if rep["delete_failed"]:
        parts.append(f"DELETE FAILED {len(rep['delete_failed'])}")
    if rep["reconciled"]:
        parts.append(f"closed {rep['reconciled']} ledger entries")
    if rep["scaled_down"]:
        parts.append(f"resized to 0: {','.join(rep['scaled_down'])}")
    lines = [f"{time.strftime('%FT%TZ', time.gmtime())} gke_ops: " + ", ".join(parts)]
    lines += [f"    deleted {n}: {why}" for n, why in rep["deleted"][:20]]
    lines += [f"    ERROR {e}" for e in rep["errors"]]
    return "\n".join(lines)


# -- status and teardown ---------------------------------------------------------------------------------------------------------
def status(api, cfg: Optional[OpsConfig] = None, mcfg: Optional[gke_meter.MeterConfig] = None) -> dict:
    """Nodes per pool with their $/h, our pods per phase and run, the cluster's burn rate, the gke spend in the ledgers."""
    from .cost import summary_all
    cfg = cfg or OpsConfig.load()
    mcfg = mcfg or gke_meter.MeterConfig.load()
    pools: dict = {}
    for n in gke_meter.nodes_from_api(api):
        p = pools.setdefault(n.pool or "?", {"nodes": 0, "vcpus": 0.0, "usd_per_hour": 0.0, "machines": Counter(), "spot": 0,
                                             "oldest_h": 0.0, "sandbox_pool": (n.pool in cfg.sandbox_pools)})
        p["nodes"] += 1
        p["vcpus"] += n.vcpus
        p["usd_per_hour"] += gke_meter.node_price_hour(n, mcfg)
        p["machines"][n.machine] += 1
        p["spot"] += int(n.spot)
        p["oldest_h"] = max(p["oldest_h"], (time.time() - n.created) / 3600.0)
    pods = list_managed_pods(api, namespace())
    burn = sum(p["usd_per_hour"] for p in pools.values()) + mcfg.cluster_fee_hour
    return {"pools": pools,
            "pods_by_phase": dict(Counter((getattr(p.status, "phase", None) or "Pending") for p in pods)),
            "pods_by_run": dict(Counter(labels_of(p).get("sandboxes.run", "") for p in pods)),
            "burn_usd_per_hour": burn,
            "pending_deletes": queued_deletes(),
            "gke_spend_usd": summary_all()["providers"].get(gke_meter.PROVIDER, {}).get("total_usd", 0.0)}


def format_status(st: dict) -> str:
    lines = [f"{'pool':<20}{'nodes':>6}{'spot':>6}{'vCPUs':>8}{'$/h':>9}{'oldest h':>10}  machines"]
    for name, p in sorted(st["pools"].items()):
        machines = ", ".join(f"{m} x{k}" for m, k in p["machines"].items())
        lines.append(f"{name + (' *' if p['sandbox_pool'] else ''):<20}{p['nodes']:>6}{p['spot']:>6}{p['vcpus']:>8.0f}"
                     f"{p['usd_per_hour']:>9.2f}{p['oldest_h']:>10.1f}  {machines}")
    lines.append(f"burn ${st['burn_usd_per_hour']:.2f}/h (nodes + cluster fee)   gke spend in the ledgers ${st['gke_spend_usd']:.2f}")
    lines.append("pods by phase: " + (", ".join(f"{k} {v}" for k, v in sorted(st["pods_by_phase"].items())) or "none"))
    if st["pods_by_run"]:
        lines.append("pods by run:   " + ", ".join(f"{k or '(none)'} {v}" for k, v in sorted(st["pods_by_run"].items())))
    if st["pending_deletes"]:
        lines.append(f"DELETES QUEUED (failed, retried by the next pass): {st['pending_deletes']}")
    lines.append("* = sandbox pool (resized to 0 when idle)")
    return "\n".join(lines)


def teardown(api, cfg: Optional[OpsConfig] = None, *, run: Optional[str] = None, pools: bool = False,
             delete_cluster: bool = False, wait: bool = True, poll_s: float = 30.0) -> dict:
    """Delete our pods (all, or `run`'s), close their ledger entries, meter. With `pools`: resize the sandbox pools to 0 and keep
    metering until their nodes are gone. With `delete_cluster`: delete the cluster, then charge what the meter last saw up to the
    end of the deletion (gke_meter.close_out)."""
    cfg = cfg or OpsConfig.load()
    ns = namespace()
    rep: dict = {"deleted": [], "delete_failed": [], "resized": [], "cluster_deleted": False, "metered_usd": 0.0}
    listed_at = time.time()
    pods = list_managed_pods(api, ns)
    mine = [p for p in pods if run is None or labels_of(p).get("sandboxes.run") == run]
    for p in mine:
        (rep["deleted"] if delete_pod_sync(api, p.metadata.name, ns) else rep["delete_failed"]).append(p.metadata.name)
    from .fleet import reconcile as fleet_reconcile
    fleet_reconcile(live_now={"gke": {p.metadata.name for p in pods} - set(rep["deleted"])}, listed_at=listed_at)
    rep["metered_usd"] += gke_meter.meter_once(api, force=True)
    if pools or delete_cluster:
        others = [p.metadata.name for p in pods if p.metadata.name not in rep["deleted"]
                  and (getattr(p.status, "phase", None) or "Pending") not in FINISHED]
        if others:
            raise RuntimeError(f"gke: {len(others)} pod(s) of ours are still running ({others[:3]}...): not resizing the pools "
                               "under them. Tear down without --run first.")
    if pools and not delete_cluster:
        for pool in cfg.sandbox_pools:
            resize_pool(cfg, pool, 0)
            rep["resized"].append(pool)
        t0 = time.time()
        while wait and time.time() - t0 < NODE_GONE_WAIT_S:
            if not [n for n in gke_meter.nodes_from_api(api) if n.pool in cfg.sandbox_pools]:
                break
            time.sleep(poll_s)
            rep["metered_usd"] += gke_meter.meter_once(api)
        rep["metered_usd"] += gke_meter.meter_once(api, force=True)
    if delete_cluster:
        gcloud("container", "clusters", "delete", *cfg.cluster_args(), "--quiet", timeout=3600)
        rep["cluster_deleted"] = True
        rep["metered_usd"] += gke_meter.close_out()
    return rep


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    s = sub.add_parser("sweep"); s.add_argument("--dry-run", action="store_true")
    w = sub.add_parser("watch"); w.add_argument("--every", type=float, default=60.0)
    t = sub.add_parser("teardown")
    t.add_argument("--run", default=None); t.add_argument("--pools", action="store_true")
    t.add_argument("--delete-cluster", action="store_true"); t.add_argument("--yes", action="store_true")
    t.add_argument("--no-wait", action="store_true")
    a = ap.parse_args(argv)
    from .backends.gke_raw import _core
    api = _core()
    if a.cmd == "status":
        print(format_status(status(api)))
        return 0
    if a.cmd == "sweep":
        rep = housekeep(api, dry_run=a.dry_run)
        print(format_report(rep))
        return 1 if rep.get("errors") or rep.get("delete_failed") else 0
    if a.cmd == "watch":
        while True:
            print(format_report(housekeep(api)), flush=True)
            time.sleep(a.every)
    if a.delete_cluster and not a.yes:
        print("teardown --delete-cluster deletes the whole cluster: add --yes", file=sys.stderr)
        return 2
    rep = teardown(api, run=a.run, pools=a.pools, delete_cluster=a.delete_cluster, wait=not a.no_wait)
    print(json.dumps(rep, indent=1))
    return 1 if rep["delete_failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
