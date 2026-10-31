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
    python -m sandboxes.gke_ops pools plan|apply [--replace] [--prune]   # make the cluster's sandbox pools match the catalog
    python -m sandboxes.gke_ops down [--no-wait]           # nothing left running or stored (system pool, images, uploads too)
    python -m sandboxes.gke_ops up                         # the system pool back; sandbox pools grow as sandboxes ask
    python -m sandboxes.gke_ops residue                    # everything in the project that bills while idle, priced per month

A pass (`housekeep`); a step that fails is reported and the others still run:
  1. meter the node bill (gke_meter), so the ledger is current before anything reads it
  2. retry the deletes that failed in a sandbox's own process (gke_pending_deletes.jsonl)
  3. delete our pods that finished (deadline, eviction, preemption, crash), outran their deadline, stayed Pending past their
     creator's wait (a pod never scheduled never starts its deadline clock, and keeps the autoscaler adding nodes), or are
     stuck terminating on a node that died
  4. close the ledger entries of pods that no longer exist
  5. resize the sandbox node pools to 0 once none of our pods has existed for `idle_scale_down_min` and nodes are still up
     (the autoscaler normally gets there first; this is for when something keeps it from scaling down)
  6. delete images no pod runs that were unused for `gke.build.image_ttl_h`, and build uploads older than an hour (gke_build)
  7. after `auto_down_min` with none of our pods: `down` (the system pool too, every image and upload). A sandbox start on a
     cluster that is down brings it back up (`auto_up`).
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
import math
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
ARTIFACT_PURGE_EVERY_S = 600.0  # unused images and stale build uploads are looked for this often
HYPERDISK_ONLY = ("c4", "c4a", "c4d", "c4n", "n4", "n4d", "x4", "h4d")


@dataclass
class OpsConfig:
    project: str = ""
    location: str = ""
    cluster: str = ""
    sandbox_pools: list = field(default_factory=lambda: ["sandbox-spot"])   # names; from `pools` when there is a catalog
    idle_scale_down_min: float = 15.0
    orphan_pending_min: float = 30.0
    pools: list = field(default_factory=list)          # the catalog (gke_place): name, machine, spot, disk_gb, max_nodes, enabled
    zones: list = field(default_factory=list)          # where sandbox pools run (spot capacity differs by zone)
    system_pool: str = "default-pool"                  # kube-system's pool: 1 node while up, 0 when down
    system_max_nodes: int = 3
    node_service_account: str = ""
    max_pods_per_node: int = 256
    auto_down_min: float = 60.0                        # idle this long (no pod of ours): `down` (0: never)
    auto_up: bool = True                               # a sandbox start on a cluster that is down brings it up
    kubelet_config: dict = field(default_factory=dict)  # every sandbox pool's kubelet (e.g. {cpuManagerPolicy: static})

    @classmethod
    def load(cls, path: Optional[os.PathLike] = None) -> "OpsConfig":
        p = Path(path or os.environ.get("SANDBOX_GKE_CONFIG") or gke_meter.DEFAULT_CONFIG)
        raw = (yaml.safe_load(p.read_text()) or {}).get("gke", {}) if p.exists() else {}
        c = cls(**{k: v for k, v in (raw or {}).items() if k in cls.__dataclass_fields__})
        c.project = os.environ.get("GKE_PROJECT") or c.project
        c.location = os.environ.get("GKE_LOCATION") or c.location
        c.cluster = os.environ.get("GKE_CLUSTER") or c.cluster
        if c.pools:
            c.pools = [p for p in c.pools if p.get("enabled", True) is not False]
            c.sandbox_pools = [p["name"] for p in c.pools]
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


BUSY = ("CLUSTER_ALREADY_HAS_OPERATION", "incompatible operation")
BUSY_WAIT_S = 900.0


def cluster_op(*args, timeout: float = 1800) -> str:
    """A gcloud call that changes the cluster. GKE runs one such operation at a time (found live: a second pool resize right
    after the first failed with CLUSTER_ALREADY_HAS_OPERATION): wait for the running one and retry, up to BUSY_WAIT_S."""
    t0, delay = time.time(), 5.0
    while True:
        try:
            return gcloud(*args, timeout=timeout)
        except RuntimeError as e:
            if not any(b in str(e) for b in BUSY) or time.time() - t0 > BUSY_WAIT_S:
                raise
            time.sleep(delay)
            delay = min(delay * 1.5, 30.0)


def resize_pool(cfg: OpsConfig, pool: str, nodes: int = 0, wait: bool = False) -> None:
    cluster_op("container", "clusters", "resize", *cfg.cluster_args(), "--node-pool", pool, "--num-nodes", str(nodes), "--quiet",
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
    p.parent.mkdir(parents=True, exist_ok=True)
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


def our_pools(api, cfg: OpsConfig) -> list:
    """Every sandbox pool that may hold nodes of ours: the catalog's, plus any pool whose nodes carry the sandbox pool label (one
    made from another catalog or worktree, one disabled or dropped since): a node bills whichever config created it."""
    names = list(cfg.sandbox_pools)
    for n in api.list_node(label_selector="sandboxes.pool").items or []:
        pool = (n.metadata.labels or {}).get("cloud.google.com/gke-nodepool")
        if pool and pool not in names and pool != cfg.system_pool:
            names.append(pool)
    return names


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
    pools = our_pools(api, cfg)
    nodes = [n for n in gke_meter.nodes_from_api(api) if n.pool in pools]
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
            listed["active"] = active
        if "pods" in listed:                              # no listing, no idea whether pods are running: never resize
            step("scale down", scale)

        def artifacts():
            from . import gke_build
            bcfg = gke_build.BuildConfig.load()
            if not bcfg.registry or now_t - float(state.get("last_artifact_purge", 0)) < ARTIFACT_PURGE_EVERY_S:
                return
            in_use = {c.image for p in listed["pods"] for c in (getattr(p.spec, "containers", None) or [])
                      if (getattr(p.status, "phase", None) or "Pending") not in FINISHED}
            r = gke_build.purge(bcfg, in_use=in_use, now=now_t)
            rep["images_deleted"] = r["deleted"] + r["gone"]
            if bcfg.bucket:
                rep["uploads_deleted"] = gke_build.purge_staging(bcfg, older_than_s=3600, now=now_t)
            state["last_artifact_purge"] = now_t
        now_t = now or time.time()
        if "pods" in listed and not dry_run:              # in-use images come from the listing: none, no purge
            step("artifacts", artifacts)

        def auto_down():
            if listed.get("active"):
                state.pop("down_at", None)
                return
            if (cfg.auto_down_min and not state.get("down_at") and cfg.cluster
                    and now_t - float(state.get("last_busy", now_t)) >= cfg.auto_down_min * 60):
                rep["auto_down"] = down(api, cfg, wait=False)
                state["down_at"] = now_t
        if "active" in listed and not dry_run:
            step("auto down", auto_down)
        if "pods" in listed and not dry_run:
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
    if rep.get("images_deleted"):
        parts.append(f"deleted {len(rep['images_deleted'])} unused image(s)")
    if rep.get("uploads_deleted"):
        parts.append(f"deleted {rep['uploads_deleted']} build upload(s)")
    if rep.get("auto_down"):
        parts.append("idle: cluster brought DOWN")
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
        names = our_pools(api, cfg)
        for pool in names:
            resize_pool(cfg, pool, 0)
            rep["resized"].append(pool)
        t0 = time.time()
        while wait and time.time() - t0 < NODE_GONE_WAIT_S:
            if not [n for n in gke_meter.nodes_from_api(api) if n.pool in names]:
                break
            time.sleep(poll_s)
            rep["metered_usd"] += gke_meter.meter_once(api)
        rep["metered_usd"] += gke_meter.meter_once(api, force=True)
    if delete_cluster:
        cluster_op("container", "clusters", "delete", *cfg.cluster_args(), "--quiet", timeout=3600)
        rep["cluster_deleted"] = True
        rep["metered_usd"] += gke_meter.close_out()
    return rep


# -- the catalog's node pools ------------------------------------------------------------------------------------------------
def cluster_pools(cfg: OpsConfig) -> dict:
    """The cluster's node pools: name -> {"machine", "spot", "max_nodes" (all zones), "disk_gb", "disk_type", "zones", "ours"}."""
    out = {}
    for n in json.loads(gcloud("container", "node-pools", "list", "--cluster", cfg.cluster, "--location", cfg.location,
                               "--project", cfg.project, "--format=json") or "[]"):
        c, a = n.get("config") or {}, n.get("autoscaling") or {}
        zones = n.get("locations") or []
        total = a.get("totalMaxNodeCount")
        out[n["name"]] = {"machine": c.get("machineType"), "spot": bool(c.get("spot")),
                          "max_nodes": int(total) if total is not None else int(a.get("maxNodeCount") or 0) * max(1, len(zones)),
                          "disk_gb": int(c.get("diskSizeGb") or 0), "disk_type": c.get("diskType"), "zones": n.get("locations") or [],
                          "kubelet": {k: v for k, v in (c.get("kubeletConfig") or {}).items() if k in ("cpuManagerPolicy",)},
                          "local_ssd": int((c.get("ephemeralStorageLocalSsdConfig") or {}).get("localSsdCount") or 0),
                          "ours": "sandboxes.pool" in (c.get("labels") or {})}
    return out


def disk_type_for(machine: str) -> str:
    return "hyperdisk-balanced" if machine.split("-")[0].lower() in HYPERDISK_ONLY else "pd-balanced"


def plan_pools(cfg: OpsConfig, current: dict) -> list:
    """What makes the cluster's sandbox pools match the catalog: [(action, name, detail)] with action create | update | replace |
    delete | ok. A pool's machine, spot-ness, disk and local SSDs cannot change in place: those are `replace` (delete + create,
    empty only)."""
    actions = []
    from .gke_place import max_nodes_of
    zones = max(1, len(cfg.zones))
    for p in cfg.pools:
        cur = current.get(p["name"])
        want_disk, want_max = int(p.get("disk_gb", 1000)), max_nodes_of(p, zones)
        if cur is None:
            actions.append(("create", p["name"], f"{p['machine']} {'spot' if p.get('spot', True) else 'on demand'} 0..{want_max} nodes"))
        elif (cur["machine"], cur["spot"], cur["disk_gb"], cur.get("local_ssd", 0)) != \
                (p["machine"], bool(p.get("spot", True)), want_disk, int(p.get("local_ssd", 0) or 0)):
            actions.append(("replace", p["name"], f"{cur['machine']}/{'spot' if cur['spot'] else 'od'}/{cur['disk_gb']}GB"
                                                  f"/{cur.get('local_ssd', 0)} SSD -> {p['machine']}/"
                                                  f"{'spot' if p.get('spot', True) else 'od'}/{want_disk}GB/{p.get('local_ssd', 0)} SSD"))
        elif cur["max_nodes"] != want_max:
            actions.append(("update", p["name"], f"max {cur['max_nodes']} -> {want_max} nodes"))
        elif any(cur.get("kubelet", {}).get(k) != v for k, v in cfg.kubelet_config.items()):
            actions.append(("kubelet", p["name"], f"{cur.get('kubelet')} -> {cfg.kubelet_config}"))
        else:
            actions.append(("ok", p["name"], ""))
    names = {p["name"] for p in cfg.pools}
    for name, cur in current.items():
        if cur["ours"] and name not in names:
            actions.append(("delete", name, "a sandbox pool no longer in the catalog"))
    return actions


def create_pool_args(cfg: OpsConfig, p: dict) -> list:
    from .gke_place import max_nodes_of
    machine = p["machine"]
    args = ["container", "node-pools", "create", p["name"], "--cluster", cfg.cluster, "--location", cfg.location,
            "--project", cfg.project, "--machine-type", machine, "--num-nodes", "0", "--enable-autoscaling", "--total-min-nodes", "0",
            "--total-max-nodes", str(max_nodes_of(p, max(1, len(cfg.zones)))), "--location-policy", "ANY",
            "--max-pods-per-node", str(cfg.max_pods_per_node), "--disk-type", p.get("disk_type") or disk_type_for(machine),
            "--disk-size", str(int(p.get("disk_gb", 1000))), "--node-labels", f"sandboxes.pool={p['name']}",
            "--node-taints", f"sandboxes.pool={p['name']}:NoSchedule", "--workload-metadata", "GKE_METADATA",
            "--enable-image-streaming"]
    zones = p.get("zones") or cfg.zones                  # a pool's own zones: where its machine type is offered
    if zones:
        args += ["--node-locations", ",".join(zones)]
    if p.get("spot", True):
        args.append("--spot")
    if p.get("boot_iops"):
        args += ["--boot-disk-provisioned-iops", str(int(p["boot_iops"]))]
    if p.get("boot_mbps"):
        args += ["--boot-disk-provisioned-throughput", str(int(p["boot_mbps"]))]
    if p.get("local_ssd"):
        # pods' writable layers, emptyDirs and the image cache on local NVMe instead of the network boot disk (field notes
        # 2026-10-30: the shared pd-balanced boot disk made solve/test runs ~2x slower than Modal)
        args += ["--ephemeral-storage-local-ssd", f"count={int(p['local_ssd'])}"]
    if cfg.node_service_account:
        args += ["--service-account", cfg.node_service_account]
    if cfg.kubelet_config:
        args += ["--system-config-from-file", system_config_file(cfg)]
    return args


def system_config_file(cfg: OpsConfig) -> str:
    """A node system config (kubelet settings) for gcloud's --system-config-from-file."""
    import tempfile
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, prefix="gke-system-config-")
    f.write(yaml.safe_dump({"kubeletConfig": dict(cfg.kubelet_config)}))
    f.close()
    return f.name


def pool_has_nodes(api, name: str) -> bool:
    return any((n.metadata.labels or {}).get("cloud.google.com/gke-nodepool") == name for n in api.list_node().items or [])


def apply_pools(api, cfg: OpsConfig, actions: list, *, replace: bool = False, prune: bool = False) -> list:
    """Carry out plan_pools' actions. A replace or delete happens only with its flag and only on a pool with no node."""
    from .gke_place import max_nodes_of
    done = []
    by_name = {p["name"]: p for p in cfg.pools}
    for action, name, detail in actions:
        if action == "create":
            cluster_op(*create_pool_args(cfg, by_name[name]), "--quiet", timeout=3600)
        elif action == "update":
            cluster_op("container", "clusters", "update", *cfg.cluster_args(), "--node-pool", name, "--enable-autoscaling",
                   "--total-min-nodes", "0", "--total-max-nodes", str(max_nodes_of(by_name[name], max(1, len(cfg.zones)))), "--quiet")
        elif action == "kubelet":
            cluster_op("container", "node-pools", "update", name, "--cluster", cfg.cluster, "--location", cfg.location,
                       "--project", cfg.project, "--system-config-from-file", system_config_file(cfg), "--quiet", timeout=3600)
        elif action in ("replace", "delete"):
            if not (replace if action == "replace" else prune):
                continue
            if pool_has_nodes(api, name):
                print(f"[sandboxes] gke_ops: not deleting pool {name}: it has nodes", file=sys.stderr, flush=True)
                continue
            cluster_op("container", "node-pools", "delete", name, "--cluster", cfg.cluster, "--location", cfg.location,
                   "--project", cfg.project, "--quiet", timeout=3600)
            if action == "replace":
                cluster_op(*create_pool_args(cfg, by_name[name]), "--quiet", timeout=3600)
        else:
            continue
        done.append((action, name))
    return done


# -- up and down -------------------------------------------------------------------------------------------------------------
def nodes_by_pool(api) -> dict:
    out: dict = {}
    for n in api.list_node().items or []:
        pool = (n.metadata.labels or {}).get("cloud.google.com/gke-nodepool", "")
        ready = any(c.type == "Ready" and c.status == "True" for c in (getattr(n.status, "conditions", None) or []))
        out.setdefault(pool, []).append(ready)
    return out


def is_up(api, cfg: OpsConfig) -> bool:
    """The cluster can run sandboxes: a Ready node in the system pool (kube-dns, the metadata server's controllers)."""
    return any(nodes_by_pool(api).get(cfg.system_pool, []))


def up(api, cfg: OpsConfig, *, wait: bool = True, poll_s: float = 10.0, timeout_s: float = 900.0) -> dict:
    """Bring the system pool back (1 node, autoscaled to system_max_nodes). Sandbox pools grow from 0 as sandboxes ask."""
    cluster_op("container", "clusters", "update", *cfg.cluster_args(), "--node-pool", cfg.system_pool, "--enable-autoscaling",
               "--min-nodes", "1", "--max-nodes", str(cfg.system_max_nodes), "--quiet")
    resize_pool(cfg, cfg.system_pool, 1, wait=False)
    t0 = time.time()
    while wait and not is_up(api, cfg):
        if time.time() - t0 > timeout_s:
            raise RuntimeError(f"gke: the system pool has no ready node after {timeout_s:.0f}s")
        time.sleep(poll_s)
    state = _load_state()
    state.pop("down_at", None)
    state["last_busy"] = time.time()
    _save_state(state)
    return {"up": True, "ready": is_up(api, cfg)}


def down(api, cfg: OpsConfig, *, wait: bool = True, poll_s: float = 30.0) -> dict:
    """Nothing left running or stored: every pod of ours deleted, every pool (system pool too) resized to 0, every image and
    build upload deleted, the meter run until the nodes are gone, then the residue audit. The control plane stays (GKE's free
    tier covers one zonal cluster's fee); `teardown --delete-cluster` removes that too."""
    from . import gke_build
    rep = teardown(api, cfg, pools=True, wait=False)
    cluster_op("container", "clusters", "update", *cfg.cluster_args(), "--node-pool", cfg.system_pool, "--no-enable-autoscaling",
               "--quiet")
    rep["system_pool_resizes"] = drain_system_pool(api, cfg, poll_s=poll_s)
    rep["resized"].append(cfg.system_pool)
    bcfg = gke_build.BuildConfig.load()
    if bcfg.registry:
        rep["builds_cancelled"] = gke_build.cancel_ongoing(bcfg)     # before the purge: a build finishing later would push
        rep["images"] = gke_build.purge(bcfg, all_images=True)
    if bcfg.bucket:
        rep["uploads_deleted"] = gke_build.purge_staging(bcfg, older_than_s=0)
    t0 = time.time()
    while wait and time.time() - t0 < NODE_GONE_WAIT_S:
        if not api.list_node().items:
            break
        time.sleep(poll_s)
        rep["metered_usd"] += gke_meter.meter_once(api)
    rep["metered_usd"] += gke_meter.meter_once(api, force=True)
    if wait:
        rep["residue"] = residue(cfg)
    return rep


def destroy(api, cfg: OpsConfig) -> dict:
    """Undo everything scripts/gke/setup.sh made that can bill: `down` (pods, nodes, builds, images, uploads), then the cluster,
    the image repository and the build bucket with whatever they still hold; then the residue audit, which should list nothing.
    Service accounts, IAM bindings, enabled APIs and the budget alert stay: none of them bills, and setup.sh reuses them."""
    from . import gke_build
    rep = down(api, cfg, wait=True)
    rep.pop("residue", None)
    rep["cluster"] = teardown(api, cfg, delete_cluster=True)
    bcfg = gke_build.BuildConfig.load()
    if bcfg.registry:
        repo, location, project = gke_build.repo_coordinates(bcfg)[1::2]
        try:
            cluster_op("artifacts", "repositories", "delete", repo, "--location", location, "--project", project, "--quiet",
                       timeout=1800)
            rep["repository_deleted"] = repo
        except RuntimeError as e:
            if "NOT_FOUND" not in str(e) and "not found" not in str(e).lower():
                raise
    if bcfg.bucket:
        try:
            gcloud("storage", "rm", "--recursive", f"gs://{bcfg.bucket}", timeout=3600)
            rep["bucket_deleted"] = bcfg.bucket
        except RuntimeError as e:
            if "not found" not in str(e).lower() and "404" not in str(e):
                raise
    rep["residue"] = residue(cfg)
    return rep


def _meter_quietly(api) -> None:
    try:
        gke_meter.meter_once(api)
    except Exception as e:  # noqa: BLE001 — a failed pass is caught up by the next one
        print(f"[sandboxes] gke_ops: meter pass failed: {e!r}", file=sys.stderr, flush=True)


def drain_system_pool(api, cfg: OpsConfig, *, tries: int = 4, settle_s: float = 60.0, poll_s: float = 30.0) -> int:
    """Resize the system pool to 0 and make it stay there. Seen live: right after autoscaling is turned off, the autoscaler can
    still hold the old minimum and replace the node the resize removed, so the resize is repeated until the pool shows no
    node for `settle_s`. Returns how many resizes it took; RuntimeError if the pool will not stay empty."""
    for attempt in range(1, tries + 1):
        # synchronous: GKE's first resize after the autoscaling change surge-replaces the node and "succeeds" with one left
        # (found live), so waiting for an empty pool after an async resize waited out the whole drain timeout. Gone nodes are
        # charged to their deletion events, so nothing is lost by not metering during the operation.
        resize_pool(cfg, cfg.system_pool, 0, wait=True)
        for _ in range(max(1, math.ceil(settle_s / poll_s))):
            if nodes_by_pool(api).get(cfg.system_pool):
                break
            time.sleep(poll_s)
            _meter_quietly(api)
        else:
            return attempt
        print(f"[sandboxes] gke_ops: a {cfg.system_pool} node came back after resize {attempt}; resizing again",
              file=sys.stderr, flush=True)
    raise RuntimeError(f"gke: {cfg.system_pool} keeps a node after {tries} resizes to 0: check its autoscaling")


def ensure_up(api, cfg: Optional[OpsConfig] = None) -> None:
    """Before a sandbox starts: a cluster that is down is brought up (auto_up) or the start fails with what to run. One process
    brings it up at a time; the others wait for it. Clusters without coordinates in the config are not managed here."""
    cfg = cfg or OpsConfig.load()
    if not cfg.cluster or is_up(api, cfg):
        return
    if not cfg.auto_up:
        raise RuntimeError("gke: the cluster is down (no ready node in the system pool): python -m sandboxes.gke_ops up")
    cache_dir().mkdir(parents=True, exist_ok=True)
    with open(cache_dir() / "gke_up.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not is_up(api, cfg):
            print("[sandboxes] gke: the cluster is down; bringing it up (gke_ops up)", file=sys.stderr, flush=True)
            up(api, cfg)


# -- what still costs money --------------------------------------------------------------------------------------------------
DISK_GB_MONTH = {"pd-standard": 0.04, "pd-balanced": 0.10, "pd-ssd": 0.17, "pd-extreme": 0.125, "hyperdisk-balanced": 0.08,
                 "hyperdisk-throughput": 0.05, "hyperdisk-extreme": 0.125}


def _gjson(*args) -> list:
    out = gcloud(*args, "--format=json")
    return json.loads(out or "[]")


def residue(cfg: OpsConfig, mcfg: Optional[gke_meter.MeterConfig] = None) -> dict:
    """Everything in the project that bills while nothing runs, ours or not, priced per month; and what could not be listed.
    {"items": [{"what", "detail", "usd_month"}], "usd_month", "unlisted": [...]}."""
    mcfg = mcfg or gke_meter.MeterConfig.load()
    P = ["--project", cfg.project]
    items, unlisted = [], []

    def section(name, fn):
        try:
            fn()
        except Exception as e:  # noqa: BLE001 — an API that is off or forbidden is reported, not fatal
            unlisted.append(f"{name}: {str(e)[:160]}")

    def clusters():
        for c in _gjson("container", "clusters", "list", *P):
            items.append({"what": "GKE cluster", "detail": f"{c['name']} ({c.get('location')}; free tier credits one zonal cluster)",
                          "usd_month": 0.10 * 730})

    def instances():
        for i in _gjson("compute", "instances", "list", *P):
            if i.get("status") not in ("RUNNING", "PROVISIONING", "STAGING"):
                continue
            mt = i.get("machineType", "").rsplit("/", 1)[-1]
            spot = (i.get("scheduling") or {}).get("provisioningModel") == "SPOT"
            kind = "spot" if spot else "ondemand"
            price = (mcfg.machine_prices.get(mt) or {}).get(kind)
            # what bills with the VM besides the machine: local SSDs (SCRATCH disks, not in the disk listing) and external IPs
            ssd_gib = sum(float(d.get("diskSizeGb") or 375) for d in i.get("disks") or [] if d.get("type") == "SCRATCH")
            ips = sum(1 for nic in i.get("networkInterfaces") or [] for ac in nic.get("accessConfigs") or [] if ac.get("natIP"))
            ssd_rate = (getattr(mcfg, "local_ssd_gib_month", None) or {"spot": 0.0478, "ondemand": 0.08})[kind]
            ip_rate = (getattr(mcfg, "node_ip_hour", None) or {"spot": 0.0025, "ondemand": 0.005})[kind]
            extra_h = ssd_gib * float(ssd_rate) / 730 + ips * float(ip_rate)
            detail = f"{i['name']} {mt}{' spot' if spot else ''}" + (f" +{ssd_gib:.0f} GiB local SSD" if ssd_gib else "") \
                + (f" +{ips} external IP" if ips else "")
            items.append({"what": "VM", "detail": detail,
                          "usd_month": ((price or 0.0) + extra_h) * 730, **({} if price else {"unpriced": True})})

    def disks():
        for d in _gjson("compute", "disks", "list", *P):
            typ = d.get("type", "").rsplit("/", 1)[-1]
            gb = float(d.get("sizeGb") or 0)
            attached = bool(d.get("users"))
            items.append({"what": "disk" if attached else "ORPHANED disk", "detail": f"{d['name']} {gb:.0f} GB {typ}",
                          "usd_month": gb * DISK_GB_MONTH.get(typ, 0.17)})

    def snapshots():
        for s_ in _gjson("compute", "snapshots", "list", *P):
            gb = float(s_.get("storageBytes") or 0) / 1e9
            items.append({"what": "snapshot", "detail": f"{s_['name']} {gb:.1f} GB", "usd_month": gb * 0.05})

    def images():
        for im in _gjson("compute", "images", "list", "--no-standard-images", *P):
            gb = float(im.get("archiveSizeBytes") or 0) / 1e9
            items.append({"what": "VM image", "detail": f"{im['name']} {gb:.1f} GB", "usd_month": gb * 0.05})

    def addresses():
        for a in _gjson("compute", "addresses", "list", *P):
            idle = a.get("status") == "RESERVED"
            items.append({"what": "IP address" + (" (unused)" if idle else ""), "detail": a["name"], "usd_month": (0.01 if idle else 0.005) * 730})

    def builds():
        from . import gke_build
        bcfg = gke_build.BuildConfig.load()
        if not bcfg.region:
            return
        for b in _gjson("builds", "list", "--ongoing", *P, "--region", bcfg.region):
            items.append({"what": "Cloud Build running", "detail": f"{b.get('id')} ({b.get('status')}; pushes an image when done)",
                          "usd_month": float(bcfg.usd_per_minute) * 60 * 730})

    def forwarding():
        for f in _gjson("compute", "forwarding-rules", "list", *P):
            items.append({"what": "load balancer rule", "detail": f["name"], "usd_month": 0.025 * 730})

    def registries():
        for r in _gjson("artifacts", "repositories", "list", *P):
            gb = float(r.get("sizeBytes") or 0) / 2 ** 30
            if gb > 0:
                items.append({"what": "registry storage", "detail": f"{r['name'].rsplit('/', 1)[-1]} {gb:.2f} GiB",
                              "usd_month": max(0.0, gb - 0.5) * 0.10})

    def buckets():
        for b in _gjson("storage", "buckets", "list", *P):
            url = b.get("storage_url") or f"gs://{b['name']}/"
            out = gcloud("storage", "du", "--summarize", url)
            size = float((out.split() or ["0"])[0])
            if size > 0:
                items.append({"what": "bucket storage", "detail": f"{url} {size / 1e9:.2f} GB", "usd_month": size / 1e9 * 0.02})

    for name, fn in (("clusters", clusters), ("VMs", instances), ("disks", disks), ("snapshots", snapshots), ("VM images", images),
                     ("addresses", addresses), ("load balancers", forwarding), ("registries", registries), ("buckets", buckets),
                     ("builds", builds)):
        section(name, fn)
    return {"items": items, "usd_month": sum(i["usd_month"] for i in items), "unlisted": unlisted}


def format_residue(r: dict) -> str:
    lines = [f"  {i['what']:<20} {i['detail']:<70} ${i['usd_month']:>8.2f}/month" + ("  (unpriced machine)" if i.get("unpriced") else "")
             for i in sorted(r["items"], key=lambda i: -i["usd_month"])]
    lines.append(f"  {'TOTAL':<20} {'':<70} ${r['usd_month']:>8.2f}/month while idle")
    lines += [f"  could not list {u}" for u in r["unlisted"]]
    return "\n".join(lines) if r["items"] or r["unlisted"] else "  nothing in the project bills while idle"


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
    pl = sub.add_parser("pools")
    pl.add_argument("action", choices=["plan", "apply"]); pl.add_argument("--replace", action="store_true")
    pl.add_argument("--prune", action="store_true")
    pl.add_argument("--only", nargs="+", default=None, help="act on these pools only (others may be in use)")
    sub.add_parser("up")
    d = sub.add_parser("down"); d.add_argument("--no-wait", action="store_true")
    sub.add_parser("residue")
    x = sub.add_parser("destroy", help="down, then delete the cluster, the image repository and the build bucket")
    x.add_argument("--yes", action="store_true")
    a = ap.parse_args(argv)
    if a.cmd == "residue":
        r = residue(OpsConfig.load())
        print(format_residue(r))
        return 1 if r["items"] else 0
    from .backends.gke_raw import _core
    api = _core()
    if a.cmd == "status":
        print(format_status(status(api)))
        return 0
    if a.cmd == "pools":
        cfg = OpsConfig.load()
        actions = [x for x in plan_pools(cfg, cluster_pools(cfg)) if not a.only or x[1] in a.only]
        for action, name, detail in actions:
            print(f"  {action:<8} {name:<24} {detail}")
        if a.action == "apply":
            print("applied:", apply_pools(api, cfg, actions, replace=a.replace, prune=a.prune))
        return 0
    if a.cmd == "up":
        print(json.dumps(up(api, OpsConfig.load())))
        return 0
    if a.cmd == "down":
        rep = down(api, OpsConfig.load(), wait=not a.no_wait)
        print(json.dumps({k: v for k, v in rep.items() if k != "residue"}, indent=1, default=str))
        if "residue" in rep:
            print("left in the project:\n" + format_residue(rep["residue"]))
        return 0
    if a.cmd == "destroy":
        if not a.yes:
            print("destroy deletes the cluster, the image repository and the build bucket: add --yes", file=sys.stderr)
            return 2
        rep = destroy(api, OpsConfig.load())
        print(json.dumps({k: v for k, v in rep.items() if k != "residue"}, indent=1, default=str))
        print("left in the project:\n" + format_residue(rep["residue"]))
        return 1 if rep["residue"]["items"] else 0
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
