"""Which node pool runs a sandbox most cheaply, and what a fleet of N sandboxes costs on each machine type.

A sandbox asks for a size (vCPUs, GiB of memory, GB of disk); a node pool is a machine type, spot or on demand, a boot disk and
a node ceiling. A sandbox's cost on a pool is the node's price (machine + boot disk) divided by how many sandboxes of that size
fit one node: GKE's allocatable (what the node keeps for itself, below) minus what the DaemonSets request, then the tightest of
CPU, memory, disk and the pod limit. So memory-heavy sandboxes land on memory-cheap machines and CPU-heavy ones on CPU-cheap
ones without either being named.

    python -m sandboxes.gke_place plan --cpu 1 --mem 4 --disk 10 --count 1000 [--max-usd-h 75]   # every machine, cheapest first
    python -m sandboxes.gke_place pools                                                         # the catalog's pools, per size

`place()` is what each GKE sandbox start calls: the cheapest pool in the catalog that has room, where room on a node already
running costs nothing (it is paid for), and a pool that just failed to get capacity (spot stockout) is skipped for a while.
Pools stay at zero nodes until a sandbox needs one; the autoscaler adds nodes, the watchdog removes idle ones (gke_ops).
"""

from __future__ import annotations

import argparse
import math
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

POOL_LABEL = "sandboxes.pool"          # node label and taint key of every sandbox pool; pods select one and tolerate them all
HOURS_PER_MONTH = 730.0
HYPERDISK_ONLY = ("c4", "c4a", "c4d", "n4", "n4d", "x4", "h4d")      # families that boot only from Hyperdisk
COOLDOWN_S = 600.0                     # a pool that could not get a node is skipped this long
EPS = 1e-6                             # room is counted in floats: 0.95 subtracted 90 times must still leave the 91st slot
LOCAL_SSD_GIB = 375.0                  # one local NVMe SSD (a pool's `local_ssd` counts these)
LOCAL_SSD_GIB_MONTH = {"spot": 0.0478, "ondemand": 0.08}   # us-central1 catalog, 2026-10-30 (gke_meter.local_ssd_gib_month)
NODE_IP_HOUR = {"spot": 0.0025, "ondemand": 0.005}         # a node's external IPv4 (gke_meter.node_ip_hour)


def _fits(r, shape: "Shape") -> bool:
    return r[0] + EPS >= shape.cpu_req and r[1] + EPS >= shape.mem_gib and r[2] + EPS >= shape.disk_req and r[3] >= 1


# -- what a node keeps for itself (GKE Standard node allocatable) ------------------------------------------------------------
def reserved_cpu(vcpus: float) -> float:
    """6% of the first core, 1% of the next, 0.5% of the next 2, 0.25% above 4."""
    v = float(vcpus)
    return 0.06 * min(v, 1) + 0.01 * min(max(v - 1, 0), 1) + 0.005 * min(max(v - 2, 0), 2) + 0.0025 * max(v - 4, 0)


def reserved_mem_gib(mem_gib: float) -> float:
    """25% of the first 4 GiB, 20% of the next 4, 10% of the next 8, 6% of the next 112, 2% above 128; plus 100 MiB eviction."""
    m = float(mem_gib)
    if m < 1:
        return 255 / 1024
    out, left = 0.0, m
    for size, frac in ((4, 0.25), (4, 0.20), (8, 0.10), (112, 0.06), (math.inf, 0.02)):
        take = min(left, size)
        out += take * frac
        left -= take
        if left <= 0:
            break
    return out + 100 / 1024


def allocatable_disk_gib(disk_gib: float) -> float:
    """Boot disk left for pods: 10% eviction threshold, and the system's own reservation (35%, at most 6 GiB)."""
    return max(0.0, float(disk_gib) * 0.9 - min(0.35 * float(disk_gib), 6.0))


def allocatable_local_ssd_gib(count: int) -> float:
    """Ephemeral storage on `count` local SSDs (a pool made with --ephemeral-storage-local-ssd: pods' writable layers, emptyDirs
    and the image cache all live there, not on the boot disk): 10% eviction threshold, and the system's reservation (50 GiB
    for one SSD, 75 for two, 100 for three or more)."""
    n = int(count)
    return max(0.0, n * LOCAL_SSD_GIB * 0.9 - (50.0 if n == 1 else 75.0 if n == 2 else 100.0))


def local_ssd_hour(count: int, spot: bool, gib_month: Optional[dict] = None) -> float:
    rate = (gib_month or LOCAL_SSD_GIB_MONTH)["spot" if spot else "ondemand"]
    return int(count) * LOCAL_SSD_GIB * float(rate) / HOURS_PER_MONTH


@dataclass
class Shape:
    """A sandbox's size: what it requests from its node."""
    cpu: float = 1.0
    mem_gib: float = 4.0
    disk_gib: float = 10.0
    cpu_request: float = 0.95                  # fraction of cpu requested (the limit is the full cpu)
    disk_request_gib: Optional[float] = None   # ephemeral storage requested (default: all of disk_gib, guaranteed)

    @property
    def cpu_req(self) -> float:
        return round(self.cpu * self.cpu_request, 3)

    @property
    def disk_req(self) -> float:
        return min(self.disk_gib, self.disk_request_gib if self.disk_request_gib is not None else self.disk_gib)


@dataclass
class Overhead:
    """What every node runs besides sandboxes (DaemonSets: networking, logging, metadata server, CSI, image streaming)."""
    cpu: float = 0.5
    mem_gib: float = 1.5
    pods: int = 12
    image_cache_gib: float = 200.0             # task images on the node's disk (image streaming keeps only what is read)


@dataclass
class Machine:
    name: str
    vcpus: float
    mem_gib: float
    spot_h: Optional[float] = None
    ondemand_h: Optional[float] = None

    @property
    def family(self) -> str:
        return self.name.split("-")[0].lower()

    def price(self, spot: bool) -> Optional[float]:
        return self.spot_h if spot else self.ondemand_h


def per_node(m: Machine, shape: Shape, disk_gib: Optional[float] = None, max_pods: int = 256, oh: Overhead = Overhead(),
             local_ssd: int = 0) -> int:
    """How many sandboxes of `shape` fit one node of `m` (with a boot disk of `disk_gib`; None: the disk is sized to fit; with
    `local_ssd` disks, pods' storage is on those instead of the boot disk)."""
    cpu = m.vcpus - reserved_cpu(m.vcpus) - oh.cpu
    mem = m.mem_gib - reserved_mem_gib(m.mem_gib) - oh.mem_gib
    fits = [int(cpu // shape.cpu_req) if shape.cpu_req else 10 ** 6, int(mem // shape.mem_gib) if shape.mem_gib else 10 ** 6,
            max_pods - oh.pods]
    if shape.disk_req and (local_ssd or disk_gib is not None):
        eph = allocatable_local_ssd_gib(local_ssd) if local_ssd else allocatable_disk_gib(disk_gib)
        fits.append(int(max(0.0, eph - oh.image_cache_gib) // shape.disk_req))
    return max(0, min(fits))


def disk_for(n: int, shape: Shape, oh: Overhead = Overhead()) -> int:
    """The boot disk (GB, rounded up to 50) that holds `n` sandboxes' disk requests and the image cache."""
    need = n * shape.disk_req + oh.image_cache_gib
    # allocatable = 0.9 d - min(0.35 d, 6): for d >= ~17 GiB that is 0.9 d - 6
    d = (need + 6.0) / 0.9
    return int(math.ceil(d / 50.0) * 50)


def disk_hour(disk_gib: float, gb_month: float = 0.10) -> float:
    return disk_gib * gb_month / HOURS_PER_MONTH


@dataclass
class Option:
    machine: str
    spot: bool
    per_node: int
    nodes: int
    disk_gib: int
    node_h: float             # one node, machine + disk
    total_h: float            # the whole fleet
    per_sandbox_h: float      # at full nodes

    def line(self) -> str:
        kind = "spot" if self.spot else "on demand"
        return (f"{self.machine:<20} {kind:<9} {self.per_node:>4}/node {self.nodes:>4} nodes  disk {self.disk_gib:>5} GB  "
                f"${self.node_h:>7.3f}/node-h  ${self.total_h:>8.2f}/h  ${self.per_sandbox_h:.5f}/sandbox-h")


def plan(shape: Shape, count: int, machines: list, *, spot: Optional[bool] = None, gb_month: float = 0.10,
         max_pods: int = 256, oh: Overhead = Overhead()) -> list:
    """Every machine (spot and/or on demand) that can run `count` sandboxes of `shape`, cheapest fleet first. Disks sized to fit."""
    out = []
    for m in machines:
        for s in ((True, False) if spot is None else (spot,)):
            price = m.price(s)
            if price is None:
                continue
            n = per_node(m, shape, None, max_pods, oh)
            if n <= 0:
                continue
            disk = disk_for(n, shape, oh)
            n = min(n, per_node(m, shape, disk, max_pods, oh))
            if n <= 0:
                continue
            node_h = price + disk_hour(disk, gb_month)
            nodes = math.ceil(count / n)
            out.append(Option(m.name, s, n, nodes, disk, node_h, nodes * node_h, node_h / n))
    return sorted(out, key=lambda o: (o.total_h, o.per_sandbox_h))


# -- the pool catalog and placement --------------------------------------------------------------------------------------------
@dataclass
class Pool:
    name: str
    machine: Machine
    spot: bool
    disk_gib: int
    max_nodes: int                             # over all its zones
    disk_type: str = ""
    local_ssd: int = 0                         # local NVMe SSDs holding the pods' storage (0: the boot disk holds it)
    local_ssd_gib_month: Optional[dict] = None

    @property
    def node_h(self) -> Optional[float]:
        p = self.machine.price(self.spot)
        if p is None:
            return None
        return (p + disk_hour(self.disk_gib) + local_ssd_hour(self.local_ssd, self.spot, self.local_ssd_gib_month)
                + NODE_IP_HOUR["spot" if self.spot else "ondemand"])

    def per_node(self, shape: Shape, max_pods: int = 256, oh: Overhead = Overhead()) -> int:
        return per_node(self.machine, shape, self.disk_gib, max_pods, oh, self.local_ssd)


@dataclass
class Snapshot:
    """What the sandbox pools hold now: nodes per pool, and per node the room left (cpu, mem GiB, disk GiB, pods)."""
    nodes: dict = field(default_factory=dict)          # pool -> number of nodes (up or coming)
    room: dict = field(default_factory=dict)           # pool -> [[cpu, mem, disk, pods], ...] per ready node
    taken_at: float = 0.0


@dataclass
class Placement:
    pool: str
    rate_h: float             # this sandbox's share of a node: what its ledger entry is charged
    per_node: int
    reason: str


def max_nodes_of(entry: dict, zones: int) -> int:
    """A catalog pool's node ceiling over all its zones: `max_nodes`, else `max_nodes_per_zone` x zones."""
    if entry.get("max_nodes") is not None:
        return int(entry["max_nodes"])
    return int(entry.get("max_nodes_per_zone", 1)) * zones


def load_catalog(path: Optional[os.PathLike] = None) -> tuple:
    """(pools, overhead, max_pods) from configs/gke.yaml (`gke.pools` + `gke_meter.machine_shapes` / `machine_prices`)."""
    from .gke_meter import DEFAULT_CONFIG
    p = Path(path or os.environ.get("SANDBOX_GKE_CONFIG") or DEFAULT_CONFIG)
    raw = (yaml.safe_load(p.read_text()) or {}) if p.exists() else {}
    g, meter = raw.get("gke") or {}, raw.get("gke_meter") or {}
    shapes, prices = meter.get("machine_shapes") or {}, meter.get("machine_prices") or {}
    zones = max(1, len(g.get("zones") or [1]))
    pools = []
    for e in g.get("pools") or []:
        if e.get("enabled", True) is False:
            continue
        mt = e["machine"]
        if mt not in shapes:
            print(f"[sandboxes] gke_place: pool {e['name']}: no shape for {mt} in gke_meter.machine_shapes "
                  "(scripts/gke/prices.py --pools --write)", file=sys.stderr, flush=True)
            continue
        pr = prices.get(mt) or {}
        m = Machine(mt, float(shapes[mt]["vcpus"]), float(shapes[mt]["mem_gib"]), pr.get("spot"), pr.get("ondemand"))
        pools.append(Pool(e["name"], m, bool(e.get("spot", True)), int(e.get("disk_gb", 1000)), max_nodes_of(e, zones),
                          e.get("disk_type", ""), int(e.get("local_ssd", 0) or 0), meter.get("local_ssd_gib_month")))
    oh = Overhead(**(g.get("node_overhead") or {}))
    return pools, oh, int(g.get("max_pods_per_node", 256))


def place(shape: Shape, pools: list, snap: Optional[Snapshot] = None, *, oh: Overhead = Overhead(), max_pods: int = 256,
          spot: str = "prefer", only: Optional[str] = None, cooldown: Optional[dict] = None, now: Optional[float] = None,
          exclude=()) -> Placement:
    """The pool for one sandbox. First choice: a node already up with room (no extra cost), the cheapest such pool. Else the
    pool whose new node runs this size most cheaply and that may still grow. `spot`: "prefer" | "only" | "never". Raises
    LookupError when no pool can take it."""
    snap, cooldown, now = snap or Snapshot(), cooldown or {}, now or time.time()
    cands = []
    for p in pools:
        if (only and p.name != only) or p.name in exclude:
            continue
        if (spot == "only" and not p.spot) or (spot == "never" and p.spot) or p.node_h is None:
            continue
        n = p.per_node(shape, max_pods, oh)
        if n <= 0:
            continue
        cands.append((p, n, p.node_h / n))
    if not cands:
        raise LookupError(f"no sandbox pool fits {shape} (spot={spot}{', pool=' + only if only else ''})")

    warm = [(p, n, rate) for p, n, rate in cands if any(_fits(r, shape) for r in snap.room.get(p.name, []))]
    if warm:
        p, n, rate = min(warm, key=lambda c: c[2])
        return Placement(p.name, rate, n, "room on a running node")
    cold = [(p, n, rate) for p, n, rate in cands
            if snap.nodes.get(p.name, 0) < p.max_nodes and now >= cooldown.get(p.name, 0.0)]
    if not cold:
        cold = [(p, n, rate) for p, n, rate in cands if snap.nodes.get(p.name, 0) < p.max_nodes]   # all cooling: try anyway
    if not cold:
        raise LookupError(f"every sandbox pool that fits {shape} is at its node ceiling")
    if spot == "prefer" and any(c[0].spot for c in cold):
        cold = [c for c in cold if c[0].spot]                  # on-demand pools are the fallback when no spot pool can grow
    p, n, rate = min(cold, key=lambda c: c[2])
    return Placement(p.name, rate, n, "new node")


def reserve(snap: Snapshot, pool: str, shape: Shape, per: int) -> None:
    """Count a placement into the snapshot (until the next refresh): its room on a node, or a node coming up for it."""
    rooms = snap.room.setdefault(pool, [])
    for r in rooms:
        if _fits(r, shape):
            r[0] -= shape.cpu_req; r[1] -= shape.mem_gib; r[2] -= shape.disk_req; r[3] -= 1
            return
    snap.nodes[pool] = snap.nodes.get(pool, 0) + 1          # a new node, with room for per-1 more of this size
    rooms.append([shape.cpu_req * (per - 1), shape.mem_gib * (per - 1), shape.disk_req * (per - 1), per - 1])


# -- the cluster as the placer sees it ---------------------------------------------------------------------------------------
def _qty(s, kind: str) -> float:
    """A Kubernetes quantity in vCPUs ("cpu") or GiB ("mem"/"disk")."""
    s = str(s or "0")
    units = {"Ki": 2 ** 10, "Mi": 2 ** 20, "Gi": 2 ** 30, "Ti": 2 ** 40, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12}
    if kind == "cpu":
        return float(s[:-1]) / 1000 if s.endswith("m") else float(s)
    for u, f in units.items():
        if s.endswith(u):
            return float(s[: -len(u)]) * f / 2 ** 30
    return float(s) / 2 ** 30


def snapshot_from_api(api, pool_names) -> Snapshot:
    """Nodes of the sandbox pools and the room left on each (allocatable minus every pod's requests on it)."""
    snap = Snapshot(taken_at=time.time())
    nodes = {}
    for n in api.list_node(label_selector=POOL_LABEL).items or []:
        pool = (n.metadata.labels or {}).get(POOL_LABEL)
        if pool not in pool_names:
            continue
        snap.nodes[pool] = snap.nodes.get(pool, 0) + 1
        a = getattr(n.status, "allocatable", None) or {}
        ready = any(c.type == "Ready" and c.status == "True" for c in (getattr(n.status, "conditions", None) or []))
        unsched = bool(getattr(getattr(n, "spec", None), "unschedulable", False))
        if ready and not unsched:
            nodes[n.metadata.name] = [pool, [_qty(a.get("cpu"), "cpu"), _qty(a.get("memory"), "mem"),
                                             _qty(a.get("ephemeral-storage"), "disk"), int(a.get("pods", 0) or 0)]]
    for p in api.list_pod_for_all_namespaces().items or []:
        node = getattr(p.spec, "node_name", None)
        if node not in nodes or getattr(p.status, "phase", None) in ("Succeeded", "Failed"):
            continue
        r = nodes[node][1]
        r[3] -= 1
        for c in getattr(p.spec, "containers", None) or []:
            req = (getattr(getattr(c, "resources", None), "requests", None) or {})
            r[0] -= _qty(req.get("cpu"), "cpu"); r[1] -= _qty(req.get("memory"), "mem"); r[2] -= _qty(req.get("ephemeral-storage"), "disk")
    for pool, r in nodes.values():
        snap.room.setdefault(pool, []).append(r)
    return snap


class Placer:
    """The process-wide placer: one cluster snapshot shared by every start (refreshed every `refresh_s`, with this process's
    placements counted in between), and pools on cooldown after a capacity failure."""

    def __init__(self, refresh_s: float = 15.0):
        self.refresh_s = refresh_s
        self.lock = threading.Lock()
        self.snap: Optional[Snapshot] = None
        self.cooldown: dict = {}

    def choose(self, api, shape: Shape, pools: list, **kw) -> Placement:
        with self.lock:
            if self.snap is None or time.time() - self.snap.taken_at > self.refresh_s:
                try:
                    self.snap = snapshot_from_api(api, {p.name for p in pools})
                except Exception as e:  # noqa: BLE001 — no snapshot: place by price alone
                    print(f"[sandboxes] gke_place: cannot read the cluster ({e!r}); placing by price only", file=sys.stderr, flush=True)
                    self.snap = Snapshot(taken_at=time.time())
            pl = place(shape, pools, self.snap, cooldown=self.cooldown, **kw)
            reserve(self.snap, pl.pool, shape, pl.per_node)
            return pl

    def failed(self, pool: str) -> None:
        """A pool could not get a node (spot stockout, quota): skip it for COOLDOWN_S, and forget the snapshot."""
        with self.lock:
            self.cooldown[pool] = time.time() + COOLDOWN_S
            self.snap = None


PLACER = Placer()


def machines_from_config(path: Optional[os.PathLike] = None) -> list:
    from .gke_meter import DEFAULT_CONFIG
    p = Path(path or os.environ.get("SANDBOX_GKE_CONFIG") or DEFAULT_CONFIG)
    meter = ((yaml.safe_load(p.read_text()) or {}) if p.exists() else {}).get("gke_meter") or {}
    shapes, prices = meter.get("machine_shapes") or {}, meter.get("machine_prices") or {}
    return [Machine(k, float(v["vcpus"]), float(v["mem_gib"]), (prices.get(k) or {}).get("spot"), (prices.get(k) or {}).get("ondemand"))
            for k, v in shapes.items()]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("plan", "pools"):
        s = sub.add_parser(name)
        s.add_argument("--cpu", type=float, default=1.0); s.add_argument("--mem", type=float, default=4.0)
        s.add_argument("--disk", type=float, default=10.0); s.add_argument("--disk-request", type=float, default=None)
    sub.choices["plan"].add_argument("--count", type=int, default=1000)
    sub.choices["plan"].add_argument("--max-usd-h", type=float, default=None)
    sub.choices["plan"].add_argument("--spot", choices=["any", "only", "never"], default="any")
    a = ap.parse_args(argv)
    shape = Shape(a.cpu, a.mem, a.disk, disk_request_gib=a.disk_request)
    if a.cmd == "plan":
        opts = plan(shape, a.count, machines_from_config(), spot={"any": None, "only": True, "never": False}[a.spot])
        print(f"{a.count} sandboxes of {a.cpu:g} vCPU / {a.mem:g} GiB / {a.disk:g} GB, cheapest first"
              + (f" (budget ${a.max_usd_h:g}/h)" if a.max_usd_h else ""))
        for o in opts:
            print(("  " if a.max_usd_h is None or o.total_h <= a.max_usd_h else "x ") + o.line())
        return 0 if opts and (a.max_usd_h is None or opts[0].total_h <= a.max_usd_h) else 1
    pools, oh, max_pods = load_catalog()
    for p in sorted(pools, key=lambda p: (p.node_h or 1e9) / max(1, p.per_node(shape, max_pods, oh))):
        n = p.per_node(shape, max_pods, oh)
        rate = (p.node_h / n) if n and p.node_h else None
        print(f"{p.name:<24} {p.machine.name:<18} {'spot' if p.spot else 'on demand':<9} {n:>4}/node  up to {p.max_nodes} nodes  "
              + (f"${rate:.5f}/sandbox-h" if rate else "does not fit"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
