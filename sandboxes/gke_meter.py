"""The GKE node bill, in the cost ledger.

Google bills NODES (and the cluster), not pods: a node billed for an hour costs the same whether it ran 160 sandboxes or none.
A pod's ledger entry carries its share of a node (CostModel "gke": vcpu_hour per requested vCPU); this meter charges the rest,
so for every node and every interval

    open pod entries on the node  +  meter charges for the node  =  node price x time

and it adds the cluster's own costs (management fee). What it charges for a node is split across the runs that had pods on it,
in proportion to their share; an empty node is charged to no run ("" = idle capacity). One pass:

  - a node alive now is charged from where the last pass left it, or from its creation time if it is new or the meter was not
    running: a node that is still alive is never under-charged, however long the gap
  - a node gone since the last pass is charged up to now when passes were close together (at most one interval too much),
    else up to its removal event (Kubernetes keeps events about an hour), else up to (last charged + STALE_GAP_S) with a
    `meter_gap` record that the billing reconciliation must explain
  - a node born and gone between passes (a spot node preempted within minutes) is charged from its registration event to
    its last removal event; a node remembered as gone (GONE_MEMORY_S) is never charged again
  - the cluster fee is charged for the whole time since the last pass (from the cluster's creation on the first pass)

State (how far each node and the cluster are charged) is ~/.cache/sandboxes/gke_meter_state.json, read and written under an
exclusive lock: any number of processes can call meter_once; one does the pass, the others skip, so nothing is charged twice.
Every pass writes a record for every node (usd 0 when its pods' entries cover it) carrying how far it is charged and its
price, so a lost or corrupt state file is rebuilt from the ledgers instead of charging every node again from its creation.

Prices: configs/gke.yaml `gke_meter:` (or the file in SANDBOX_GKE_CONFIG). Spot prices move up to once a day; check the ledger
against Cloud Billing with scripts/verify_costs_gke.py.
"""

from __future__ import annotations

import fcntl
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from .cost import CostLedger, all_ledger_paths, cache_dir, get_ledger
from .pricing import cost_model_for

PROVIDER = "gke"
STATE_FILE = "gke_meter_state.json"
LOCK_FILE = "gke_meter.lock"
MIN_INTERVAL_S = 30.0          # a pass sooner than this after the previous one is skipped
STATE_VERSION = 1
REREGISTER_MARGIN_S = 0.0        # a registration before the node's creation is an earlier life (both are API-server times, and
                                 # registration follows creation; a 5 s margin hid a life re-registered within it: an under-charge)
STALE_GAP_S = 300.0            # passes further apart than this mean the meter was down
GONE_MEMORY_S = 3 * 3600.0     # how long a charged-as-gone node is remembered: longer than Kubernetes keeps its events (1 h)
REGISTERED_REASONS = ("RegisteredNode",)
REMOVED_REASONS = ("RemovingNode", "DeletingNode", "NodeNotReady")
DELETED_REASONS = ("RemovingNode", "DeletingNode")   # emitted once the VM is gone: a safe end for a node's charge
HOURS_PER_MONTH = 730.0
DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "configs" / "gke.yaml"
SPOT_LABELS = ("cloud.google.com/gke-spot", "cloud.google.com/gke-preemptible")
MACHINE_LABEL = "node.kubernetes.io/instance-type"
POOL_LABEL = "cloud.google.com/gke-nodepool"


@dataclass
class MeterConfig:
    machine_prices: dict = field(default_factory=dict)      # machine type -> {"spot": USD/h, "ondemand": USD/h}
    fallback_vcpu_hour: dict = field(default_factory=lambda: {"spot": 0.02, "ondemand": 0.05})
    disk_gb_month: float = 0.10                            # boot disk, USD per GB-month
    pool_disk_gb: dict = field(default_factory=dict)       # node pool -> boot disk GB
    default_disk_gb: float = 100.0
    pool_local_ssd: dict = field(default_factory=dict)     # node pool -> local SSDs per node (the catalog's `local_ssd`)
    local_ssd_gib_month: dict = field(default_factory=lambda: {"spot": 0.0478, "ondemand": 0.08})
    node_ip_hour: dict = field(default_factory=lambda: {"spot": 0.0025, "ondemand": 0.005})   # each node's external IPv4
    cluster_fee_hour: float = 0.10

    @classmethod
    def load(cls, path: Optional[os.PathLike] = None) -> "MeterConfig":
        p = Path(path or os.environ.get("SANDBOX_GKE_CONFIG") or DEFAULT_CONFIG)
        whole = (yaml.safe_load(p.read_text()) or {}) if p.exists() else {}
        raw = whole.get("gke_meter") or {}
        known = set(cls.__dataclass_fields__)
        c = cls(**{k: v for k, v in raw.items() if k in known})
        for pool in (whole.get("gke") or {}).get("pools") or []:          # the catalog's boot disks, unless set here
            if pool.get("name") and pool.get("disk_gb"):
                c.pool_disk_gb.setdefault(pool["name"], float(pool["disk_gb"]))
            if pool.get("name") and pool.get("local_ssd"):
                c.pool_local_ssd.setdefault(pool["name"], int(pool["local_ssd"]))
        return c


@dataclass
class Node:
    name: str
    created: float
    machine: str
    spot: bool
    pool: str
    vcpus: float


@dataclass
class OpenPod:
    """A pod on a node whose ledger entry is open: its share is already charged by that entry."""
    sid: str
    node: str
    opened: float
    cpu: float
    ram_gib: float
    disk_gib: float
    run: str
    usd_per_hour: Optional[float] = None   # recorded when its entry opened: what the entry is actually charged at
    closed: Optional[float] = None         # its entry closed at (since the last pass): it covered its node up to then


def node_price_hour(node: Node, cfg: MeterConfig) -> float:
    """What the node costs per hour: its machine type at spot or on-demand price, plus its boot disk, local SSDs and external
    IP."""
    kind = "spot" if node.spot else "ondemand"
    price = (cfg.machine_prices.get(node.machine) or {}).get(kind)
    if price is None:
        price = node.vcpus * float(cfg.fallback_vcpu_hour.get(kind, 0.05))
        print(f"[sandboxes] gke_meter: no {kind} price for {node.machine!r}; using {node.vcpus:g} vCPU x fallback "
              f"= ${price:.4f}/h (add it to the gke_meter config)", file=sys.stderr, flush=True)
    disk = float(cfg.pool_disk_gb.get(node.pool, cfg.default_disk_gb)) * cfg.disk_gb_month / HOURS_PER_MONTH
    from .gke_place import local_ssd_hour
    return (float(price) + disk + local_ssd_hour(cfg.pool_local_ssd.get(node.pool, 0), node.spot, cfg.local_ssd_gib_month)
            + float(cfg.node_ip_hour.get(kind, 0.0)))


def ephemeral_price_hour(pool: str, prev_nodes: dict, nodes: list, cfg: MeterConfig, pool_prices: Optional[dict] = None) -> float:
    """Price of a node seen only in events: what nodes of its pool cost (now, at the last pass, or when one was last seen; a
    node pool has one machine type), else the most expensive price we know (an over-charge is safe, an under-charge is not)."""
    for n in nodes:
        if n.pool == pool:
            return node_price_hour(n, cfg)
    for s in prev_nodes.values():
        if s.get("pool") == pool and s.get("price_h"):
            return float(s["price_h"])
    if (pool_prices or {}).get(pool):
        return float(pool_prices[pool])
    known = [float(s.get("price_h") or 0) for s in prev_nodes.values()] + [node_price_hour(n, cfg) for n in nodes]
    known += [max(p.values()) for p in cfg.machine_prices.values() if p]
    return max(known) if known else 0.0


def compute_charges(now: float, state: dict, nodes: list, pods: list, cfg: MeterConfig, model=None,
                    cluster_created: Optional[float] = None, ephemeral: tuple = ()) -> tuple[list, dict]:
    """One meter pass, as pure arithmetic. Returns (charges, new_state); each charge is
    {"usd", "kind", "label", "run", "seconds"}: kind "node" (a node's unattributed cost, split by run), "cluster_fee", or
    "meter_gap" (usd 0; `max_usd` says how much more the vanished node might have cost).
    `ephemeral`: nodes known only from their lifecycle events ({"name", "pool", "born", "reborn", "died"}; born/reborn are its
    first/latest registration, died its latest removal, each None when there is no such event): a node born and gone between two
    passes is never seen alive, so it is charged from these (registration to removal, else to now). A name can come back (a
    managed instance group recreates a preempted VM under its old name): a node charged as gone is charged again only for a
    registration after that, never for the time already charged."""
    model = model or cost_model_for(PROVIDER)
    prev_nodes = dict(state.get("nodes") or {})
    gone_before = dict(state.get("gone") or {})
    last_pass = state.get("last_pass")
    charges, new_nodes = [], {}
    gone_now: dict = {}
    by_node: dict = {}
    events = {e["name"]: e for e in ephemeral}
    for p in pods:
        by_node.setdefault(p.node, []).append(p)
    for n in nodes:
        price = node_price_hour(n, cfg)
        # a known node from where the last pass left it, even if it is a new node under the old name (a recreated VM: the gap
        # between its lives is charged too, at most one interval); a new one from its creation, never over time charged before
        start = float(prev_nodes[n.name].get("until", n.created)) if n.name in prev_nodes else n.created
        start = max(start, float(gone_before.get(n.name, 0.0)))
        e = events.get(n.name)
        if e and n.name not in prev_nodes:
            # Registered before this node was created: earlier lives under this name. Repeats of an event fold into one (first and
            # last timestamps only), so how many and when is unknown: charge from where this name is accounted for (the last
            # pass: alive then, it would be in prev_nodes; or when it was charged as gone), else from its first registration.
            known = [float(x) for x in (last_pass, gone_before.get(n.name)) if x is not None]
            floor = max(known) if known else None
            first = float(e["born"]) if e.get("born") is not None else None
            if first is not None and first < n.created - REREGISTER_MARGIN_S:
                start = min(start, first if floor is None else max(floor, first))
            elif floor is None and e.get("died") is not None and float(e["died"]) < n.created:
                charges.append({"usd": 0.0, "kind": "meter_gap", "label": n.name, "run": "", "seconds": 0.0,
                                "died": float(e["died"])})            # an earlier life of unknown length: for reconciliation
        seconds = max(0.0, now - start)
        node_usd = price * seconds / 3600.0
        allocated_by_run: dict = {}
        for p in by_node.get(n.name, []):
            overlap = max(0.0, (min(now, p.closed) if p.closed is not None else now) - max(start, p.opened))
            share = (p.usd_per_hour * overlap / 3600.0 if p.usd_per_hour is not None
                     else model.sandbox_usd(p.cpu, p.ram_gib, p.disk_gib, overlap))
            allocated_by_run[p.run] = allocated_by_run.get(p.run, 0.0) + share
        allocated = sum(allocated_by_run.values())
        rest = max(0.0, node_usd - allocated)
        if rest > 0 and allocated > 0:
            for run, share in allocated_by_run.items():              # the node's unused part goes to the runs that held it
                charges.append({"usd": rest * share / allocated, "kind": "node", "label": n.name, "run": run, "seconds": seconds,
                                "price_h": price})
        else:                                                        # also when rest is 0: the record says how far it is charged
            charges.append({"usd": rest, "kind": "node", "label": n.name, "run": "", "seconds": seconds, "price_h": price})
        new_nodes[n.name] = {"until": now, "price_h": price, "pool": n.pool, "machine": n.machine, "spot": n.spot}
    alive = {n.name for n in nodes}
    gap = (now - last_pass) if last_pass else None
    died_at = {e["name"]: float(e["died"]) for e in ephemeral if e.get("died")}
    removed_at = {e["name"]: float(e["removed"]) for e in ephemeral if e.get("removed")}
    for name, s in prev_nodes.items():
        if name in alive:
            continue
        until = float(s.get("until", now))
        price = float(s.get("price_h", 0.0))
        if removed_at.get(name, 0.0) >= until:                        # its deletion event says when it stopped billing (found
            seconds = max(0.0, min(removed_at[name], now) - until)    # live: "up to now" charged a deleted node 5 more minutes)
        elif gap is not None and gap <= STALE_GAP_S:
            seconds = max(0.0, now - until)
        elif died_at.get(name, 0.0) >= until:                         # the meter was down, but the node's removal event says when
            seconds = max(0.0, min(died_at[name], now) - until)
        else:
            seconds = max(0.0, min(now, until + STALE_GAP_S) - until)
            unknown = max(0.0, now - until - seconds)
            if unknown > 0:
                charges.append({"usd": 0.0, "kind": "meter_gap", "label": name, "run": "", "seconds": unknown,
                                "max_usd": price * unknown / 3600.0})
        charges.append({"usd": price * seconds / 3600.0, "kind": "node", "label": name, "run": "", "seconds": seconds,
                        "price_h": price, "gone": True})
        gone_now[name] = now
    for e in ephemeral:
        name = e["name"]
        if name in alive or name in prev_nodes:
            continue                                                  # charged above
        charged_to = gone_now.get(name, gone_before.get(name))
        if charged_to is not None:                                    # charged as gone up to then, and every pass since saw
            floor = max(float(charged_to), float(last_pass or 0.0))   # its events: only a registration after both is a new life,
            if e.get("reborn") is None or float(e["reborn"]) <= floor:  # charged from the first registration after that
                continue                                              # (folded: unknown, so from `floor` at the earliest)
            born = max(floor, float(e["born"]))
        elif e.get("born") is None:                                   # a node never seen, registered before the events kept: its
            charges.append({"usd": 0.0, "kind": "meter_gap", "label": name, "run": "", "seconds": 0.0,   # cost is unknown here
                             "died": e.get("died")})
            gone_now[name] = now
            continue
        else:
            born = e["born"]
        born = float(born)
        died = float(e["died"]) if e.get("died") and float(e["died"]) >= born else now   # no removal since: up to now
        seconds = max(0.0, min(died, now) - born)
        price = ephemeral_price_hour(e.get("pool", ""), prev_nodes, nodes, cfg, state.get("pool_prices"))
        charges.append({"usd": price * seconds / 3600.0, "kind": "node", "label": name, "run": "", "seconds": seconds,
                        "price_h": price, "gone": True})
        gone_now[name] = now
    fee_from = state.get("cluster_until") or cluster_created
    if fee_from is not None and state.get("cluster_closed"):          # close_out charged a deleted cluster up to then
        fee_from = max(float(fee_from), float(state["cluster_closed"]))
    if fee_from is not None and cfg.cluster_fee_hour:
        seconds = max(0.0, now - float(fee_from))
        if seconds > 0:
            charges.append({"usd": cfg.cluster_fee_hour * seconds / 3600.0, "kind": "cluster_fee", "label": "cluster", "run": "",
                            "seconds": seconds})
    keep_gone = {n: t for n, t in {**gone_before, **gone_now}.items() if now - float(t) < GONE_MEMORY_S}
    pool_prices = dict(state.get("pool_prices") or {})
    for known in (prev_nodes, new_nodes):                            # the nodes just gone, then the nodes alive
        pool_prices.update({s["pool"]: s["price_h"] for s in known.values() if s.get("pool") and s.get("price_h")})
    new_state = {"version": STATE_VERSION, "last_pass": now, "nodes": new_nodes, "gone": keep_gone, "pool_prices": pool_prices,
                 "pod_nodes": {p.sid: p.node for p in pods if p.closed is None},
                 "cluster_until": now if fee_from is not None else state.get("cluster_until")}
    if state.get("cluster_closed"):
        new_state["cluster_closed"] = state["cluster_closed"]
    return charges, new_state


# -- reading the cluster and the ledgers -----------------------------------------------------------------------------------------
def nodes_from_api(api) -> list:
    from .backends.gke_raw import epoch
    out = []
    for n in api.list_node().items or []:
        labels = n.metadata.labels or {}
        cap = (getattr(n.status, "capacity", None) or {}) if n.status else {}
        try:
            vcpus = float(str(cap.get("cpu", "0")).rstrip("m")) / (1000.0 if str(cap.get("cpu", "")).endswith("m") else 1.0)
        except ValueError:
            vcpus = 0.0
        out.append(Node(n.metadata.name, epoch(n.metadata.creation_timestamp) or time.time(), labels.get(MACHINE_LABEL, "unknown"),
                        any(labels.get(k) == "true" for k in SPOT_LABELS), labels.get(POOL_LABEL, ""), vcpus))
    return out


def ledger_entries(paths=None, closed_since: Optional[float] = None) -> tuple[dict, dict]:
    """(sid -> open entry, sid -> (entry, close time) for entries closed after `closed_since`) of this provider, every ledger."""
    from .cost import _replay
    opens, closed = {}, {}
    for p in paths if paths is not None else all_ledger_paths():
        o, c, _ = _replay(Path(p), None)                  # incremental: only lines appended since the last pass are parsed
        opens.update({h: r for h, r in o.items() if r.get("provider") == PROVIDER and r.get("sid")})
        closed.update({h: float(r.get("t") or 0.0) for h, r in c.items()})
    still = {r["sid"]: r for h, r in opens.items() if h not in closed}
    recent = {r["sid"]: (r, closed[h]) for h, r in opens.items()
              if h in closed and closed_since is not None and closed[h] > float(closed_since)}
    return still, recent


def open_entries(paths=None) -> dict:
    """sid -> the open ledger entry (no close yet) of every sandbox of this provider, across every ledger."""
    return ledger_entries(paths)[0]


def closed_pods(recent: dict, pod_nodes: dict) -> list:
    """Entries closed since the last pass whose node that pass saw: each covered its node up to its close."""
    out = []
    for sid, (e, t) in recent.items():
        if sid in pod_nodes:
            out.append(OpenPod(sid, pod_nodes[sid], float(e["t"]), float(e.get("cpu") or 0), float(e.get("ram_gib") or 0),
                               float(e.get("disk_gib") or 0), e.get("run", ""),
                               float(e["usd_per_hour"]) if e.get("usd_per_hour") is not None else None, closed=t))
    return out


def open_pods_from_api(api, entries: dict) -> list:
    """Pods running on a node that have an open ledger entry (their share is being charged by that entry)."""
    from .backends.gke_raw import list_managed_pods, namespace
    out = []
    for p in list_managed_pods(api, namespace()):
        e = entries.get(p.metadata.name)
        node = getattr(p.spec, "node_name", None)
        if e is None or not node:
            continue
        out.append(OpenPod(p.metadata.name, node, float(e["t"]), float(e.get("cpu") or 0), float(e.get("ram_gib") or 0),
                           float(e.get("disk_gib") or 0), e.get("run", ""),
                           float(e["usd_per_hour"]) if e.get("usd_per_hour") is not None else None))
    return out


def pool_of(node_name: str, known_pools) -> str:
    """The node pool in a GKE node name (gke-<cluster>-<pool>-<hash>-<id>), among the pools we know."""
    for pool in sorted(known_pools, key=len, reverse=True):
        if pool and f"-{pool}-" in node_name:
            return pool
    return ""


def ephemeral_from_api(api, known_pools=()) -> list:
    """Nodes in the lifecycle events Kubernetes still keeps (about an hour): first registration and LAST removal-type event
    (a NotReady node may come back; the later the death, the safer the charge)."""
    from .backends.gke_raw import epoch
    try:
        events = api.list_event_for_all_namespaces(field_selector="involvedObject.kind=Node").items or []
    except Exception as e:  # noqa: BLE001 — without events only short-lived nodes can be missed; billing reconciliation catches them
        print(f"[sandboxes] gke_meter: cannot read node events ({e!r}); nodes living less than one pass may be missed",
              file=sys.stderr, flush=True)
        return []
    born, reborn, died, removed = {}, {}, {}, {}
    for ev in events:
        name = getattr(getattr(ev, "involved_object", None), "name", None)
        if not name:
            continue
        t_first = epoch(getattr(ev, "event_time", None)) or epoch(getattr(ev, "first_timestamp", None)) \
            or epoch(getattr(getattr(ev, "metadata", None), "creation_timestamp", None))
        t_last = epoch(getattr(ev, "last_timestamp", None)) or t_first
        if t_first is None:
            continue
        if ev.reason in REGISTERED_REASONS:                         # repeats fold into one event: first and last timestamps
            born[name] = min(born.get(name, t_first), t_first)
            reborn[name] = max(reborn.get(name, t_last), t_last)
        elif ev.reason in REMOVED_REASONS:
            died[name] = max(died.get(name, t_last), t_last)
            if ev.reason in DELETED_REASONS:
                removed[name] = max(removed.get(name, t_last), t_last)
    return [{"name": n, "born": born.get(n), "reborn": reborn.get(n), "died": died.get(n), "removed": removed.get(n),
             "pool": pool_of(n, known_pools)} for n in {*born, *died}]


def cluster_created_from_api(api) -> Optional[float]:
    from .backends.gke_raw import epoch
    try:
        return epoch(api.read_namespace("kube-system").metadata.creation_timestamp)
    except Exception:  # noqa: BLE001
        return None


# -- one pass, with state and lock ------------------------------------------------------------------------------------------------
def state_path() -> Path:
    return cache_dir() / STATE_FILE


def load_state(paths=None) -> dict:
    """The meter's state; a missing or corrupt file is rebuilt from the meter's own ledger records."""
    try:
        s = json.loads(state_path().read_text())
        if isinstance(s, dict) and isinstance(s.get("nodes", {}), dict) and s.get("version") == STATE_VERSION:
            return s
    except (OSError, ValueError):
        pass
    return rebuild_state(paths)


def rebuild_state(paths=None) -> dict:
    """State from the ledgers: each node's last `node` record says how far it was charged and at what price; a node whose last
    record marks it gone is not carried over. Empty when the meter never ran."""
    nodes, cluster_until, last = {}, None, None
    for p in paths if paths is not None else all_ledger_paths():
        try:
            with open(p) as f:
                for line in f:
                    try:
                        r = json.loads(line)
                    except ValueError:
                        continue
                    if r.get("ev") != "storage" or r.get("provider") != PROVIDER:
                        continue
                    t = float(r.get("t", 0))
                    if r.get("kind") == "node" and r.get("label"):
                        cur = nodes.get(r["label"])
                        if cur is None or t >= cur["until"]:
                            nodes[r["label"]] = {"until": t, "price_h": float(r.get("price_h") or 0.0), "gone": bool(r.get("gone"))}
                        last = max(last or t, t)
                    elif r.get("kind") == "cluster_fee":
                        cluster_until = max(cluster_until or t, t)
                        last = max(last or t, t)
        except OSError:
            continue
    if last is None:
        return {}
    return {"version": STATE_VERSION, "last_pass": last, "cluster_until": cluster_until,
            "nodes": {n: {"until": s["until"], "price_h": s["price_h"]} for n, s in nodes.items() if not s["gone"]},
            "gone": {n: s["until"] for n, s in nodes.items() if s["gone"] and last - s["until"] < GONE_MEMORY_S}}


def save_state(state: dict) -> None:
    p = state_path()
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, sort_keys=True))
    os.replace(tmp, p)                                     # atomic: a crash mid-write leaves the previous state


def meter_once(api, *, now: Optional[float] = None, force: bool = False, cfg: Optional[MeterConfig] = None,
               ledger_path: Optional[os.PathLike] = None) -> float:
    """One pass if no other process is doing one and the last was >= MIN_INTERVAL_S ago (or `force`). Returns USD charged."""
    cache_dir().mkdir(parents=True, exist_ok=True)
    with open(cache_dir() / LOCK_FILE, "a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0.0                                     # another process is metering right now
        state = load_state()
        now = now or time.time()
        if not force and state.get("last_pass") and now - float(state["last_pass"]) < MIN_INTERVAL_S:
            return 0.0
        cfg = cfg or MeterConfig.load()
        nodes = nodes_from_api(api)
        still, recent = ledger_entries(closed_since=state.get("last_pass"))
        pods = open_pods_from_api(api, still) + closed_pods(recent, state.get("pod_nodes") or {})
        created = None if state.get("cluster_until") else cluster_created_from_api(api)
        pools = {n.pool for n in nodes} | {s.get("pool", "") for s in (state.get("nodes") or {}).values()}
        ephemeral = ephemeral_from_api(api, pools)
        charges, new_state = compute_charges(now, state, nodes, pods, cfg, cluster_created=created, ephemeral=ephemeral)
        check_pod_rate(nodes, pods, cfg)
        _write(charges, now, ledger_path)                 # ledger first, state second: a crash in between over-charges, never under
        save_state(new_state)
        return sum(c["usd"] for c in charges)


_rate_warned: set = set()


def check_pod_rate(nodes: list, pods: list, cfg: MeterConfig, model=None) -> list:
    """Machine types whose node price and the pods' rate (SANDBOX_GKE_VCPU_HOUR x vCPUs) differ by more than 5%, warned once per
    process. Totals stay right either way (the meter charges what pods do not cover, and pods covering more than the node is an
    over-charge), but runs are then charged the wrong share."""
    model = model or cost_model_for(PROVIDER)
    per_vcpu = {}                                                  # what pod entries on each node are charged per vCPU-hour
    for p in pods:
        if p.usd_per_hour is None:                                  # a recorded rate is a placement share of its node, priced
            per_vcpu.setdefault(p.node, model.vcpu_hour)            # from the same catalog: nothing to compare per vCPU
    off = []
    for n in nodes:
        if n.name not in per_vcpu or not n.vcpus:
            continue
        price, pods_rate = node_price_hour(n, cfg), per_vcpu[n.name] * n.vcpus
        if price and abs(pods_rate - price) / price > 0.05:
            off.append(n.machine)
            if n.machine not in _rate_warned:
                _rate_warned.add(n.machine)
                print(f"[sandboxes] gke_meter: {n.machine} costs ${price:.3f}/h but its pods are charged ${pods_rate:.3f}/h "
                      f"({per_vcpu[n.name]:.5f}/vCPU-h x {n.vcpus:.0f}): set SANDBOX_GKE_VCPU_HOUR={price / n.vcpus:.5f}",
                      file=sys.stderr, flush=True)
    return off


def _write(charges: list, now: float, ledger_path=None) -> None:
    led = CostLedger(Path(ledger_path) if ledger_path else get_ledger().path)
    for c in charges:
        rec = {"t": now, "ev": "storage", "provider": PROVIDER, "run": c["run"], "kind": c["kind"], "gib": 0,
               "seconds": round(c["seconds"], 1), "usd": round(c["usd"], 6), "label": c["label"]}
        rec.update({k: (round(c[k], 6) if isinstance(c[k], float) else c[k]) for k in ("price_h", "max_usd", "gone", "died") if k in c})
        led._append(rec)


def close_out(*, now: Optional[float] = None, cfg: Optional[MeterConfig] = None, ledger_path: Optional[os.PathLike] = None) -> float:
    """The cluster was deleted (call this when the deletion has finished, after a last meter_once while the API still answered):
    charge every node the meter last saw, and the cluster fee, up to `now`. A node may have gone earlier in the deletion: over,
    never under. Waits for a pass in progress instead of skipping. Returns USD charged."""
    cache_dir().mkdir(parents=True, exist_ok=True)
    with open(cache_dir() / LOCK_FILE, "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = load_state()
        now = now or time.time()
        cfg = cfg or MeterConfig.load()
        charges = []
        gone = dict(state.get("gone") or {})
        for name, s in (state.get("nodes") or {}).items():
            seconds = max(0.0, now - float(s.get("until", now)))
            price = float(s.get("price_h", 0.0))
            charges.append({"usd": price * seconds / 3600.0, "kind": "node", "label": name, "run": "", "seconds": seconds,
                            "price_h": price, "gone": True})
            gone[name] = now
        if state.get("cluster_until") and cfg.cluster_fee_hour:
            seconds = max(0.0, now - float(state["cluster_until"]))
            charges.append({"usd": cfg.cluster_fee_hour * seconds / 3600.0, "kind": "cluster_fee", "label": "cluster", "run": "",
                            "seconds": seconds})
        _write(charges, now, ledger_path)
        save_state({"version": STATE_VERSION, "last_pass": now, "nodes": {}, "gone": gone, "cluster_until": None, "cluster_closed": now,
                    "pool_prices": state.get("pool_prices") or {}})
        return sum(c["usd"] for c in charges)
