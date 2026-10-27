"""The cluster's life between runs: node pools kept to the catalog, `down` leaving nothing that bills (pods, nodes, the system
pool, images, build uploads), `up` and the automatic up on a sandbox start, the watchdog's artifact purge and auto-down, and
the residue audit that prices whatever is left in the project. Against the fake Kubernetes API and a fake gcloud."""

from __future__ import annotations

import json
import threading
import time

import pytest

from sandboxes import gke_build, gke_ops
from sandboxes.backends import gke_raw
from sandboxes.backends.gke_raw import GKERaw
from sandboxes.gke_ops import OpsConfig

import gke_fakes as fk
from test_gke_raw import k8s, ledger_events, run, spec  # noqa: F401  (k8s is a fixture, used by name)

CATALOG = [{"name": "c3d-180-spot", "machine": "c3d-standard-180", "spot": True, "disk_gb": 2200, "max_nodes": 4},
           {"name": "c4d-192-spot", "machine": "c4d-standard-192", "spot": True, "disk_gb": 2250, "max_nodes": 2},
           {"name": "c3d-180-od", "machine": "c3d-standard-180", "spot": False, "disk_gb": 2200, "max_nodes": 2}]


def cfg(**kw) -> OpsConfig:
    c = OpsConfig(project="p", location="us-central1-a", cluster="c", pools=list(CATALOG), zones=["us-central1-a", "us-central1-b"],
                  node_service_account="nodes@p.iam.gserviceaccount.com", **kw)
    c.sandbox_pools = [p["name"] for p in c.pools]
    return c


class FakeGcloud:
    def __init__(self, api):
        self.api, self.calls, self.pools, self.lists, self.fail = api, [], {}, {}, set()

    def __call__(self, *args, timeout=0):
        a = list(args)
        self.calls.append(a)
        key = " ".join(a[:3])
        if key in self.fail:
            raise RuntimeError(f"PERMISSION_DENIED: {key}")
        if key in self.lists:
            return json.dumps(self.lists[key])
        if a[:3] == ["container", "node-pools", "list"]:
            return json.dumps(list(self.pools.values()))
        if a[:3] == ["container", "clusters", "resize"]:
            pool, n = a[a.index("--node-pool") + 1], int(a[a.index("--num-nodes") + 1])
            self.api.nodes = [x for x in self.api.nodes if x.metadata.labels.get("cloud.google.com/gke-nodepool") != pool]
            self.api.nodes += [fk.make_node(f"gke-c-{pool}-{i}", time.time(), pool=pool, spot=False) for i in range(n)]
            return ""
        if a[:3] == ["container", "clusters", "list"]:
            return "[]"
        if a[:2] in (["container", "clusters"], ["container", "node-pools"]):
            return ""
        if a[:2] == ["storage", "du"]:
            return self.lists.get("du", {}).get(a[-1], "0  " + a[-1])
        if a[:3] in (["compute", "instances", "list"], ["compute", "disks", "list"], ["compute", "snapshots", "list"],
                     ["compute", "images", "list"], ["compute", "addresses", "list"], ["compute", "forwarding-rules", "list"],
                     ["artifacts", "repositories", "list"], ["storage", "buckets", "list"], ["container", "clusters", "list"]):
            return "[]"
        raise AssertionError(f"unexpected gcloud {a}")


@pytest.fixture
def g(k8s, monkeypatch):
    fake = FakeGcloud(k8s)
    monkeypatch.setattr(gke_ops, "gcloud", fake)
    monkeypatch.setattr(gke_ops.time, "sleep", lambda s: None)
    monkeypatch.setattr(gke_raw, "_UP", {"t": 0.0})
    return fake


def pool_json(name, machine, spot, disk, maxn, ours=True):
    return {"name": name, "config": {"machineType": machine, "spot": spot, "diskSizeGb": disk,
                                     "labels": {"sandboxes.pool": name} if ours else {}},
            "autoscaling": {"enabled": True, "totalMaxNodeCount": maxn}, "locations": ["us-central1-a"]}


# -- the catalog's pools -----------------------------------------------------------------------------------------------------
def test_the_pool_plan_creates_updates_replaces_and_prunes_only_our_pools(g):
    g.pools = {"c3d-180-spot": pool_json("c3d-180-spot", "c3d-standard-180", True, 2200, 1),
               "c4d-192-spot": pool_json("c4d-192-spot", "c4d-standard-96", True, 2250, 2),
               "old-sandboxes": pool_json("old-sandboxes", "n2d-standard-96", True, 1250, 2),
               "default-pool": pool_json("default-pool", "e2-standard-2", False, 50, 3, ours=False)}
    plan = {(a, n) for a, n, _ in gke_ops.plan_pools(cfg(), gke_ops.cluster_pools(cfg()))}
    assert plan == {("update", "c3d-180-spot"), ("replace", "c4d-192-spot"), ("create", "c3d-180-od"), ("delete", "old-sandboxes")}


def test_applying_the_plan_creates_tainted_labelled_pools_and_never_deletes_one_with_nodes(g, k8s):
    actions = [("create", "c3d-180-od", ""), ("create", "c4d-192-spot", ""), ("update", "c3d-180-spot", ""),
               ("replace", "c4d-192-spot", ""), ("delete", "old-sandboxes", "")]
    k8s.nodes = [fk.make_node("gke-c-old-1", time.time(), pool="old-sandboxes")]
    done = gke_ops.apply_pools(k8s, cfg(), actions)                          # no --replace, no --prune
    assert done == [("create", "c3d-180-od"), ("create", "c4d-192-spot"), ("update", "c3d-180-spot")]
    od = next(c for c in g.calls if c[:4] == ["container", "node-pools", "create", "c3d-180-od"])
    assert "--spot" not in od and od[od.index("--disk-type") + 1] == "pd-balanced"
    assert od[od.index("--node-taints") + 1] == "sandboxes.pool=c3d-180-od:NoSchedule"
    assert od[od.index("--node-labels") + 1] == "sandboxes.pool=c3d-180-od" and od[od.index("--max-pods-per-node") + 1] == "256"
    assert od[od.index("--total-max-nodes") + 1] == "2" and "--max-nodes" not in od
    assert od[od.index("--node-locations") + 1] == "us-central1-a,us-central1-b" and "--workload-metadata" in od
    c4d = next(c for c in g.calls if c[:4] == ["container", "node-pools", "create", "c4d-192-spot"])
    assert "--spot" in c4d and c4d[c4d.index("--disk-type") + 1] == "hyperdisk-balanced"          # C4D boots from Hyperdisk only
    g.calls.clear()
    gke_ops.apply_pools(k8s, cfg(), [("delete", "old-sandboxes", "")], prune=True)
    assert not any(c[:3] == ["container", "node-pools", "delete"] for c in g.calls)   # it has a node
    k8s.nodes = []
    gke_ops.apply_pools(k8s, cfg(), [("delete", "old-sandboxes", "")], prune=True)
    assert any(c[:4] == ["container", "node-pools", "delete", "old-sandboxes"] for c in g.calls)


# -- down and up -------------------------------------------------------------------------------------------------------------
def test_down_leaves_no_pod_no_node_no_image_and_no_upload(g, k8s, monkeypatch):
    purged = {}
    monkeypatch.setattr(gke_build.BuildConfig, "load", classmethod(lambda cls, path=None: gke_build.BuildConfig(
        project="p", registry="r/p/envs", bucket="stage", region="us-central1")))
    monkeypatch.setattr(gke_build, "purge", lambda bcfg, all_images=False, **kw: purged.setdefault("images", all_images) and {"deleted": ["x"]})
    monkeypatch.setattr(gke_build, "purge_staging", lambda bcfg, older_than_s=3600, **kw: purged.setdefault("uploads", older_than_s) or 1)
    k8s.nodes = [fk.make_node("gke-c-c3d-180-spot-1", time.time(), pool="c3d-180-spot"),
                 fk.make_node("gke-c-default-pool-1", time.time(), pool="default-pool", spot=False)]
    sb = GKERaw(spec()); run(sb.start())
    rep = gke_ops.down(k8s, cfg())
    assert not k8s.pods and not k8s.nodes
    assert purged == {"images": True, "uploads": 0}
    resized = [c[c.index("--node-pool") + 1] for c in g.calls if c[:3] == ["container", "clusters", "resize"]]
    assert set(resized) == {"c3d-180-spot", "c4d-192-spot", "c3d-180-od", "default-pool"}
    off = [i for i, c in enumerate(g.calls) if "--no-enable-autoscaling" in c]
    sys_resize = [i for i, c in enumerate(g.calls) if c[:3] == ["container", "clusters", "resize"] and "default-pool" in c]
    assert off and off[0] < sys_resize[0]                                    # autoscaling off first, or it would bring a node back
    assert rep["residue"]["items"] == [] and not [e for e in ledger_events(k8s) if e["ev"] == "open"
                                                  and e["id"] not in {x["id"] for x in ledger_events(k8s) if x["ev"] == "close"}]


def test_up_brings_the_system_pool_back_and_waits_for_a_ready_node(g, k8s):
    rep = gke_ops.up(k8s, cfg())
    assert rep == {"up": True, "ready": True}
    on = next(c for c in g.calls if "--enable-autoscaling" in c and "default-pool" in c)
    assert on[on.index("--min-nodes") + 1] == "1"


def test_a_sandbox_start_on_a_cluster_that_is_down_brings_it_up_once(g, k8s, tmp_path, monkeypatch):
    (tmp_path / "gke.yaml").write_text(json.dumps({"gke": {"project": "p", "location": "us-central1-a", "cluster": "c"}}))
    monkeypatch.setenv("SANDBOX_GKE_CONFIG", str(tmp_path / "gke.yaml"))
    sbs = [GKERaw(spec()) for _ in range(6)]
    ts = [threading.Thread(target=lambda s=s: run(s.start())) for s in sbs]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert len(k8s.pods) == 6
    assert sum(c[:3] == ["container", "clusters", "resize"] and "default-pool" in c for c in g.calls) == 1
    for s in sbs:
        run(s.stop())


def test_without_auto_up_a_start_on_a_cluster_that_is_down_fails_with_what_to_run(g, k8s, tmp_path, monkeypatch):
    from sandboxes.errors import SandboxCreateError
    (tmp_path / "gke.yaml").write_text(json.dumps({"gke": {"project": "p", "location": "l", "cluster": "c", "auto_up": False}}))
    monkeypatch.setenv("SANDBOX_GKE_CONFIG", str(tmp_path / "gke.yaml"))
    with pytest.raises(SandboxCreateError, match="gke_ops up"):
        run(GKERaw(spec(create_tries=1)).start())
    assert not k8s.created and not g.calls


# -- the watchdog ------------------------------------------------------------------------------------------------------------
def test_the_watchdog_purges_images_no_running_pod_uses_at_most_every_10_minutes(g, k8s, monkeypatch):
    seen = []
    monkeypatch.setattr(gke_build.BuildConfig, "load", classmethod(lambda cls, path=None: gke_build.BuildConfig(
        project="p", registry="r/p/envs", bucket="stage", region="us-central1")))
    monkeypatch.setattr(gke_build, "purge", lambda bcfg, in_use=frozenset(), now=None, **kw: seen.append(set(in_use)) or
                        {"deleted": [], "gone": [], "failed": []})
    monkeypatch.setattr(gke_build, "purge_staging", lambda *a, **kw: 0)
    sb = GKERaw(spec(image="r/p/envs/ctx:abc")); run(sb.start())
    done = GKERaw(spec(image="r/p/envs/ctx:old")); run(done.start())
    fk.set_finished(k8s.pods[done.sandbox_id])
    t = time.time()
    gke_ops.housekeep(k8s, cfg(auto_down_min=0), now=t)
    gke_ops.housekeep(k8s, cfg(auto_down_min=0), now=t + 60)
    assert seen == [{"r/p/envs/ctx:abc"}]                                    # the finished pod's image is not in use
    gke_ops.housekeep(k8s, cfg(auto_down_min=0), now=t + 700)
    assert len(seen) == 2
    run(sb.stop())


def test_an_idle_cluster_goes_down_once_and_comes_back_on_the_next_start(g, k8s, monkeypatch):
    downs = []
    monkeypatch.setattr(gke_ops, "down", lambda api, c, wait=True: downs.append(time.time()) or {"down": True})
    t = time.time()
    gke_ops.housekeep(k8s, cfg(auto_down_min=60), now=t)
    gke_ops.housekeep(k8s, cfg(auto_down_min=60), now=t + 59 * 60)
    assert downs == []
    gke_ops.housekeep(k8s, cfg(auto_down_min=60), now=t + 61 * 60)
    gke_ops.housekeep(k8s, cfg(auto_down_min=60), now=t + 62 * 60)
    assert len(downs) == 1
    sb = GKERaw(spec()); run(sb.start())                                    # busy again: the down marker is cleared
    gke_ops.housekeep(k8s, cfg(auto_down_min=60), now=t + 63 * 60)
    assert "down_at" not in gke_ops._load_state()
    run(sb.stop())


# -- residue -----------------------------------------------------------------------------------------------------------------
def test_residue_prices_everything_that_bills_while_idle_and_says_what_it_could_not_see(g):
    g.lists = {
        "container clusters list": [{"name": "c", "location": "us-central1-a"}],
        "compute instances list": [{"name": "gke-c-x", "status": "RUNNING", "machineType": "zones/a/machineTypes/c3d-standard-180",
                                    "scheduling": {"provisioningModel": "SPOT"}},
                                   {"name": "stopped", "status": "TERMINATED", "machineType": "x/e2-small"}],
        "compute disks list": [{"name": "gke-c-x", "sizeGb": "2200", "type": "x/pd-balanced", "users": ["vm"]},
                               {"name": "pvc-lost", "sizeGb": "100", "type": "x/pd-ssd"}],
        "artifacts repositories list": [{"name": "projects/p/locations/us-central1/repositories/envs", "sizeBytes": str(10 * 2 ** 30)}],
        "storage buckets list": [{"name": "stage", "storage_url": "gs://stage/"}],
        "du": {"gs://stage/": "2000000000  gs://stage/"},
    }
    g.fail = {"compute snapshots list"}
    mcfg = type("M", (), {"machine_prices": {"c3d-standard-180": {"spot": 1.9732}}})()
    r = gke_ops.residue(cfg(), mcfg)
    what = {i["what"]: i for i in r["items"]}
    assert what["VM"]["usd_month"] == pytest.approx(1.9732 * 730) and "stopped" not in json.dumps(r["items"])
    assert what["ORPHANED disk"]["usd_month"] == pytest.approx(17.0) and what["disk"]["usd_month"] == pytest.approx(220.0)
    assert what["registry storage"]["usd_month"] == pytest.approx(0.95) and what["bucket storage"]["usd_month"] == pytest.approx(0.04)
    assert what["GKE cluster"]["usd_month"] == pytest.approx(73.0)
    assert r["unlisted"] and r["unlisted"][0].startswith("snapshots")
    assert "ORPHANED disk" in gke_ops.format_residue(r)


def test_an_empty_project_has_no_residue(g):
    r = gke_ops.residue(cfg(), type("M", (), {"machine_prices": {}})())
    assert r == {"items": [], "usd_month": 0.0, "unlisted": []}
    assert gke_ops.format_residue(r) == "  nothing in the project bills while idle"


def test_a_disabled_catalog_pool_is_neither_created_nor_placed_on(g, tmp_path, monkeypatch):
    from sandboxes import gke_place
    cat = [dict(CATALOG[0], enabled=False), CATALOG[1]]
    (tmp_path / "gke.yaml").write_text(json.dumps({"gke": {"project": "p", "location": "l", "cluster": "c", "pools": cat},
                                                   "gke_meter": {"machine_shapes": {"c3d-standard-180": {"vcpus": 180, "mem_gib": 720},
                                                                                    "c4d-standard-192": {"vcpus": 192, "mem_gib": 744}},
                                                                 "machine_prices": {"c3d-standard-180": {"spot": 1.97},
                                                                                    "c4d-standard-192": {"spot": 4.03}}}}))
    monkeypatch.setenv("SANDBOX_GKE_CONFIG", str(tmp_path / "gke.yaml"))
    assert [p.name for p in gke_place.load_catalog()[0]] == ["c4d-192-spot"]
    assert [n for a, n, _ in gke_ops.plan_pools(OpsConfig.load(), {})] == ["c4d-192-spot"]


def test_down_repeats_the_system_pool_resize_when_a_node_comes_back(g, k8s, monkeypatch):
    """Seen live: the first resize to 0 right after turning autoscaling off was undone by the autoscaler (a new node in 5 s)."""
    resizes = []
    real = g.__call__

    def flaky(*args, timeout=0):
        out = real(*args, timeout=timeout)
        if args[:3] == ("container", "clusters", "resize") and "default-pool" in args:
            resizes.append(1)
            if len(resizes) == 1:                                            # the autoscaler puts a node back
                k8s.nodes.append(fk.make_node("gke-c-default-pool-new", time.time(), pool="default-pool", spot=False))
        return out
    monkeypatch.setattr(gke_ops, "gcloud", flaky)
    k8s.nodes = [fk.make_node("gke-c-default-pool-1", time.time(), pool="default-pool", spot=False)]
    assert gke_ops.drain_system_pool(k8s, cfg(), settle_s=0.01, poll_s=0.001, drain_s=0.01) == 2
    assert not k8s.nodes and len(resizes) == 2


def test_a_system_pool_that_will_not_stay_empty_is_an_error_not_a_silent_bill(g, k8s, monkeypatch):
    real = g.__call__

    def stubborn(*args, timeout=0):
        out = real(*args, timeout=timeout)
        if args[:3] == ("container", "clusters", "resize") and "default-pool" in args:
            k8s.nodes.append(fk.make_node(f"gke-c-default-pool-{len(k8s.nodes)}", time.time(), pool="default-pool", spot=False))
        return out
    monkeypatch.setattr(gke_ops, "gcloud", stubborn)
    with pytest.raises(RuntimeError, match="keeps a node"):
        gke_ops.drain_system_pool(k8s, cfg(), tries=3, settle_s=0.01, poll_s=0.001, drain_s=0.01)


def test_a_cluster_operation_waits_for_the_one_already_running(g, monkeypatch):
    """Found live: GKE runs one operation at a time; a pool resize right after another failed with CLUSTER_ALREADY_HAS_OPERATION."""
    calls = []

    def busy_twice(*args, timeout=0):
        calls.append(args)
        if len(calls) <= 2:
            raise RuntimeError("FAILED_PRECONDITION: Cluster is running incompatible operation op-1. reason: CLUSTER_ALREADY_HAS_OPERATION")
        return ""
    monkeypatch.setattr(gke_ops, "gcloud", busy_twice)
    gke_ops.resize_pool(cfg(), "c3d-180-spot", 0)
    assert len(calls) == 3

    def denied(*args, timeout=0):
        raise RuntimeError("PERMISSION_DENIED: no")
    monkeypatch.setattr(gke_ops, "gcloud", denied)
    with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
        gke_ops.resize_pool(cfg(), "c3d-180-spot", 0)                       # other errors are not retried
