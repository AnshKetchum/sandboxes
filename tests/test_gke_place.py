"""Pool placement: GKE's allocatable arithmetic, packing, the cheapest fleet for N sandboxes, and the per-sandbox choice
(room on a running node first, then the cheapest pool that can grow, spot preferred, pools that failed cooled down), wired
through the backend so a pod lands on its pool and its ledger entry carries its share of the node."""

from __future__ import annotations

import asyncio
import json
import time

import pytest

from sandboxes import gke_place as gp
from sandboxes.backends import gke_raw
from sandboxes.backends.gke_raw import GKERaw
from sandboxes.gke_place import Machine, Overhead, Pool, Shape, Snapshot, per_node, place, plan, reserve

import gke_fakes as fk
from test_gke_raw import k8s, ledger_events, run, spec  # noqa: F401  (k8s is a fixture, used by name)

# us-central1 catalog prices, 2026-10-27 (scripts/gke/prices.py)
C3D180 = Machine("c3d-standard-180", 180, 720, 1.9732, 8.1718)
C3D90 = Machine("c3d-standard-90", 90, 360, 0.9866, 4.0859)
C4D192 = Machine("c4d-standard-192", 192, 744, 4.0293, 9.0716)
N2D96 = Machine("n2d-standard-96", 96, 384, 1.9567, 4.0556)
N2DHM = Machine("n2d-highmem-96", 96, 768, 2.6394, 5.4710)
C3DHC = Machine("c3d-highcpu-180", 180, 354, 1.6225, 6.7228)
E2_32 = Machine("e2-standard-32", 32, 128, 0.6434, 1.0722)
ALL = [C3D180, C3D90, C4D192, N2D96, N2DHM, C3DHC, E2_32]
UNIT = Shape(cpu=1, mem_gib=4, disk_gib=10)


# -- what a node keeps for itself --------------------------------------------------------------------------------------------
@pytest.mark.parametrize("vcpus,reserved", [(1, 0.06), (2, 0.07), (4, 0.08), (8, 0.09), (180, 0.52)])
def test_cpu_reservation_follows_gkes_table(vcpus, reserved):
    assert gp.reserved_cpu(vcpus) == pytest.approx(reserved)


@pytest.mark.parametrize("mem,reserved", [(4, 1.0), (8, 1.8), (16, 2.6), (128, 9.32), (720, 21.16)])
def test_memory_reservation_follows_gkes_table(mem, reserved):
    assert gp.reserved_mem_gib(mem) == pytest.approx(reserved + 100 / 1024)


def test_packing_is_set_by_the_tightest_resource():
    assert per_node(C3D180, UNIT) == 174                                   # memory: (720 - 21.26 - 1.5) / 4
    assert per_node(C3DHC, UNIT) == 84                                     # half the memory of the same CPUs
    assert per_node(C3D180, Shape(1, 2, 10)) == 188                        # 2 GiB: the CPUs bind (178.98 / 0.95)
    assert per_node(E2_32, Shape(1, 4, 10), disk_gib=100) == 0             # 100 GB boot disk: no room after the image cache
    assert per_node(Machine("tiny", 400, 4000, 1, 1), Shape(1, 1, 1)) == 256 - Overhead().pods   # the pod limit binds


# -- the cheapest fleet ------------------------------------------------------------------------------------------------------
def test_1000_units_of_1_vcpu_4_gib_10_gb_cost_well_under_75_an_hour():
    opts = plan(UNIT, 1000, ALL)
    best = opts[0]
    assert (best.machine, best.spot, best.nodes) == ("c3d-standard-180", True, 6)
    assert best.total_h < 15 and best.disk_gib >= 174 * 10                 # guaranteed 10 GB each, disks included
    assert all(o.total_h < 75 for o in opts if o.spot)
    assert all(o.per_node * o.nodes >= 1000 for o in opts)


def test_the_shape_picks_the_machine():
    assert plan(Shape(1, 7, 10), 100, ALL, spot=True)[0].machine == "n2d-highmem-96"   # memory heavy: cheapest GiB
    assert plan(Shape(1, 1, 10), 1000, ALL, spot=True)[0].machine == "c3d-highcpu-180"  # CPU heavy: cheapest vCPU


# -- one sandbox at a time ---------------------------------------------------------------------------------------------------
def pools(**ceil):
    return [Pool("c3d-180-spot", C3D180, True, 2200, ceil.get("c3d", 6)), Pool("n2d-96-spot", N2D96, True, 1250, ceil.get("n2d", 2)),
            Pool("c3d-180-od", C3D180, False, 2200, ceil.get("od", 6))]


def test_a_new_node_goes_to_the_pool_cheapest_per_sandbox():
    pl = place(UNIT, pools(), Snapshot())
    assert pl.pool == "c3d-180-spot" and pl.per_node == 174
    assert pl.rate_h == pytest.approx((1.9732 + 2200 * 0.10 / 730) / 174)


def test_room_on_a_running_node_wins_over_a_cheaper_new_node():
    snap = Snapshot(nodes={"n2d-96-spot": 1}, room={"n2d-96-spot": [[10, 40, 100, 10]]})
    assert place(UNIT, pools(), snap).pool == "n2d-96-spot"


def test_spot_is_preferred_and_on_demand_is_the_fallback():
    full = Snapshot(nodes={"c3d-180-spot": 6, "n2d-96-spot": 2})
    assert place(UNIT, pools(), full).pool == "c3d-180-od"
    assert place(UNIT, pools(), Snapshot(), spot="never").pool == "c3d-180-od"
    with pytest.raises(LookupError):
        place(UNIT, pools(), full, spot="only")


def test_a_pool_that_failed_to_get_a_node_is_skipped_until_it_cools_down():
    t = time.time()
    cool = {"c3d-180-spot": t + 600}
    assert place(UNIT, pools(), Snapshot(), cooldown=cool, now=t).pool == "n2d-96-spot"
    assert place(UNIT, pools(), Snapshot(), cooldown=cool, now=t + 601).pool == "c3d-180-spot"
    every = {"c3d-180-spot": t + 600, "n2d-96-spot": t + 600, "c3d-180-od": t + 600}
    assert place(UNIT, pools(), Snapshot(), cooldown=every, now=t).pool == "c3d-180-spot"     # all cooling: try the best anyway


def test_a_sandbox_too_big_for_every_pool_is_refused():
    with pytest.raises(LookupError, match="no sandbox pool fits"):
        place(Shape(200, 4, 10), pools(), Snapshot())


def test_a_fleet_placed_one_by_one_fills_nodes_and_spills_at_ceilings():
    snap, counts = Snapshot(), {}
    ps = pools(c3d=2)
    for _ in range(1000):
        pl = place(UNIT, ps, snap)
        reserve(snap, pl.pool, UNIT, pl.per_node)
        counts[pl.pool] = counts.get(pl.pool, 0) + 1
    assert snap.nodes["c3d-180-spot"] == 2 and counts["c3d-180-spot"] == 348
    assert snap.nodes["n2d-96-spot"] == 2 and counts["n2d-96-spot"] == 2 * per_node(N2D96, UNIT, 1250)
    assert sum(counts.values()) == 1000 and snap.nodes["c3d-180-od"] == 3                # the other 470, on demand
    cost = sum(snap.nodes[p.name] * p.node_h for p in ps)
    assert cost < 75


def test_the_snapshot_reads_room_from_allocatable_minus_every_pods_requests(k8s):
    k8s.nodes = [fk.make_node("n-a", time.time(), sandbox_pool="c3d-180-spot",
                              allocatable={"cpu": "179480m", "memory": "700Gi", "ephemeral-storage": "1900Gi", "pods": "256"}),
                 fk.make_node("n-b", time.time(), sandbox_pool="c3d-180-spot", ready=False),
                 fk.make_node("n-sys", time.time(), pool="system")]
    pod = fk.make_pod({"metadata": {"name": "p1", "labels": {}}, "spec": {"containers": [
        {"image": "x", "resources": {"requests": {"cpu": "950m", "memory": "4Gi", "ephemeral-storage": "10Gi"}}}]}}, time.time())
    fk.set_ready(pod, node="n-a")
    k8s.other_pods = [pod]
    snap = gp.snapshot_from_api(k8s, {"c3d-180-spot"})
    assert snap.nodes == {"c3d-180-spot": 2}                               # the not-ready one is coming: counted, no room yet
    cpu, mem, disk, pods_left = snap.room["c3d-180-spot"][0]
    assert (cpu, mem, disk, pods_left) == (pytest.approx(178.53), pytest.approx(696), pytest.approx(1890), 255)


# -- through the backend -----------------------------------------------------------------------------------------------------
CATALOG_YAML = {
    "gke": {"zones": ["us-central1-a"], "pools": [
        {"name": "c3d-180-spot", "machine": "c3d-standard-180", "spot": True, "disk_gb": 2200, "max_nodes": 6},
        {"name": "n2d-96-spot", "machine": "n2d-standard-96", "spot": True, "disk_gb": 1250, "max_nodes": 2}]},
    "gke_meter": {"machine_shapes": {"c3d-standard-180": {"vcpus": 180, "mem_gib": 720}, "n2d-standard-96": {"vcpus": 96, "mem_gib": 384}},
                  "machine_prices": {"c3d-standard-180": {"spot": 1.9732, "ondemand": 8.1718},
                                     "n2d-standard-96": {"spot": 1.9567, "ondemand": 4.0556}}}}


@pytest.fixture
def catalog(k8s, tmp_path, monkeypatch):
    (tmp_path / "gke.yaml").write_text(json.dumps(CATALOG_YAML))
    monkeypatch.setenv("SANDBOX_GKE_CONFIG", str(tmp_path / "gke.yaml"))
    monkeypatch.setattr(gp, "PLACER", gp.Placer())
    return k8s


def test_a_sandbox_lands_on_its_pool_and_its_entry_carries_its_share_of_the_node(catalog):
    sb = GKERaw(spec(cpu=1, mem_gb=4, disk_gb=10)); run(sb.start())
    pod = catalog.pods[sb.sandbox_id]
    assert pod.spec.node_selector == {"sandboxes.pool": "c3d-180-spot"} and sb.pool == "c3d-180-spot"
    assert {"key": "sandboxes.pool", "operator": "Exists", "effect": "NoSchedule"} in pod.spec.tolerations
    opened = [e for e in ledger_events(catalog) if e["ev"] == "open"][0]
    assert opened["usd_per_hour"] == pytest.approx((1.9732 + 2200 * 0.10 / 730) / 174, rel=1e-6)
    run(sb.stop())


def test_a_spot_stockout_moves_the_retry_to_another_pool(catalog, monkeypatch):
    def unschedulable_on_c3d(pod):
        if pod.spec.node_selector.get("sandboxes.pool") == "c3d-180-spot":
            fk.set_unschedulable(pod, "0/1 nodes available; scale-up failed: ZONE_RESOURCE_POOL_EXHAUSTED")
        else:
            fk.set_ready(pod)
    catalog.on_create = unschedulable_on_c3d
    sb = GKERaw(spec(cpu=1, mem_gb=4, disk_gb=10, create_tries=2, extra={"capacity_wait_s": 0.05}))
    run(sb.start())
    assert sb.pool == "n2d-96-spot" and len(catalog.created) == 2 and len(catalog.pods) == 1
    run(sb.stop())


def test_a_dockerfile_sandbox_runs_the_image_built_for_its_context(catalog, tmp_path, monkeypatch):
    from sandboxes import gke_build
    ctx = tmp_path / "environment"; ctx.mkdir()
    (ctx / "Dockerfile").write_text("FROM python:3.12-slim\n")
    calls = []

    def ensure(cfg, context_dir, dockerfile=None, *, run="", ledger=None):
        calls.append((str(context_dir), str(dockerfile)))
        return "us-central1-docker.pkg.dev/p/envs/ctx:abc"
    monkeypatch.setattr(gke_build.BUILDER, "ensure", ensure)
    sb = GKERaw(spec(image="", extra={"dockerfile": str(ctx / "Dockerfile"), "context_dir": str(ctx)}))
    run(sb.start())
    pod = catalog.pods[sb.sandbox_id]
    assert pod.spec.containers[0].image == "us-central1-docker.pkg.dev/p/envs/ctx:abc"
    assert calls == [(str(ctx), str(ctx / "Dockerfile"))]
    run(sb.stop())


def test_with_a_catalog_a_pool_gets_300_s_to_find_a_node_before_the_next_is_tried(catalog, monkeypatch):
    waits = []
    real = GKERaw._wait_ready

    async def spy(self, name, extra=None):
        waits.append((extra or {}).get("capacity_wait_s"))
        return await real(self, name, extra)
    monkeypatch.setattr(GKERaw, "_wait_ready", spy)
    sb = GKERaw(spec()); run(sb.start())
    assert waits == [300]
    run(sb.stop())


def test_when_every_pool_fails_the_provider_reports_capacity_for_the_router(catalog):
    from sandboxes.errors import CapacityError
    catalog.on_create = lambda pod: fk.set_unschedulable(pod)
    with pytest.raises(CapacityError):
        run(GKERaw(spec(extra={"capacity_wait_s": 0.02})).start())
    assert len(catalog.created) == 2 and not catalog.pods                 # each pool tried once, nothing left behind
