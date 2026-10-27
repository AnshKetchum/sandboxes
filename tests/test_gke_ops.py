"""The GKE watchdog against an in-memory Kubernetes API and a fake gcloud: what a housekeeping pass deletes and what it must
never touch, the pending-delete queue under crashes and concurrent writers, when node pools may be resized to zero, teardown,
and the meter's close-out after a cluster deletion."""

from __future__ import annotations

import json
import threading
import time

import pytest

from sandboxes import fleet, gke_meter, gke_ops
from sandboxes.backends import gke_raw
from sandboxes.backends.gke_raw import GKERaw, pod_manifest, queue_pending_delete, take_pending_deletes
from sandboxes.gke_ops import OpsConfig, housekeep, why_delete

import gke_fakes as fk
from test_gke_raw import k8s, ledger_events, run, spec  # noqa: F401  (k8s is a fixture, used by name)

POOL = "sandbox-spot"
CFG = OpsConfig(project="p", location="us-central1", cluster="c", sandbox_pools=[POOL], idle_scale_down_min=15,
                orphan_pending_min=30)


@pytest.fixture
def ops(k8s, tmp_path, monkeypatch):
    (tmp_path / "gke.yaml").write_text(json.dumps({
        "gke": {"project": "p", "location": "us-central1", "cluster": "c", "sandbox_pools": [POOL]},
        "gke_meter": {"machine_prices": {"c4d-standard-192": {"spot": 3.6}}, "disk_gb_month": 0, "cluster_fee_hour": 0.1}}))
    monkeypatch.setenv("SANDBOX_GKE_CONFIG", str(tmp_path / "gke.yaml"))
    for k in ("GKE_PROJECT", "GKE_LOCATION", "GKE_CLUSTER"):
        monkeypatch.delenv(k, raising=False)
    calls = []

    def fake_gcloud(*args, timeout=0):
        calls.append(list(args))
        if args[:3] == ("container", "clusters", "resize") and args[args.index("--num-nodes") + 1] == "0":
            pool = args[args.index("--node-pool") + 1]
            k8s.nodes = [n for n in k8s.nodes if n.metadata.labels.get("cloud.google.com/gke-nodepool") != pool]
        if args[:3] == ("container", "clusters", "delete"):
            k8s.nodes = []
        return ""
    monkeypatch.setattr(gke_ops, "gcloud", fake_gcloud)
    monkeypatch.setattr(gke_raw, "_UP", {"t": float("inf")})     # the cluster counts as up here (auto-up: test_gke_lifecycle)
    k8s.gcloud_calls = calls
    return k8s


def pod(api, created_ago: float, **kw):
    """A pod of ours put straight into the fake API (no creator process: what a dead process leaves)."""
    name = f"sbx-{len(api.pods):016x}"
    p = fk.make_pod(pod_manifest(name, spec(**kw), "sandboxes"), time.time() - created_ago)
    api.pods[name] = p
    return p


def open_sids(api) -> set:
    ev = ledger_events(api)
    closed = {e["id"] for e in ev if e["ev"] == "close"}
    return {e["sid"] for e in ev if e["ev"] == "open" and e["id"] not in closed}


# -- what a pass deletes ---------------------------------------------------------------------------------------------------------
def test_why_delete_spares_live_pods_and_names_every_kind_of_leftover(k8s):
    now = time.time()
    running = pod(k8s, 600, lifetime_s=3600); fk.set_ready(running)
    assert why_delete(running, now, CFG) is None
    overdue = pod(k8s, 3600 + 700, lifetime_s=3600); fk.set_ready(overdue)
    assert "past its deadline" in why_delete(overdue, now, CFG)
    within_slack = pod(k8s, 3600 + 300, lifetime_s=3600); fk.set_ready(within_slack)
    assert why_delete(within_slack, now, CFG) is None                         # the kubelet's own deadline gets it first
    done = pod(k8s, 60); fk.set_finished(done, "Failed", "DeadlineExceeded")
    assert why_delete(done, now, CFG) == "finished (Failed:DeadlineExceeded)"
    waiting = pod(k8s, 1200 + 200); fk.set_unschedulable(waiting)              # within its creator's 1200 s wait + slack
    assert why_delete(waiting, now, CFG) is None
    orphan = pod(k8s, 1200 + 400); fk.set_unschedulable(orphan)
    assert "past its creator's wait" in why_delete(orphan, now, CFG)
    long_wait = pod(k8s, 3000, extra={"ready_timeout_s": 3600}); fk.set_unschedulable(long_wait)
    assert why_delete(long_wait, now, CFG) is None                              # a creator configured to wait longer is respected
    legacy = pod(k8s, 31 * 60); legacy.metadata.annotations.pop(gke_raw.READY_ANNOTATION)
    assert "past its creator's wait" in why_delete(legacy, now, CFG)           # no annotation: orphan_pending_min
    deleting = pod(k8s, 60); fk.set_ready(deleting); deleting.metadata.deletion_timestamp = fk.ts(now - 30)
    assert why_delete(deleting, now, CFG) is None
    stuck = pod(k8s, 60); fk.set_ready(stuck); stuck.metadata.deletion_timestamp = fk.ts(now - 600)
    assert "stuck terminating" in why_delete(stuck, now, CFG)


def test_a_pass_deletes_leftovers_closes_their_entries_and_leaves_live_sandboxes_alone(ops):
    live = GKERaw(spec()); run(live.start())
    dead = GKERaw(spec()); run(dead.start())
    fk.set_finished(ops.pods[dead.sandbox_id], "Failed", "DeadlineExceeded")    # its process died; the kubelet ended it
    orphan = pod(ops, 1700); fk.set_unschedulable(orphan)
    rep = housekeep(ops)
    assert not rep["errors"], rep["errors"]
    assert sorted(n for n, _ in rep["deleted"]) == sorted([dead.sandbox_id, orphan.metadata.name])
    assert list(ops.pods) == [live.sandbox_id]
    assert open_sids(ops) == {live.sandbox_id} and rep["reconciled"] == 1
    run(live.stop())


def test_dry_run_deletes_nothing_meters_nothing_and_resizes_nothing(ops):
    done = pod(ops, 60); fk.set_finished(done)
    ops.nodes = [fk.make_node("gke-c-sandbox-spot-1", time.time() - 7200)]
    rep = housekeep(ops, CFG, now=time.time() + 3600, dry_run=True)
    assert [n for n, _ in rep["deleted"]] == [done.metadata.name] and done.metadata.name in ops.pods
    assert not ops.gcloud_calls and not ledger_events(ops)


def test_a_failing_step_does_not_stop_the_others(ops):
    done = pod(ops, 60); fk.set_finished(done)

    def broken(label_selector=None):
        raise fk.FakeApiError(500, "nodes unavailable")
    ops.list_node = broken                                                     # the meter cannot list nodes
    rep = housekeep(ops)
    assert any(e.startswith("meter:") for e in rep["errors"])
    assert done.metadata.name not in ops.pods                                   # the pods step still ran


def test_a_failed_listing_closes_no_entries_and_resizes_no_pool(ops):
    live = GKERaw(spec()); run(live.start())
    ops.nodes = [fk.make_node("gke-c-sandbox-spot-1", time.time() - 7200)]

    def broken(ns, **kw):
        raise fk.FakeApiError(503, "apiserver unavailable")
    ops.list_namespaced_pod = broken
    now = time.time()
    housekeep(ops, CFG, now=now)
    rep = housekeep(ops, CFG, now=now + 3600)
    assert any(e.startswith("pods:") for e in rep["errors"])
    assert open_sids(ops) == {live.sandbox_id} and not ops.gcloud_calls


def test_only_one_pass_runs_at_a_time(ops):
    entered, release = threading.Event(), threading.Event()
    real = gke_ops.retry_pending_deletes

    def slow(api):
        entered.set(); release.wait(5)
        return real(api)
    gke_ops.retry_pending_deletes = slow
    try:
        t = threading.Thread(target=housekeep, args=(ops,)); t.start()
        assert entered.wait(5)
        assert housekeep(ops)["skipped"]
        release.set(); t.join(5)
    finally:
        gke_ops.retry_pending_deletes = real


# -- the pending-delete queue ------------------------------------------------------------------------------------------------------
def test_a_delete_that_failed_in_its_process_is_retried_by_the_next_pass(ops, monkeypatch):
    monkeypatch.setattr(gke_raw.time, "sleep", lambda s: None)
    sb = GKERaw(spec()); run(sb.start()); name = sb.sandbox_id
    ops.delete_failures = 99
    run(sb.stop())                                                              # every try fails: queued
    assert name in ops.pods and take_pending_deletes()[0][0]["name"] == name
    rep = housekeep(ops)                                                        # still failing: queued again, not lost
    assert rep["pending_failed"] == 1 and name in ops.pods
    assert [e["name"] for e in take_pending_deletes()[0]] == [name]
    ops.delete_failures = 0
    rep = housekeep(ops)
    assert rep["pending_deleted"] == 1 and name not in ops.pods
    assert take_pending_deletes() == ([], [])


def test_a_delete_queued_while_the_watchdog_holds_the_queue_is_not_lost(ops):
    queue_pending_delete("sbx-a", "sandboxes", "500")
    entries, files = take_pending_deletes()
    queue_pending_delete("sbx-b", "sandboxes", "500")                            # a sandbox fails a delete meanwhile
    for f in files:
        f.unlink()
    assert [e["name"] for e in entries] == ["sbx-a"]
    assert [e["name"] for e in take_pending_deletes()[0]] == ["sbx-b"]


def test_a_queue_taken_by_a_crashed_pass_is_taken_again(ops):
    queue_pending_delete("sbx-a", "sandboxes", "500")
    take_pending_deletes()                                                      # taken, then the process died
    queue_pending_delete("sbx-b", "sandboxes", "500")
    entries, files = take_pending_deletes()
    assert sorted(e["name"] for e in entries) == ["sbx-a", "sbx-b"] and len(files) == 2


def test_many_concurrent_queuers_and_takers_lose_nothing(ops):
    names = [f"sbx-{i:04d}" for i in range(400)]
    seen, lock = [], threading.Lock()

    def queuer(chunk):
        for n in chunk:
            queue_pending_delete(n, "sandboxes", "500")

    def taker(stop):
        while not stop.is_set():
            with lock:                                                          # takers are serialized by the ops lock
                entries, files = take_pending_deletes()
                seen.extend(e["name"] for e in entries)
                for f in files:
                    f.unlink()
    stop = threading.Event()
    tk = threading.Thread(target=taker, args=(stop,)); tk.start()
    qs = [threading.Thread(target=queuer, args=(names[i::8],)) for i in range(8)]
    for q in qs:
        q.start()
    for q in qs:
        q.join()
    stop.set(); tk.join()
    entries, _ = take_pending_deletes()
    assert sorted(seen + [e["name"] for e in entries]) == names


# -- idle node pools -------------------------------------------------------------------------------------------------------------
def test_idle_sandbox_pools_are_resized_to_zero_once_and_only_after_the_idle_time(ops):
    ops.nodes = [fk.make_node("gke-c-sandbox-spot-1", time.time() - 7200), fk.make_node("gke-c-system-1", time.time() - 7200,
                                                                                         machine="e2-standard-4", pool="system")]
    t = time.time()
    assert housekeep(ops, CFG, now=t)["scaled_down"] == []                    # idle clock starts
    assert housekeep(ops, CFG, now=t + 14 * 60)["scaled_down"] == []
    assert housekeep(ops, CFG, now=t + 16 * 60)["scaled_down"] == [POOL]
    resize = ops.gcloud_calls[0]
    assert resize[:3] == ["container", "clusters", "resize"] and resize[resize.index("--node-pool") + 1] == POOL
    assert "--async" in resize and resize[resize.index("--project") + 1] == "p"
    assert [n.metadata.name for n in ops.nodes] == ["gke-c-system-1"]          # the system pool is never touched
    ops.nodes.append(fk.make_node("gke-c-sandbox-spot-2", t))                  # a node comes back: no second resize this soon
    assert housekeep(ops, CFG, now=t + 20 * 60)["scaled_down"] == [] and len(ops.gcloud_calls) == 1


def test_a_pool_with_any_pod_of_ours_or_a_foreign_workload_is_not_resized(ops):
    node = "gke-c-sandbox-spot-1"
    ops.nodes = [fk.make_node(node, time.time() - 7200)]
    t = time.time()
    live = GKERaw(spec()); run(live.start())
    housekeep(ops, CFG, now=t); assert housekeep(ops, CFG, now=t + 3600)["scaled_down"] == []
    run(live.stop())
    ops.other_pods = [fk.other_pod("ds", node, owner_kind="DaemonSet"), fk.other_pod("static", node, mirror=True),
                      fk.other_pod("someones-job", node, namespace="default", owner_kind="Job")]
    housekeep(ops, CFG, now=t + 3700)
    assert housekeep(ops, CFG, now=t + 3700 + 16 * 60)["scaled_down"] == [] and not ops.gcloud_calls
    ops.other_pods = ops.other_pods[:2]                                        # DaemonSets and static pods do not block it
    assert housekeep(ops, CFG, now=t + 3700 + 32 * 60)["scaled_down"] == [POOL]


def test_without_cluster_coordinates_the_resize_is_reported_not_crashed(ops):
    ops.nodes = [fk.make_node("gke-c-sandbox-spot-1", time.time() - 7200)]
    bare = OpsConfig(sandbox_pools=[POOL])
    t = time.time()
    housekeep(ops, bare, now=t)
    rep = housekeep(ops, bare, now=t + 3600)
    assert any("gke.project" in e for e in rep["errors"]) and not ops.gcloud_calls


# -- teardown and the meter's close-out ------------------------------------------------------------------------------------------
def test_teardown_of_one_run_leaves_the_others(ops):
    a = GKERaw(spec(labels={"sandboxes.run": "a"})); run(a.start())
    b = GKERaw(spec(labels={"sandboxes.run": "b"})); run(b.start())
    rep = gke_ops.teardown(ops, CFG, run="a")
    assert rep["deleted"] == [a.sandbox_id] and list(ops.pods) == [b.sandbox_id] and open_sids(ops) == {b.sandbox_id}
    with pytest.raises(RuntimeError, match="still running"):
        gke_ops.teardown(ops, CFG, run="a", pools=True)                        # never resize a pool under b
    assert not ops.gcloud_calls
    run(b.stop())


def test_teardown_with_pools_meters_until_the_nodes_are_gone(ops, monkeypatch):
    monkeypatch.setattr(gke_ops.time, "sleep", lambda s: None)
    ops.nodes = [fk.make_node("gke-c-sandbox-spot-1", time.time() - 3600)]
    sb = GKERaw(spec()); run(sb.start())
    rep = gke_ops.teardown(ops, CFG, pools=True)
    assert rep["resized"] == [POOL] and not ops.pods and not ops.nodes and not open_sids(ops)
    node_usd = sum(e["usd"] for e in ledger_events(ops) if e.get("kind") == "node") + \
        sum(e.get("usd", 0) for e in ledger_events(ops) if e["ev"] == "close")
    assert node_usd == pytest.approx(3.6, rel=0.02)                             # an hour of the node, pod share included


def test_delete_cluster_charges_what_the_meter_last_saw_up_to_the_end_of_the_deletion(ops, monkeypatch):
    t0 = time.time()
    ops.nodes = [fk.make_node("gke-c-sandbox-spot-1", t0 - 3600)]
    ops.cluster_created = t0 - 3600
    slow_delete = {}
    real = ops.gcloud_calls

    def gcloud_slow(*args, timeout=0):
        real.append(list(args))
        slow_delete["took"] = 600                                               # nodes bill through the 10 min deletion
        ops.nodes = []
        return ""
    monkeypatch.setattr(gke_ops, "gcloud", gcloud_slow)
    monkeypatch.setattr(gke_meter.time, "time", lambda: t0 + slow_delete.get("took", 0))
    rep = gke_ops.teardown(ops, CFG, delete_cluster=True)
    assert rep["cluster_deleted"] and real[-1][:3] == ["container", "clusters", "delete"]
    ev = ledger_events(ops)
    node = sum(e["usd"] for e in ev if e.get("kind") == "node")
    fee = sum(e["usd"] for e in ev if e.get("kind") == "cluster_fee")
    assert node == pytest.approx(3.6 * (3600 + 600) / 3600, rel=0.01)
    assert fee == pytest.approx(0.1 * (3600 + 600) / 3600, rel=0.01)
    # an API that still answers (a cluster recreated under the same context): the fee from the close-out on, nothing twice
    assert gke_meter.meter_once(ops, force=True, now=t0 + 900) == pytest.approx(0.1 * 300 / 3600)


def test_close_out_is_idempotent(ops):
    t0 = time.time()
    ops.nodes = [fk.make_node("gke-c-sandbox-spot-1", t0 - 3600)]
    gke_meter.meter_once(ops, force=True, now=t0)
    first = gke_meter.close_out(now=t0 + 300)
    assert first == pytest.approx(3.6 * 300 / 3600 + 0.1 * 300 / 3600, rel=0.01)
    assert gke_meter.close_out(now=t0 + 600) == 0.0


# -- the janitor and status ------------------------------------------------------------------------------------------------------
def test_the_janitor_runs_the_gke_pass(ops):
    done = pod(ops, 60); fk.set_finished(done)
    reps = run(fleet.housekeep_all(["gke"]))
    assert [n for n, _ in reps["gke"]["deleted"]] == [done.metadata.name]


def test_status_shows_pools_burn_pods_and_queued_deletes(ops):
    ops.nodes = [fk.make_node("gke-c-sandbox-spot-1", time.time() - 3600), fk.make_node("gke-c-sandbox-spot-2", time.time())]
    a = GKERaw(spec(labels={"sandboxes.run": "r1"})); run(a.start())
    queue_pending_delete("sbx-x", "sandboxes", "500")
    st = gke_ops.status(ops)
    assert st["pools"][POOL]["nodes"] == 2 and st["pools"][POOL]["usd_per_hour"] == pytest.approx(7.2)
    assert st["burn_usd_per_hour"] == pytest.approx(7.3) and st["pods_by_run"] == {"r1": 1} and st["pending_deletes"] == 1
    assert "DELETES QUEUED" in gke_ops.format_status(st)
    run(a.stop())
