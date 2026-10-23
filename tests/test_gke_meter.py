"""The GKE node meter: the ledger must add up to what Google bills for the nodes (and the cluster), never less, whatever the
interleaving of node births and deaths, pods, meter outages, lost state and concurrent meters."""

from __future__ import annotations

import json
import random
import threading
from pathlib import Path

import pytest

from sandboxes import gke_meter
from sandboxes.cost import CostLedger, summary
from sandboxes.gke_meter import STALE_GAP_S, MeterConfig, Node, OpenPod, compute_charges, meter_once, node_price_hour
from sandboxes.pricing import CostModel

import gke_fakes as fk

PRICE = 3.60          # USD/h for the test machine type
VCPUS = 192
CFG = MeterConfig(machine_prices={"c4d-standard-192": {"spot": PRICE, "ondemand": 9.0}}, disk_gb_month=0.0, cluster_fee_hour=0.0)
MODEL = CostModel("gke", vcpu_hour=PRICE / VCPUS)
T0 = 1_000_000.0


def node(name="n1", created=T0, spot=True, machine="c4d-standard-192", pool="sandbox-spot", vcpus=VCPUS):
    return Node(name, created, machine, spot, pool, vcpus)


def total(charges, kind="node"):
    return sum(c["usd"] for c in charges if c["kind"] == kind)


# -- pure arithmetic --------------------------------------------------------------------------------------------------------------
def test_a_lone_node_is_charged_from_its_creation_then_per_pass():
    charges, st = compute_charges(T0 + 3600, {}, [node()], [], CFG, MODEL)
    assert total(charges) == pytest.approx(PRICE)
    charges, st = compute_charges(T0 + 3660, st, [node()], [], CFG, MODEL)
    assert total(charges) == pytest.approx(PRICE * 60 / 3600)


def test_open_pods_cover_their_share_and_the_rest_is_split_across_their_runs():
    pods = [OpenPod("a1", "n1", T0, 96, 0, 0, "run-a"), OpenPod("b1", "n1", T0, 32, 0, 0, "run-b")]
    charges, _ = compute_charges(T0 + 3600, {}, [node()], pods, CFG, MODEL)
    rest = PRICE * (1 - 128 / 192)
    by_run = {c["run"]: c["usd"] for c in charges if c["kind"] == "node"}
    assert by_run["run-a"] == pytest.approx(rest * 0.75) and by_run["run-b"] == pytest.approx(rest * 0.25)
    assert sum(by_run.values()) + MODEL.sandbox_usd(128, 0, 0, 3600) == pytest.approx(PRICE)


def test_a_fully_packed_node_still_gets_a_zero_record_that_marks_how_far_it_is_charged():
    pods = [OpenPod(f"p{i}", "n1", T0, 1, 0, 0, "r") for i in range(VCPUS)]
    charges, _ = compute_charges(T0 + 600, {}, [node()], pods, CFG, MODEL)
    recs = [c for c in charges if c["kind"] == "node"]
    assert len(recs) == 1 and recs[0]["usd"] == pytest.approx(0, abs=1e-12) and recs[0]["price_h"] == pytest.approx(PRICE)


def test_a_pod_opened_mid_interval_only_covers_its_part():
    st = {"version": 1, "last_pass": T0 + 3600, "nodes": {"n1": {"until": T0 + 3600, "price_h": PRICE}}}
    pods = [OpenPod("late", "n1", T0 + 3600 + 1800, 192, 0, 0, "r")]                   # whole node, for the second half hour
    charges, _ = compute_charges(T0 + 7200, st, [node()], pods, CFG, MODEL)
    assert total(charges) == pytest.approx(PRICE / 2)


def test_a_node_gone_between_close_passes_is_charged_up_to_now_and_dropped():
    st = {"version": 1, "last_pass": T0, "nodes": {"gone": {"until": T0, "price_h": PRICE}}}
    charges, new = compute_charges(T0 + 60, st, [], [], CFG, MODEL)
    assert total(charges) == pytest.approx(PRICE * 60 / 3600) and "gone" not in new["nodes"]
    assert all(c.get("gone") for c in charges if c["kind"] == "node")


def test_a_node_gone_while_the_meter_was_down_is_bounded_and_flagged_for_reconciliation():
    st = {"version": 1, "last_pass": T0, "nodes": {"gone": {"until": T0, "price_h": PRICE}}}
    charges, _ = compute_charges(T0 + 7200, st, [], [], CFG, MODEL)
    assert total(charges) == pytest.approx(PRICE * STALE_GAP_S / 3600)
    gap = [c for c in charges if c["kind"] == "meter_gap"]
    assert len(gap) == 1 and gap[0]["seconds"] == pytest.approx(7200 - STALE_GAP_S)
    assert gap[0]["max_usd"] == pytest.approx(PRICE * (7200 - STALE_GAP_S) / 3600)


def test_a_node_alive_across_a_meter_outage_is_charged_for_all_of_it():
    st = {"version": 1, "last_pass": T0, "nodes": {"n1": {"until": T0, "price_h": PRICE}}}
    charges, _ = compute_charges(T0 + 2 * 3600, st, [node()], [], CFG, MODEL)
    assert total(charges) == pytest.approx(2 * PRICE)


def test_the_cluster_fee_starts_at_cluster_creation_and_is_then_charged_per_pass():
    cfg = MeterConfig(machine_prices=CFG.machine_prices, disk_gb_month=0.0, cluster_fee_hour=0.10)
    charges, st = compute_charges(T0 + 3600, {}, [], [], cfg, MODEL, cluster_created=T0 - 3600)
    assert total(charges, "cluster_fee") == pytest.approx(0.20)
    charges, st = compute_charges(T0 + 7200, st, [], [], cfg, MODEL, cluster_created=None)
    assert total(charges, "cluster_fee") == pytest.approx(0.10)


def test_prices_spot_on_demand_disk_and_unknown_machine_fallback(capsys):
    cfg = MeterConfig(machine_prices={"c4d-standard-192": {"spot": 3.6, "ondemand": 9.0}},
                      fallback_vcpu_hour={"spot": 0.01, "ondemand": 0.04}, disk_gb_month=73.0, pool_disk_gb={"big": 10},
                      default_disk_gb=1)
    assert node_price_hour(node(spot=True), cfg) == pytest.approx(3.6 + 1 * 73.0 / 730)
    assert node_price_hour(node(spot=False), cfg) == pytest.approx(9.0 + 0.1)
    assert node_price_hour(node(pool="big"), cfg) == pytest.approx(3.6 + 1.0)
    assert node_price_hour(node(machine="n9-weird-8", vcpus=8, spot=False), cfg) == pytest.approx(8 * 0.04 + 0.1)
    assert "no ondemand price for 'n9-weird-8'" in capsys.readouterr().err


def folded_events(lives: dict, flaps: dict, t: float) -> list:
    """What list_event_for_all_namespaces returns at `t`: per node and reason ONE event (repeats fold into it: first and last
    timestamps), dropped an hour after its last update."""
    out = []
    for name, ls in lives.items():
        regs = [b for b, _ in ls if b <= t]
        rems = sorted([d for _, d in ls if d <= t] + [f for f in flaps.get(name, []) if f <= t])
        born = reborn = died = None
        if regs and t - regs[-1] <= 3600:
            reborn, born = regs[-1], regs[-1]
            for prev in reversed(regs[:-1]):                          # the folded event goes back while it was kept alive
                if born - prev > 3600:
                    break
                born = prev
        if rems and t - rems[-1] <= 3600:
            died = rems[-1]
        if born is not None or died is not None:
            out.append({"name": name, "born": born, "reborn": reborn, "died": died, "pool": "sandbox-spot"})
    return out


def random_history(rng, outages: bool):
    """Up to 10 node names, each with up to 4 lives (a managed instance group recreates a preempted VM under its old name), lives
    from 1 s (spot preemption) to 2.5 h, NotReady flaps, passes 1..300 s apart and, with `outages`, sometimes 1-2.5 h apart."""
    lives, flaps = {}, {}
    for i in range(rng.randint(1, 10)):
        name, t = f"gke-c-sandbox-spot-{i:04d}", T0 + rng.uniform(0, 8000)
        lives[name] = []
        for _ in range(rng.choice([1, 1, 2, 3, 4])):
            d = t + rng.choice([rng.uniform(1, 60), rng.uniform(5, 400), rng.uniform(60, 9000)])
            lives[name].append((t, d))
            flaps[name] = flaps.get(name, []) + [rng.uniform(t, d) for _ in range(rng.choice([0, 0, 1, 2]))]
            t = d + rng.choice([rng.uniform(1, 60), rng.uniform(20, 2000)])
    passes, t = [], T0
    while t < T0 + 30000:
        t += rng.uniform(4000, 9000) if outages and rng.random() < 0.05 else rng.uniform(1, STALE_GAP_S)
        passes.append(t)
    return lives, flaps, passes


@pytest.mark.parametrize("outages", [False, True])
def test_ledger_never_under_counts_random_node_histories(outages):
    """Every life is charged in full or flagged (a meter_gap whose max_usd covers it), except a life no meter can know of: born
    and gone inside an outage, its registration event expired (billing reconciliation's job). Without outages, at most one
    pass interval (plus the gap between lives) too much per life. A 20 000-history version of this ran clean."""
    rng = random.Random(11 if outages else 7)
    for trial in range(400):
        lives, flaps, passes = random_history(rng, outages)
        st, charged, flagged = {}, {}, {}
        for t in passes:
            alive = [node(n, created=b) for n, ls in lives.items() for b, d in ls if b <= t < d]
            charges, st = compute_charges(t, st, alive, [], CFG, MODEL, ephemeral=folded_events(lives, flaps, t))
            for c in charges:
                if c["kind"] == "node":
                    charged[c["label"]] = charged.get(c["label"], 0.0) + c["usd"]
                elif c["kind"] == "meter_gap":
                    flagged[c["label"]] = flagged.get(c["label"], 0.0) + (c.get("max_usd") or float("inf"))
        end = passes[-1]

        def knowable(n, b, d):
            if any(b <= p < d for p in passes):
                return True
            nxt = next((p for p in passes if p >= b), None)
            evs = folded_events({n: lives[n]}, {n: flaps.get(n, [])}, nxt) if nxt else []
            return bool(evs) and evs[0]["born"] is not None and evs[0]["born"] <= b <= evs[0]["reborn"]
        for n, ls in lives.items():
            known = [(b, d) for b, d in ls if not outages or knowable(n, b, d)]
            life = sum(max(0.0, min(d, end) - b) for b, d in known if b <= end) * PRICE / 3600
            got = charged.get(n, 0.0)
            assert got + flagged.get(n, 0.0) >= life - 1e-9, (trial, n, got, life, ls)
            if not outages:
                assert got <= life + len(ls) * PRICE * (STALE_GAP_S + 2000) / 3600 + 1e-9, (trial, n, got, life, ls)


def test_a_node_born_and_gone_between_passes_is_charged_from_its_events():
    st = {"version": 1, "last_pass": T0, "nodes": {"gke-c-sandbox-spot-a": {"until": T0, "price_h": PRICE, "pool": "sandbox-spot"}}}
    alive = [node("gke-c-sandbox-spot-a", created=T0 - 600)]
    ev = [{"name": "gke-c-sandbox-spot-b", "born": T0 + 30, "died": T0 + 150, "pool": "sandbox-spot"}]
    charges, st = compute_charges(T0 + 200, st, alive, [], CFG, MODEL, ephemeral=ev)
    b = [c for c in charges if c["label"] == "gke-c-sandbox-spot-b"]
    assert len(b) == 1 and b[0]["usd"] == pytest.approx(PRICE * 120 / 3600) and b[0]["gone"]
    charges, st = compute_charges(T0 + 260, st, alive, [], CFG, MODEL, ephemeral=ev)          # same events next pass
    assert not [c for c in charges if c["label"] == "gke-c-sandbox-spot-b"]                     # not charged twice


def test_an_event_only_node_with_no_removal_event_is_charged_up_to_now_at_its_pools_price():
    other = MeterConfig(machine_prices={"c4d-standard-192": {"spot": PRICE, "ondemand": 9.0}, "e2-standard-4": {"spot": 0.04}},
                        disk_gb_month=0.0, cluster_fee_hour=0.0)
    st = {"version": 1, "last_pass": T0, "nodes": {"gke-c-system-x": {"until": T0, "price_h": 0.04, "pool": "system"}}}
    ev = [{"name": "gke-c-sandbox-spot-z", "born": T0 + 10, "died": None, "pool": "sandbox-spot"}]
    charges, _ = compute_charges(T0 + 70, st, [node("gke-c-system-x", machine="e2-standard-4", pool="system")], [], other, MODEL,
                                 ephemeral=ev)
    z = [c for c in charges if c["label"] == "gke-c-sandbox-spot-z"][0]
    assert z["price_h"] == pytest.approx(9.0)          # unknown pool price: the most expensive price we know
    assert z["usd"] == pytest.approx(9.0 * 60 / 3600)


def test_events_are_read_from_the_api_and_node_pools_come_from_node_names(env):
    api, led_path = env
    api.nodes = [fk.make_node("gke-c-sandbox-spot-alive", T0)]
    api.events = [fk.node_event("gke-c-sandbox-spot-flash", "RegisteredNode", T0 + 100),
                  fk.node_event("gke-c-sandbox-spot-flash", "NodeNotReady", T0 + 150),
                  fk.node_event("gke-c-sandbox-spot-flash", "RemovingNode", T0 + 160, last=T0 + 170)]
    meter_once(api, now=T0 + 3600, force=True, cfg=CFG, ledger_path=led_path)
    recs = {r["label"]: r for r in records(led_path) if r.get("kind") == "node"}
    assert recs["gke-c-sandbox-spot-flash"]["usd"] == pytest.approx(PRICE * 70 / 3600)     # registration to the LAST removal event
    assert recs["gke-c-sandbox-spot-alive"]["usd"] == pytest.approx(PRICE)


def test_unreadable_events_warn_and_do_not_stop_the_meter(env, capsys):
    api, led_path = env
    api.events_fail = True
    assert meter_once(api, now=T0 + 3600, force=True, cfg=CFG, ledger_path=led_path) == pytest.approx(PRICE)
    assert "cannot read node events" in capsys.readouterr().err


# -- with the API, the ledger and the state file ---------------------------------------------------------------------------------
@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("SANDBOX_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(tmp_path / "ledger.jsonl"))
    monkeypatch.setenv("GKE_NAMESPACE", "sandboxes")
    monkeypatch.setattr(gke_meter, "cost_model_for", lambda p, o=None: MODEL)
    api = fk.FakeCore()
    api.cluster_created = T0
    api.nodes = [fk.make_node("n1", T0)]
    return api, tmp_path / "ledger.jsonl"


def records(path):
    return [json.loads(l) for l in Path(path).read_text().splitlines()] if Path(path).exists() else []


def node_usd(path):
    return sum(r["usd"] for r in records(path) if r.get("ev") == "storage" and r.get("kind") == "node")


def test_pods_plus_meter_add_up_to_the_node_bill_end_to_end(env):
    """Two runs' pods with real ledger entries for part of the time, metered every minute: the ledger total for the
    provider equals node price x time."""
    api, led_path = env
    led = CostLedger(led_path, run="")
    pods = {"pod-a": ("run-a", 64, T0 + 600, T0 + 3000), "pod-b": ("run-b", 32, T0 + 1200, T0 + 3600)}
    entries = {}
    for t in range(int(T0) + 60, int(T0) + 3601, 60):
        for sid, (run, cpu, start, end) in pods.items():
            if start <= t and sid not in entries:
                api.pods[sid] = fk.make_pod({"metadata": {"name": sid, "labels": {"sandboxes.managed": "true"}}, "spec": {}}, start)
                api.pods[sid].spec.node_name = "n1"
                h = f"h-{sid}"
                led._append({"t": start, "ev": "open", "id": h, "provider": "gke", "run": run, "sid": sid, "cpu": cpu,
                             "ram_gib": 0, "disk_gib": 0, "labels": {}})
                entries[sid] = h
            if end <= t and sid in api.pods:
                del api.pods[sid]
                led._append({"t": end, "ev": "close", "id": entries[sid], "provider": "gke", "run": run,
                             "seconds": end - start, "usd": MODEL.sandbox_usd(cpu, 0, 0, end - start)})
        meter_once(api, now=float(t), force=True, cfg=CFG, ledger_path=led_path)
    s = summary(led_path)["providers"]["gke"]
    assert s["total_usd"] == pytest.approx(PRICE * 1.0, rel=0.02)            # one node for one hour
    by_run = {r: summary(led_path, run=r)["total_usd"] for r in ("run-a", "run-b", "")}
    assert by_run["run-a"] > by_run["run-b"] > 0 and sum(by_run.values()) == pytest.approx(s["total_usd"], rel=1e-6)


def test_concurrent_meters_charge_once(env):
    api, led_path = env
    results = []
    barrier = threading.Barrier(8)

    def go():
        barrier.wait()
        results.append(meter_once(api, now=T0 + 3600, force=True, cfg=CFG, ledger_path=led_path))
    threads = [threading.Thread(target=go) for _ in range(8)]
    [t.start() for t in threads]; [t.join() for t in threads]
    assert sum(1 for r in results if r > 0) == 1
    assert node_usd(led_path) == pytest.approx(PRICE)


def test_passes_closer_than_the_minimum_interval_are_skipped(env):
    api, led_path = env
    assert meter_once(api, now=T0 + 3600, cfg=CFG, ledger_path=led_path) > 0
    assert meter_once(api, now=T0 + 3605, cfg=CFG, ledger_path=led_path) == 0.0
    assert meter_once(api, now=T0 + 3660, cfg=CFG, ledger_path=led_path) == pytest.approx(PRICE / 60)


@pytest.mark.parametrize("damage", ["delete", "corrupt", "wrong-shape"])
def test_a_lost_or_corrupt_state_file_is_rebuilt_from_the_ledger_not_recharged(env, damage):
    api, led_path = env
    meter_once(api, now=T0 + 3600, force=True, cfg=CFG, ledger_path=led_path)
    sp = gke_meter.state_path()
    if damage == "delete":
        sp.unlink()
    elif damage == "corrupt":
        sp.write_text("{not json")
    else:
        sp.write_text(json.dumps({"nodes": []}))
    added = meter_once(api, now=T0 + 3660, force=True, cfg=CFG, ledger_path=led_path)
    assert added == pytest.approx(PRICE / 60)                       # only the new minute, not the hour again
    assert node_usd(led_path) == pytest.approx(PRICE * 61 / 60)


def test_a_rebuilt_state_does_not_resurrect_a_node_that_was_already_charged_as_gone(env):
    api, led_path = env
    api.events = [fk.node_event("n1", "RegisteredNode", T0), fk.node_event("n1", "RemovingNode", T0 + 3630)]
    meter_once(api, now=T0 + 3600, force=True, cfg=CFG, ledger_path=led_path)
    api.nodes = []
    meter_once(api, now=T0 + 3660, force=True, cfg=CFG, ledger_path=led_path)    # n1 charged up to its disappearance
    gke_meter.state_path().unlink()
    before = node_usd(led_path)
    meter_once(api, now=T0 + 3720, force=True, cfg=CFG, ledger_path=led_path)    # its events are still there: not charged again
    assert node_usd(led_path) == pytest.approx(before)


def test_a_crash_between_ledger_and_state_over_charges_and_never_under(env, monkeypatch):
    api, led_path = env
    monkeypatch.setattr(gke_meter, "save_state", lambda s: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError):
        meter_once(api, now=T0 + 3600, force=True, cfg=CFG, ledger_path=led_path)
    monkeypatch.undo()
    monkeypatch.setenv("SANDBOX_CACHE_DIR", str(Path(led_path).parent / "cache"))
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(led_path))
    monkeypatch.setattr(gke_meter, "cost_model_for", lambda p, o=None: MODEL)
    meter_once(api, now=T0 + 3660, force=True, cfg=CFG, ledger_path=led_path)       # rebuilt from the ledger records
    assert node_usd(led_path) == pytest.approx(PRICE * 61 / 60)


def test_only_pods_with_an_open_entry_count_as_covered(env):
    """A pod whose ledger entry is closed (deleted from our side but still running: a leak) is charged by the meter."""
    api, led_path = env
    led = CostLedger(led_path, run="r")
    for sid in ("tracked", "leaked"):
        api.pods[sid] = fk.make_pod({"metadata": {"name": sid, "labels": {"sandboxes.managed": "true"}}, "spec": {}}, T0)
        api.pods[sid].spec.node_name = "n1"
        led._append({"t": T0, "ev": "open", "id": f"h-{sid}", "provider": "gke", "run": "r", "sid": sid, "cpu": 96,
                     "ram_gib": 0, "disk_gib": 0, "labels": {}})
    led._append({"t": T0, "ev": "close", "id": "h-leaked", "provider": "gke", "run": "r", "seconds": 0, "usd": 0})
    meter_once(api, now=T0 + 3600, force=True, cfg=CFG, ledger_path=led_path)
    assert node_usd(led_path) == pytest.approx(PRICE / 2)           # half the node is covered by the tracked pod's entry


def test_a_known_node_gone_during_an_outage_is_charged_to_its_removal_event():
    st = {"version": 1, "last_pass": T0, "nodes": {"n1": {"until": T0, "price_h": PRICE, "pool": "sandbox-spot"}}}
    ev = [{"name": "n1", "born": None, "died": T0 + 1800, "pool": "sandbox-spot"}]
    charges, st = compute_charges(T0 + 3000, st, [], [], CFG, MODEL, ephemeral=ev)          # 50 min gap: the meter was down
    n1 = [c for c in charges if c["label"] == "n1"]
    assert [c["kind"] for c in n1] == ["node"] and n1[0]["usd"] == pytest.approx(PRICE * 1800 / 3600)


def test_a_stale_removal_event_does_not_shorten_a_known_nodes_charge():
    st = {"version": 1, "last_pass": T0, "nodes": {"n1": {"until": T0, "price_h": PRICE, "pool": "sandbox-spot"}}}
    ev = [{"name": "n1", "born": None, "died": T0 - 600, "pool": "sandbox-spot"}]           # NotReady before, then came back
    charges, _ = compute_charges(T0 + 3000, st, [], [], CFG, MODEL, ephemeral=ev)
    kinds = {c["kind"]: c for c in charges if c["label"] == "n1"}
    assert kinds["node"]["usd"] == pytest.approx(PRICE * STALE_GAP_S / 3600) and kinds["meter_gap"]["max_usd"] > 0


def test_a_node_never_seen_with_only_a_removal_event_is_flagged_once_for_reconciliation():
    ev = [{"name": "ghost", "born": None, "died": T0 + 10, "pool": "sandbox-spot"}]
    charges, st = compute_charges(T0 + 60, {"version": 1, "last_pass": T0, "nodes": {}}, [], [], CFG, MODEL, ephemeral=ev)
    assert [(c["kind"], c["usd"]) for c in charges if c["label"] == "ghost"] == [("meter_gap", 0.0)]
    charges, _ = compute_charges(T0 + 120, st, [], [], CFG, MODEL, ephemeral=ev)
    assert not [c for c in charges if c["label"] == "ghost"]


def test_a_name_charged_as_gone_and_registered_again_is_charged_for_its_new_life_only():
    st = {"version": 1, "last_pass": T0, "nodes": {"n1": {"until": T0, "price_h": PRICE, "pool": "sandbox-spot"}}}
    charges, st = compute_charges(T0 + 60, st, [], [], CFG, MODEL)                          # gone: charged to T0+60
    ev = [{"name": "n1", "born": T0 + 100, "reborn": T0 + 100, "died": T0 + 160, "pool": "sandbox-spot"}]
    charges, st = compute_charges(T0 + 200, st, [], [], CFG, MODEL, ephemeral=ev)           # second life between passes
    assert sum(c["usd"] for c in charges if c["label"] == "n1") == pytest.approx(PRICE * 60 / 3600)   # at its pool's price
    ev = [{"name": "n1", "born": T0 + 100, "reborn": T0 + 300, "died": T0 + 160, "pool": "sandbox-spot"}]
    charges, st = compute_charges(T0 + 400, st, [node("n1", created=T0 + 300)], [], CFG, MODEL, ephemeral=ev)   # third, alive
    assert sum(c["usd"] for c in charges if c["label"] == "n1") == pytest.approx(PRICE * 200 / 3600)
    # registrations fold into one event: it cannot say whether a life began between the last pass and this node's creation,
    # so the node is charged from the last pass (at most one interval too much), never from its creation alone


def test_a_folded_registration_older_than_the_gone_time_charges_from_the_gone_time():
    st = {"version": 1, "last_pass": T0, "nodes": {"n1": {"until": T0, "price_h": PRICE, "pool": "sandbox-spot"}}}
    charges, st = compute_charges(T0 + 60, st, [], [], CFG, MODEL)
    ev = [{"name": "n1", "born": T0 - 3000, "reborn": T0 + 100, "died": T0 + 160, "pool": "sandbox-spot"}]
    charges, st = compute_charges(T0 + 200, st, [], [], CFG, MODEL, ephemeral=ev)
    assert sum(c["usd"] for c in charges if c["label"] == "n1") == pytest.approx(PRICE * 100 / 3600)


def test_after_a_close_out_the_cluster_fee_is_not_charged_again_from_the_clusters_creation():
    st = {"version": 1, "last_pass": T0, "nodes": {}, "gone": {}, "cluster_until": None, "cluster_closed": T0}
    cfg = MeterConfig(machine_prices=CFG.machine_prices, disk_gb_month=0.0, cluster_fee_hour=0.10)
    charges, st = compute_charges(T0 + 3600, st, [], [], cfg, MODEL, cluster_created=T0 - 86400)
    assert sum(c["usd"] for c in charges if c["kind"] == "cluster_fee") == pytest.approx(0.10)
