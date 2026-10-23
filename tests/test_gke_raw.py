"""The GKE backend against an in-memory Kubernetes API: every way a pod can be created, fail, finish or be deleted, checked
for the two things that cost money when they go wrong: a pod left behind, and a ledger entry that does not match reality.
Commands run on this machine through the same argv the pod would get (tests/gke_fakes.local_exec)."""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

import pytest

from sandboxes import fleet
from sandboxes.backends import gke_raw
from sandboxes.backends.gke_raw import GKERaw, exec_argv, k8s_label_key, k8s_label_value, pod_manifest, pod_start_state
from sandboxes.cost import summary
from sandboxes.errors import CapacityError, ImageBuildError, SandboxCreateError, SandboxError
from sandboxes.pricing import cost_model_for
from sandboxes.raw import SandboxSpec

import gke_fakes as fk


class FastAsyncio:
    """asyncio, with every sleep cut to at most 10 ms (for the module under test only)."""

    def __getattr__(self, name):
        return getattr(asyncio, name)

    @staticmethod
    async def sleep(seconds, result=None):
        return await asyncio.sleep(min(seconds, 0.01), result)


@pytest.fixture
def k8s(tmp_path, monkeypatch):
    """A fake cluster wired into the backend, with this test's own cache dir and ledger."""
    monkeypatch.setenv("SANDBOX_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(tmp_path / "ledger.jsonl"))
    monkeypatch.setenv("SANDBOX_RATELIMIT_DIR", str(tmp_path / "rl"))
    monkeypatch.setenv("SANDBOX_HEALTH_DIR", str(tmp_path / "health"))
    monkeypatch.setenv("GKE_NAMESPACE", "sandboxes")
    monkeypatch.setenv("SANDBOX_GKE_CONFIG", str(tmp_path / "no-gke-config.yaml"))
    from sandboxes import cost as cost_mod, guard
    for k in (cost_mod.SESSION_ENV, cost_mod.RUN_ENV, "SANDBOX_SESSION_CLIENT", "SANDBOX_CAP_USD", "SANDBOX_RUN_CAP_USD"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(guard, "CAPS_FILE", tmp_path / "caps.json")         # never the machine's real caps
    monkeypatch.setattr(guard, "_cache", {"t": 0.0, "key": None, "sum": None, "run_sum": None})
    monkeypatch.setattr(guard, "_CACHE_S", 0.0)                             # every admission sees the ledger as it is now
    api = fk.FakeCore(now=time.time)
    api.cluster_created = time.time() - 60
    monkeypatch.setattr(gke_raw, "_core", lambda: api)
    monkeypatch.setattr(gke_raw, "_stream_exec", fk.local_exec)
    monkeypatch.setattr(gke_raw, "POLL_S", 0.01)
    import sandboxes.raw as raw_mod                    # RawSandbox.start backs off 10 s after a failed attempt: not here
    monkeypatch.setattr(raw_mod, "asyncio", FastAsyncio())
    from sandboxes import fleet as fleet_mod
    monkeypatch.setattr(fleet_mod, "providers_available", lambda: ["gke"])
    api.ledger = tmp_path / "ledger.jsonl"
    return api


def spec(**kw) -> SandboxSpec:
    extra = kw.pop("extra", {})
    s = SandboxSpec(image=kw.pop("image", "us-central1-docker.pkg.dev/p/envs/task:1"), cpu=kw.pop("cpu", 1), mem_gb=kw.pop("mem_gb", 4),
                    disk_gb=kw.pop("disk_gb", 10), labels=kw.pop("labels", {}), lifetime_s=kw.pop("lifetime_s", 3600),
                    create_tries=kw.pop("create_tries", 1), extra=extra, **kw)
    return s


def run(coro):
    return asyncio.run(coro)


def ledger_events(api) -> list:
    p = Path(api.ledger)
    return [json.loads(l) for l in p.read_text().splitlines()] if p.exists() else []


def open_entries(api) -> set:
    ev = ledger_events(api)
    closed = {e["id"] for e in ev if e["ev"] == "close"}
    return {e["id"] for e in ev if e["ev"] == "open" and e["id"] not in closed}


# -- manifest and label rules ---------------------------------------------------------------------------------------------------
def test_manifest_has_a_hard_lifetime_no_token_no_restarts_and_exact_labels():
    s = spec(labels={"sandboxes.owner": "euphoria", "sandboxes.run": "lhg ab3/weird*run" + "x" * 80, "prefix": "t-1"}, lifetime_s=5400,
             extra={"node_selector": {"sandboxes.pool": "sandbox"}, "runtime_class": "gvisor",
                    "tolerations": [{"key": "sandbox", "operator": "Equal", "value": "true", "effect": "NoSchedule"}]})
    m = pod_manifest("sbx-1", s, "sandboxes")
    ps = m["spec"]
    assert ps["activeDeadlineSeconds"] == 5400 and ps["restartPolicy"] == "Never"
    assert ps["automountServiceAccountToken"] is False and ps["enableServiceLinks"] is False
    assert ps["nodeSelector"] == {"sandboxes.pool": "sandbox"} and ps["runtimeClassName"] == "gvisor" and ps["tolerations"]
    labels = m["metadata"]["labels"]
    assert labels["sandboxes.managed"] == "true" and labels["sandboxes.owner"] == "euphoria"
    assert len(labels["sandboxes.run"]) <= 63 and all(c.isalnum() or c in "-_." for c in labels["sandboxes.run"])
    exact = json.loads(m["metadata"]["annotations"]["sandboxes/labels"])
    assert exact["sandboxes.run"].startswith("lhg ab3/weird*run")              # the annotation keeps the value as given
    res = ps["containers"][0]["resources"]
    assert res["requests"]["cpu"] == "0.95" and res["limits"]["cpu"] == "1.0" and res["limits"]["memory"] == "4Gi"
    assert res["limits"]["ephemeral-storage"] == "10Gi"


@pytest.mark.parametrize("raw,want", [("ok", "ok"), ("a b", "a-b"), ("-x-", "x"), ("é", ""), ("x" * 70, "x" * 63), ("a/b:c", "a-b-c")])
def test_label_values_are_made_valid(raw, want):
    assert k8s_label_value(raw) == want


def test_label_keys_keep_a_valid_prefix_and_fold_anything_else():
    assert k8s_label_key("sandboxes.managed") == "sandboxes.managed"
    assert k8s_label_key("example.com/tier") == "example.com/tier"
    assert k8s_label_key("Bad Prefix/name") == "name" or k8s_label_key("Bad Prefix/name").endswith("name")


# -- start states ----------------------------------------------------------------------------------------------------------------
def pod_with(setter, *a):
    p = fk.make_pod({"metadata": {"name": "p"}, "spec": {}}, 0)
    setter(p, *a)
    return p


def test_start_states():
    assert pod_start_state(pod_with(fk.set_ready), 10, 0)[0] == "ready"
    assert pod_start_state(pod_with(fk.set_unschedulable), 10, 0)[0] == "unschedulable"
    assert pod_start_state(pod_with(fk.set_finished, "Failed", "DeadlineExceeded"), 10, 0)[0] == "failed"
    assert pod_start_state(pod_with(fk.set_waiting, "ContainerCreating"), 10, 0)[0] == "pending"
    assert pod_start_state(pod_with(fk.set_waiting, "CrashLoopBackOff", "exit 127"), 10, 0)[0] == "failed"
    # pull errors: permanent ones at once, transient ones only after the grace period
    assert pod_start_state(pod_with(fk.set_waiting, "ErrImagePull", "manifest unknown"), 10, 0)[0] == "image_error"
    assert pod_start_state(pod_with(fk.set_waiting, "InvalidImageName"), 10, 0)[0] == "image_error"
    assert pod_start_state(pod_with(fk.set_waiting, "ImagePullBackOff", "i/o timeout"), 10, 0)[0] == "pending"
    assert pod_start_state(pod_with(fk.set_waiting, "ImagePullBackOff", "i/o timeout"), gke_raw.PULL_GRACE_S + 1, 0)[0] == "image_error"


# -- the happy path and its ledger -------------------------------------------------------------------------------------------------
def test_start_exec_stop_round_trip_records_one_open_and_one_close(k8s):
    sb = GKERaw(spec(cpu=2))
    run(sb.start())
    name = sb.sandbox_id
    assert name.startswith("sbx-") and name in k8s.pods
    r = run(sb.exec("echo hello; echo err >&2; exit 3"))
    assert r.rc == 3 and "hello" in r.stdout and "err" in r.stdout
    usd = run(sb.stop())
    assert name not in k8s.pods and k8s.deleted == [name]
    ev = ledger_events(k8s)
    opens = [e for e in ev if e["ev"] == "open"]; closes = [e for e in ev if e["ev"] == "close"]
    assert len(opens) == 1 and len(closes) == 1 and opens[0]["sid"] == name and opens[0]["provider"] == "gke"
    want = cost_model_for("gke").sandbox_usd(2, 4, 10, closes[0]["seconds"])     # the ledger rounds usd to 6 places, seconds to 2
    assert abs(closes[0]["usd"] - want) < 2e-6 and abs(usd - closes[0]["usd"]) < 2e-6
    assert open_entries(k8s) == set()


def test_stop_twice_and_kill_of_a_missing_pod_are_harmless(k8s):
    sb = GKERaw(spec())
    run(sb.start())
    run(sb.stop()); run(sb.stop())
    run(GKERaw.kill("sbx-never-existed"))
    assert open_entries(k8s) == set()


def test_pricing_overrides_are_refused_so_pods_and_meter_use_one_rate(k8s, capsys):
    from sandboxes.pricing import CostModel
    sb = GKERaw(spec(), CostModel("gke", vcpu_hour=9.0))
    assert sb.model == cost_model_for("gke")
    assert "ignoring a `pricing:` override" in capsys.readouterr().err


# -- every failed start leaves nothing behind ------------------------------------------------------------------------------------
def assert_nothing_left(k8s):
    assert k8s.pods == {}, f"pods left behind: {list(k8s.pods)}"
    assert open_entries(k8s) == set(), "a failed start left a ledger entry open"


def test_unschedulable_too_long_is_capacity_and_the_pod_is_deleted(k8s):
    k8s.on_create = fk.set_unschedulable
    with pytest.raises(CapacityError):
        run(GKERaw(spec(extra={"capacity_wait_s": 0.05})).start())
    assert_nothing_left(k8s)
    assert len(k8s.created) == 1 and k8s.deleted == k8s.created


def test_an_image_that_cannot_be_pulled_fails_once_and_is_deleted(k8s):
    k8s.on_create = lambda p: fk.set_waiting(p, "ErrImagePull", "manifest unknown")
    with pytest.raises(ImageBuildError):
        run(GKERaw(spec(create_tries=3)).start())
    assert_nothing_left(k8s)
    assert len(k8s.created) == 1                      # not retried: the same image fails the same way


def test_a_pod_that_fails_to_start_is_retried_and_every_attempt_is_deleted(k8s):
    k8s.on_create = lambda p: fk.set_waiting(p, "CrashLoopBackOff", "no /bin/sh")
    with pytest.raises(SandboxCreateError):
        run(GKERaw(spec(create_tries=3)).start())
    assert_nothing_left(k8s)
    assert len(k8s.created) == 3 and sorted(k8s.deleted) == sorted(k8s.created)


def test_not_ready_in_time_is_an_error_and_the_pod_is_deleted(k8s):
    k8s.on_create = lambda p: fk.set_waiting(p, "ContainerCreating")
    with pytest.raises(SandboxCreateError, match="not ready"):
        run(GKERaw(spec(extra={"ready_timeout_s": 0.05})).start())
    assert_nothing_left(k8s)


def test_a_create_whose_response_was_lost_still_deletes_the_pod(k8s):
    k8s.create_raises_after = True                     # the API made the pod, then the call failed
    with pytest.raises(SandboxCreateError):
        run(GKERaw(spec()).start())
    assert_nothing_left(k8s)


def test_cancelling_a_start_mid_wait_deletes_the_pod(k8s):
    k8s.on_create = fk.set_unschedulable               # waits for a node that never comes

    async def go():
        sb = GKERaw(spec(extra={"capacity_wait_s": 60}))
        task = asyncio.ensure_future(sb.start())
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.2)                       # the shielded delete runs to completion
    run(go())
    assert_nothing_left(k8s)


def test_a_pod_reaped_by_someone_else_mid_start_is_an_error_not_a_leak(k8s):
    def vanish(pod, n):
        if n == 2:
            del k8s.pods[pod.metadata.name]
    k8s.on_create = lambda p: fk.set_waiting(p, "ContainerCreating")
    k8s.on_read = vanish
    with pytest.raises(SandboxCreateError):
        run(GKERaw(spec()).start())
    assert_nothing_left(k8s)


def test_a_sandbox_without_an_image_is_refused_before_anything_is_created(k8s):
    with pytest.raises(ImageBuildError, match="registry image"):
        run(GKERaw(spec(image="")).start())
    assert k8s.created == []


# -- deletion when the API misbehaves ------------------------------------------------------------------------------------------
def test_delete_retries_through_api_errors(k8s, monkeypatch):
    monkeypatch.setattr(gke_raw.time, "sleep", lambda s: None)
    sb = GKERaw(spec()); run(sb.start()); name = sb.sandbox_id
    k8s.delete_failures = 2
    run(sb.stop())
    assert name not in k8s.pods


def test_a_pod_that_cannot_be_deleted_is_queued_for_the_watchdog_and_kill_raises(k8s, monkeypatch):
    monkeypatch.setattr(gke_raw.time, "sleep", lambda s: None)
    sb = GKERaw(spec()); run(sb.start()); name = sb.sandbox_id
    k8s.delete_failures = 100
    run(sb.stop())                                     # stop never raises; the pod is still there
    assert name in k8s.pods
    queued = [json.loads(l) for l in (Path(os.environ["SANDBOX_CACHE_DIR"]) / gke_raw.PENDING_DELETES).read_text().splitlines()]
    assert queued[-1]["name"] == name and queued[-1]["namespace"] == "sandboxes"
    with pytest.raises(SandboxError, match="could not delete"):
        run(GKERaw.kill(name))


# -- fleet: list, janitor, reconcile, reap, stop -------------------------------------------------------------------------------
def test_list_live_shows_only_ours_with_exact_labels_and_finished_pods(k8s):
    sb = GKERaw(spec(labels={"sandboxes.run": "r 1"})); run(sb.start())
    foreign = fk.make_pod({"metadata": {"name": "someone-else", "labels": {"app": "x"}}, "spec": {}}, time.time())
    k8s.pods["someone-else"] = foreign
    fk.set_finished(k8s.pods[sb.sandbox_id], "Failed", "DeadlineExceeded")
    live = run(GKERaw.list_live())
    assert [x.id for x in live] == [sb.sandbox_id]
    assert live[0].labels["sandboxes.run"] == "r 1" and live[0].state == "Failed:DeadlineExceeded"
    assert live[0].created_at and abs(live[0].created_at - time.time()) < 60


def test_list_live_follows_pages(k8s):
    for i in range(7):
        GKERaw(spec()); run(GKERaw(spec()).start())
    import sandboxes.backends.gke_raw as g
    calls = []
    orig = k8s.list_namespaced_pod

    def small_pages(ns, label_selector=None, limit=None, _continue=None):
        calls.append(_continue)
        return orig(ns, label_selector=label_selector, limit=3, _continue=_continue)
    k8s.list_namespaced_pod = small_pages
    assert len(run(g.GKERaw.list_live())) == 7 and len(calls) == 3


def test_a_preempted_pod_is_closed_in_the_ledger_by_reconcile(k8s):
    sb = GKERaw(spec()); run(sb.start()); name = sb.sandbox_id
    del k8s.pods[name]                                 # the node was preempted and the pod went with it
    assert fleet.reconcile(Path(k8s.ledger)) == 1
    assert open_entries(k8s) == set()
    closes = [e for e in ledger_events(k8s) if e["ev"] == "close"]
    assert closes[0]["note"].startswith("reconciled")


def test_janitor_sweep_kills_old_pods_and_reap_deletes_finished_ones(k8s):
    old = GKERaw(spec()); run(old.start())
    k8s.pods[old.sandbox_id].metadata.creation_timestamp = fk.ts(time.time() - 7 * 3600)
    done = GKERaw(spec()); run(done.start())
    fk.set_finished(k8s.pods[done.sandbox_id], "Failed", "DeadlineExceeded")
    fresh = GKERaw(spec()); run(fresh.start())
    found = run(fleet.sweep(6.0, ["gke"]))
    assert [n for n, _ in found] == [f"gke:{old.sandbox_id}"] and old.sandbox_id not in k8s.pods
    reaped = run(fleet.reap(providers=["gke"]))
    assert [sb.id for sb, why in reaped] == [done.sandbox_id] and done.sandbox_id not in k8s.pods
    assert set(k8s.pods) == {fresh.sandbox_id}
    assert {e["sid"] for e in ledger_events(k8s) if e["ev"] == "open" and e["id"] in open_entries(k8s)} == {fresh.sandbox_id}


def test_stop_by_run_deletes_that_run_only(k8s):
    from sandboxes.labels import standard_labels
    a = GKERaw(spec(labels={"sandboxes.run": "run-a"})); run(a.start())
    b = GKERaw(spec(labels={"sandboxes.run": "run-b"})); run(b.start())
    gone = run(fleet.stop(run="run-a", providers=["gke"]))
    assert [x.id for x in gone] == [a.sandbox_id] and set(k8s.pods) == {b.sandbox_id}


# -- exec and files ------------------------------------------------------------------------------------------------------------
def test_exec_honours_cwd_env_and_times_out(k8s, tmp_path):
    sb = GKERaw(spec()); run(sb.start())
    r = run(sb.exec("pwd; echo $GREETING", cwd=str(tmp_path), env={"GREETING": "hi there"}))
    assert r.rc == 0 and str(tmp_path) in r.stdout and "hi there" in r.stdout
    t0 = time.time()
    r = run(sb.exec("sleep 30", timeout=2))
    assert r.rc == -1 and "timed out" in r.stdout and time.time() - t0 < 15
    run(sb.stop())


def test_exec_whose_stream_ends_early_reports_it(k8s, monkeypatch):
    sb = GKERaw(spec()); run(sb.start())
    monkeypatch.setattr(gke_raw, "_stream_exec", lambda *a, **kw: fk.local_exec(*a, drop_stream=True, **kw))
    r = run(sb.exec("echo partial"))                   # no timeout given: still one direct stream, not detached mode
    assert r.rc == -1 and "partial" in r.stdout and "stream ended" in r.stdout
    run(sb.stop())


def test_only_commands_longer_than_two_hours_run_detached(k8s):
    sb = GKERaw(spec()); run(sb.start())
    calls = []
    orig = gke_raw._stream_exec

    def count(*a, **kw):
        calls.append(a[3])
        return orig(*a, **kw)
    gke_raw._stream_exec = count
    try:
        run(sb.exec("true"))
        run(sb.exec("true", timeout=7200))
        assert len(calls) == 2                         # one stream each, no script upload, no polling
    finally:
        gke_raw._stream_exec = orig
    run(sb.stop())


def test_exec_argv_wraps_in_timeout_and_falls_back_to_sh():
    argv = exec_argv("echo $X", "/w d", {"X": "a b"}, 30)
    assert argv[:2] == ["/bin/sh", "-c"] and "timeout -s KILL 30" in argv[2] and "SH=sh" in argv[2]


def test_files_and_directories_round_trip_byte_for_byte(k8s, tmp_path):
    sb = GKERaw(spec()); run(sb.start())
    blob = bytes(range(256)) * 4097 + b"\n\x00tail"               # binary, newlines, NULs, ~1 MB (more than one stdin chunk)
    src = tmp_path / "in.bin"; src.write_bytes(blob)
    remote = tmp_path / "remote" / "x.bin"
    run(sb.put_file(src, str(remote)))
    assert remote.read_bytes() == blob
    out = tmp_path / "out.bin"
    run(sb.get_file(str(remote), out))
    assert out.read_bytes() == blob
    d = tmp_path / "dir"; (d / "sub").mkdir(parents=True); (d / "sub" / "f.txt").write_text("deep"); (d / "empty.txt").write_text("")
    run(sb.put_file(d, str(tmp_path / "rdir")))
    run(sb.get_file(str(tmp_path / "rdir"), tmp_path / "back"))
    assert (tmp_path / "back" / "sub" / "f.txt").read_text() == "deep" and (tmp_path / "back" / "empty.txt").exists()
    run(sb.stop())


def test_a_truncated_upload_is_detected(k8s, tmp_path, monkeypatch):
    sb = GKERaw(spec()); run(sb.start())
    monkeypatch.setattr(gke_raw, "_stream_exec", lambda *a, **kw: fk.local_exec(*a, truncate_stdin_by=10, **kw))
    src = tmp_path / "f"; src.write_bytes(b"x" * 1000)
    with pytest.raises(SandboxError, match="upload of 1000 bytes"):
        run(sb.put_file(src, str(tmp_path / "dest")))
    run(sb.stop())


def test_downloading_a_missing_file_fails_cleanly(k8s, tmp_path):
    sb = GKERaw(spec()); run(sb.start())
    with pytest.raises(SandboxError):
        run(sb.get_file(str(tmp_path / "nope"), tmp_path / "x"))
    run(sb.stop())


def test_the_node_bill_counts_against_the_spend_cap_and_a_capped_start_creates_no_pod(k8s, tmp_path):
    from sandboxes.cost import SpendCapExceeded, summary
    (tmp_path / "caps.json").write_text(json.dumps({"total_usd": 3.0}))
    k8s.nodes = [fk.make_node("gke-c-sandbox-spot-0001", time.time() - 3600)]     # an hour of a 192-vCPU node: ~$3.84
    sb = GKERaw(spec()); run(sb.start())                                             # admitted at $0; its start meters the node
    assert summary(k8s.ledger)["total_usd"] >= 3.0
    with pytest.raises(SpendCapExceeded):
        run(GKERaw(spec()).start())
    assert len(k8s.created) == 1 and list(k8s.pods) == [sb.sandbox_id]
    run(sb.stop())
    assert not k8s.pods
