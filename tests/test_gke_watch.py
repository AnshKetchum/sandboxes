"""The process's pod watch: starts wake on their pod's change instead of polling the API server (a 1,000-sandbox ramp polling
every 2 s is ~500 requests/s), fall back to reading when the watch is down, and ride out a throttled API server (429s)."""

from __future__ import annotations

import queue
import threading
import time

import pytest

from sandboxes.backends import gke_raw
from sandboxes.backends.gke_raw import GKERaw

import gke_fakes as fk
from test_gke_raw import k8s, run, spec  # noqa: F401  (k8s is a fixture, used by name)


class FakeWatch:
    """kubernetes.watch.Watch: stream() yields what the test puts on `events` (None ends the stream)."""
    events: "queue.Queue" = queue.Queue()
    streams = 0
    fail = False

    def stream(self, func, *args, **kw):
        FakeWatch.streams += 1
        if FakeWatch.fail:
            raise ConnectionError("watch refused")
        while True:
            try:
                ev = FakeWatch.events.get(timeout=0.05)
            except queue.Empty:
                continue
            if ev is None:
                return
            yield ev


@pytest.fixture
def watched(k8s, monkeypatch):
    import kubernetes.watch
    FakeWatch.events, FakeWatch.streams, FakeWatch.fail = queue.Queue(), 0, False
    monkeypatch.setattr(kubernetes.watch, "Watch", FakeWatch)
    monkeypatch.setenv("GKE_POD_WATCH", "1")
    monkeypatch.setattr(gke_raw, "_watch", None)
    yield k8s
    if gke_raw._watch is not None:
        gke_raw._watch.close()
        FakeWatch.events.put(None)
        gke_raw._watch.thread.join(timeout=5)


def wait_for(cond, timeout=5.0):
    t = time.time()
    while not cond():
        assert time.time() - t < timeout, "condition not met"
        time.sleep(0.01)


def test_a_start_wakes_on_its_pods_change_not_on_a_poll(watched, monkeypatch):
    monkeypatch.setattr(gke_raw, "POLL_S", 30.0)            # a poll would take 30 s: only the watch can make this fast
    watched.on_create = None                                 # the pod stays Pending until the "kubelet" below runs it
    w = gke_raw.pod_watch()
    wait_for(lambda: w.alive)

    def kubelet():
        wait_for(lambda: watched.pods)
        pod = next(iter(watched.pods.values()))
        FakeWatch.events.put({"type": "ADDED", "object": pod})
        time.sleep(0.3)
        fk.set_ready(pod)
        FakeWatch.events.put({"type": "MODIFIED", "object": pod})
    threading.Thread(target=kubelet, daemon=True).start()
    t = time.time()
    sb = GKERaw(spec()); run(sb.start())
    assert time.time() - t < 5
    assert watched.reads.get(sb.sandbox_id, 0) <= 1           # at most the one read before the watch had the pod
    run(sb.stop())


def test_a_deleted_pod_leaves_the_watchs_state(watched):
    w = gke_raw.pod_watch()
    wait_for(lambda: w.alive)
    sb = GKERaw(spec()); run(sb.start())
    pod = watched.pods[sb.sandbox_id]
    FakeWatch.events.put({"type": "ADDED", "object": pod})
    wait_for(lambda: w.get(sb.sandbox_id) is not None)
    FakeWatch.events.put({"type": "DELETED", "object": pod})
    wait_for(lambda: w.get(sb.sandbox_id) is None)
    run(sb.stop())


def test_without_a_working_watch_starts_poll_as_before(watched, monkeypatch):
    FakeWatch.fail = True
    w = gke_raw.pod_watch()
    wait_for(lambda: w.failures >= 1 and not w.alive)
    reads = {"n": 0}

    def on_read(pod, n):
        reads["n"] = n
        if n >= 3:
            fk.set_ready(pod)
    watched.on_create, watched.on_read = None, on_read
    sb = GKERaw(spec()); run(sb.start())
    assert reads["n"] == 3                                   # polled until ready
    run(sb.stop())


def test_a_throttled_api_server_does_not_fail_a_start(k8s, monkeypatch):
    real, calls = k8s.read_namespaced_pod, {"n": 0}

    def flaky(name, namespace):
        calls["n"] += 1
        if calls["n"] <= 3:
            raise fk.FakeApiError(429, "Too Many Requests")
        return real(name, namespace)
    monkeypatch.setattr(k8s, "read_namespaced_pod", flaky)
    monkeypatch.setattr(gke_raw.asyncio, "sleep", _no_sleep)
    sb = GKERaw(spec()); run(sb.start())
    assert sb.sandbox_id and calls["n"] == 4
    run(sb.stop())


def test_a_start_whose_pod_cannot_be_read_at_all_gives_up_and_deletes_it(k8s, monkeypatch):
    k8s.reads_fail_with = 503
    monkeypatch.setattr(gke_raw.asyncio, "sleep", _no_sleep)
    sb = GKERaw(spec(create_tries=1))
    with pytest.raises(Exception):
        run(sb.start())
    assert not k8s.pods                                      # the pod it created is deleted on the way out


async def _no_sleep(s):
    return None
