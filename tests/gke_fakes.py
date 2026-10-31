"""An in-memory Kubernetes CoreV1 API and a local exec stand-in, for testing the GKE backend's lifecycle, deletion and metering
without a cluster. Pods follow the manifest the backend sends; tests script what happens to them (scheduled, ready,
unschedulable, image errors, failures, preemption) and which API calls fail."""

from __future__ import annotations

import datetime as dt
import subprocess
from types import SimpleNamespace


class FakeApiError(Exception):
    def __init__(self, status: int, reason: str = ""):
        super().__init__(f"({status}) {reason}")
        self.status = status


def ts(epoch: float) -> dt.datetime:
    return dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)


def make_pod(manifest: dict, created: float):
    md = manifest["metadata"]
    return SimpleNamespace(
        metadata=SimpleNamespace(name=md["name"], namespace=md.get("namespace"), labels=dict(md.get("labels") or {}),
                                 annotations=dict(md.get("annotations") or {}), creation_timestamp=ts(created),
                                 deletion_timestamp=None, owner_references=None),
        spec=SimpleNamespace(node_name=None, manifest=manifest["spec"],
                             active_deadline_seconds=manifest["spec"].get("activeDeadlineSeconds"),
                             node_selector=dict(manifest["spec"].get("nodeSelector") or {}),
                             tolerations=list(manifest["spec"].get("tolerations") or []),
                             containers=[SimpleNamespace(image=c.get("image"), resources=SimpleNamespace(
                                 requests=dict((c.get("resources") or {}).get("requests") or {})))
                                 for c in manifest["spec"].get("containers") or []]),
        status=SimpleNamespace(phase="Pending", reason=None, message=None, conditions=[], container_statuses=[], start_time=None))


def set_ready(pod, node: str = "node-a") -> None:
    pod.spec.node_name = node
    pod.status.start_time = pod.status.start_time or pod.metadata.creation_timestamp
    pod.status.phase = "Running"
    pod.status.conditions = [SimpleNamespace(type="PodScheduled", status="True", reason=None, message=None)]
    pod.status.container_statuses = [SimpleNamespace(ready=True, state=SimpleNamespace(waiting=None, terminated=None, running=True))]


def set_unschedulable(pod, msg: str = "0/3 nodes are available: 3 Insufficient cpu.") -> None:
    pod.status.conditions = [SimpleNamespace(type="PodScheduled", status="False", reason="Unschedulable", message=msg)]


def set_waiting(pod, reason: str, msg: str = "", node: str = "node-a") -> None:
    pod.spec.node_name = node
    pod.status.conditions = [SimpleNamespace(type="PodScheduled", status="True", reason=None, message=None)]
    pod.status.container_statuses = [SimpleNamespace(ready=False, state=SimpleNamespace(
        waiting=SimpleNamespace(reason=reason, message=msg), terminated=None, running=None))]


def set_finished(pod, phase: str = "Failed", reason: str = "DeadlineExceeded") -> None:
    pod.status.phase = phase
    pod.status.reason = reason


def other_pod(name: str, node: str, namespace: str = "kube-system", owner_kind: str = "", phase: str = "Running", mirror=False):
    """A pod that is not a sandbox (a DaemonSet's, a static pod, someone's workload) on a node."""
    owners = [SimpleNamespace(kind=owner_kind, name=f"{name}-owner")] if owner_kind else None
    ann = {"kubernetes.io/config.mirror": "x"} if mirror else {}
    return SimpleNamespace(metadata=SimpleNamespace(name=name, namespace=namespace, labels={}, annotations=ann, owner_references=owners,
                                                    creation_timestamp=ts(0), deletion_timestamp=None),
                           spec=SimpleNamespace(node_name=node), status=SimpleNamespace(phase=phase, reason=None))


def make_node(name: str, created: float, machine: str = "c4d-standard-192", spot: bool = True, pool: str = "sandbox-spot",
              cpu: str = "192", allocatable: dict = None, ready: bool = True, sandbox_pool: str = None):
    labels = {"node.kubernetes.io/instance-type": machine, "cloud.google.com/gke-nodepool": pool}
    if spot:
        labels["cloud.google.com/gke-spot"] = "true"
    if sandbox_pool:
        labels["sandboxes.pool"] = sandbox_pool
    return SimpleNamespace(metadata=SimpleNamespace(name=name, labels=labels, creation_timestamp=ts(created)),
                           spec=SimpleNamespace(unschedulable=False),
                           status=SimpleNamespace(capacity={"cpu": cpu, "memory": "768Gi"},
                                                  allocatable=allocatable or {"cpu": cpu, "memory": "740Gi",
                                                                              "ephemeral-storage": "2000Gi", "pods": "256"},
                                                  conditions=[SimpleNamespace(type="Ready", status="True" if ready else "False")]))


class FakeCore:
    """The CoreV1Api calls the backend, meter and ops make. `on_create(pod)` and `on_read(pod, n)` script pod progress."""

    def __init__(self, now=None):
        self.pods: dict = {}
        self.nodes: list = []
        self.other_pods: list = []        # not ours: DaemonSets, system pods, other workloads
        self.events: list = []
        self.deleted: list = []
        self.created: list = []
        self.delete_failures = 0          # the next N deletes raise a 500
        self.create_raises_after = False  # the API creates the pod, then the call fails (a lost response)
        self.reads_fail_with = None       # status code every read raises
        self.on_create = set_ready        # default: ready at once
        self.on_read = None
        self.reads: dict = {}
        self.page_limit_seen = []
        self.now = now or (lambda: 1_000_000.0)
        self.cluster_created = self.now() - 60

    def create_namespaced_pod(self, namespace, body):
        name = body["metadata"]["name"]
        if name in self.pods:
            raise FakeApiError(409, "AlreadyExists")
        pod = make_pod(body, self.now())
        self.pods[name] = pod
        self.created.append(name)
        if self.on_create:
            self.on_create(pod)
        if self.create_raises_after:
            raise FakeApiError(500, "lost response")
        return pod

    def read_namespaced_pod(self, name, namespace):
        if self.reads_fail_with:
            raise FakeApiError(self.reads_fail_with, "read failed")
        pod = self.pods.get(name)
        if pod is None:
            raise FakeApiError(404, "NotFound")
        self.reads[name] = self.reads.get(name, 0) + 1
        if self.on_read:
            self.on_read(pod, self.reads[name])
        return pod

    def delete_namespaced_pod(self, name, namespace, grace_period_seconds=None, propagation_policy=None):
        if self.delete_failures > 0:
            self.delete_failures -= 1
            raise FakeApiError(500, "etcd timeout")
        if name not in self.pods:
            raise FakeApiError(404, "NotFound")
        del self.pods[name]
        self.deleted.append(name)

    def list_namespaced_pod(self, namespace, label_selector=None, limit=None, _continue=None):
        self.page_limit_seen.append(limit)
        key, _, value = (label_selector or "").partition("=")
        items = [p for p in self.pods.values() if not key or p.metadata.labels.get(key) == value]
        start = int(_continue or 0)
        end = start + limit if limit else len(items)
        nxt = str(end) if limit and end < len(items) else None
        return SimpleNamespace(items=items[start:end], metadata=SimpleNamespace(_continue=nxt))

    def list_pod_for_all_namespaces(self, field_selector=None):
        key, _, value = (field_selector or "").partition("=")
        pods = list(self.pods.values()) + list(self.other_pods)
        return SimpleNamespace(items=[p for p in pods if key != "spec.nodeName" or p.spec.node_name == value])

    def list_node(self, label_selector=None):
        if label_selector and "=" not in label_selector:                       # "key": nodes that carry the label
            return SimpleNamespace(items=[n for n in self.nodes if label_selector in (n.metadata.labels or {})])
        return SimpleNamespace(items=list(self.nodes))

    def read_namespace(self, name):
        return SimpleNamespace(metadata=SimpleNamespace(creation_timestamp=ts(self.cluster_created)))

    def list_event_for_all_namespaces(self, field_selector=None):
        if getattr(self, "events_fail", False):
            raise FakeApiError(403, "forbidden")
        return SimpleNamespace(items=list(self.events))


def node_event(name: str, reason: str, when: float, last: float = None):
    """A Kubernetes Event about a node (RegisteredNode, RemovingNode, NodeNotReady ...)."""
    return SimpleNamespace(involved_object=SimpleNamespace(kind="Node", name=name), reason=reason, event_time=None,
                           first_timestamp=ts(when), last_timestamp=ts(last if last is not None else when),
                           metadata=SimpleNamespace(creation_timestamp=ts(when)))


def local_exec(api, name, namespace, argv, *, stdin, timeout, stdout_only, truncate_stdin_by=0, drop_stream=False):
    """Run the argv the backend would send to the pod, here, on this machine: (rc, output bytes)."""
    if name not in api.pods:
        raise FakeApiError(404, "pod gone")
    data = stdin[:len(stdin) - truncate_stdin_by] if stdin is not None and truncate_stdin_by else stdin
    try:
        p = subprocess.run(argv, input=data if data is not None else b"", capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        return None, (e.stdout or b"")
    if drop_stream:
        return None, p.stdout
    rc = p.returncode if p.returncode >= 0 else 128 - p.returncode     # killed by signal N: a pod's exec reports 128+N
    return rc, p.stdout if stdout_only else p.stdout + p.stderr
