"""GKE: one Kubernetes pod per sandbox, on a GKE cluster's (spot) node pools. A pod starts from a REGISTRY image: GKE cannot
build a task's Dockerfile, so build and push first (e.g. `euphoria build` into Artifact Registry) and open the reference.

Deletion is layered so that no pod outlives its use, whatever dies:
  1. stop() deletes the pod (grace 0, retried); a 404 counts as deleted. A delete that still fails is queued in
     ~/.cache/sandboxes/gke_pending_deletes.jsonl for the GKE watchdog (gke_ops.py) to retry.
  2. every pod carries activeDeadlineSeconds = its lifetime, so the kubelet kills it there even when every process of ours died
  3. list_live/kill expose every pod with our labels (finished ones too) to the janitor, `sandboxes stop|reap` and the watchdog
  4. a create that fails at ANY point after the API call (unschedulable, image pull, timeout, cancellation) deletes the pod
     before raising: the pod name is fixed before the call, so even a create whose response was lost is cleaned up
Pods are not what Google bills; nodes are. The node bill goes into the cost ledger through gke_meter.py, and node pools are
scaled back to zero by gke_ops.py. Pod ledger entries carry the pod's share of a node (CostModel vcpu_hour), the meter adds
the rest, so pods + meter = what the nodes cost.

Environment (one value per machine: fleet operations such as the janitor see only a pod's id, so these are never per-config):
  GKE_NAMESPACE      namespace of every sandbox pod (default "sandboxes")
  GKE_KUBE_CONTEXT   kubeconfig context (default: the current one)
Configuration (the provider config's `extra:` mapping):
  node_selector     {...}              e.g. {"sandboxes.pool": "sandbox"}
  tolerations       [...]              e.g. the sandbox pool's taint and cloud.google.com/gke-spot
  runtime_class     "gvisor" or unset
  cpu_request       fraction of `cpu` requested (default 0.95, so N one-vCPU pods fit an N-vCPU node's allocatable)
  capacity_wait_s   how long a pod may stay unschedulable before CapacityError (default 900: covers a node scale-up)
  ready_timeout_s   how long a pod may take to become ready in total (default 1200: scale-up plus a large image pull)

Credentials: a kubeconfig (`gcloud container clusters get-credentials`), from KUBECONFIG or ~/.kube/config.
"""

from __future__ import annotations

import asyncio
import functools
import json
import os
import re
import shlex
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

from ..errors import CapacityError, ImageBuildError, SandboxCreateError, SandboxError
from ..pricing import cost_model_for
from ..raw import ExecResult, LiveSandbox, RawSandbox

MANAGED_SELECTOR = "sandboxes.managed=true"
LABELS_ANNOTATION = "sandboxes/labels"          # the exact labels (k8s label values are restricted to 63 safe characters)
CONTAINER = "sandbox"
# PID 1 of every pod: sleeps until deleted, and exits at once on SIGTERM (no 30 s grace wait).
KEEPALIVE = "trap 'exit 0' TERM INT; while :; do sleep 3600 & wait $!; done"
# A pull failure that will not fix itself; anything else gets PULL_GRACE_S to recover (a registry hiccup).
PERMANENT_PULL_HINTS = ("not found", "manifest unknown", "unauthorized", "denied", "invalid reference", "no such host")
IMAGE_WAIT_REASONS = ("ErrImagePull", "ImagePullBackOff", "InvalidImageName", "ErrImageNeverPull", "RegistryUnavailable")
START_FAILURE_REASONS = ("CreateContainerConfigError", "CreateContainerError", "CrashLoopBackOff", "RunContainerError")
PULL_GRACE_S = 180.0
POLL_S = 2.0
DELETE_TRIES = 4
STDIN_CHUNK = 1 << 20
MAX_OUTPUT_BYTES = 16 << 20                       # keep the tail of a runaway output, not all of it in memory
PENDING_DELETES = "gke_pending_deletes.jsonl"

_api = None
_api_lock = threading.Lock()
_executor: Optional[ThreadPoolExecutor] = None


# -- client (one per process; the kubernetes client is synchronous, so calls run on a dedicated thread pool) --------------------
def _core():
    global _api
    with _api_lock:
        if _api is None:
            from kubernetes import client, config
            config.load_kube_config(context=os.environ.get("GKE_KUBE_CONTEXT") or None)
            _api = client.CoreV1Api()
        return _api


def _pool() -> ThreadPoolExecutor:
    global _executor
    with _api_lock:
        if _executor is None:
            _executor = ThreadPoolExecutor(max_workers=int(os.environ.get("GKE_API_THREADS", "256")), thread_name_prefix="gke-api")
        return _executor


async def _call(fn, *args, **kw):
    return await asyncio.get_running_loop().run_in_executor(_pool(), functools.partial(fn, *args, **kw))


def _status(e: BaseException) -> Optional[int]:
    return getattr(e, "status", None)


# -- pure helpers (unit tested) ------------------------------------------------------------------------------------------------
def k8s_label_value(v) -> str:
    """A string Kubernetes accepts as a label value: <= 63 chars of [A-Za-z0-9_.-], alphanumeric at both ends."""
    s = re.sub(r"[^A-Za-z0-9_.-]", "-", str(v))[:63]
    return re.sub(r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", "", s)


def k8s_label_key(k) -> str:
    """A label key: an optional DNS prefix and a <= 63 char name; anything else is folded into the name."""
    k = str(k)
    prefix, _, name = k.rpartition("/")
    name = k8s_label_value(name) or "label"
    return f"{prefix}/{name}" if prefix and re.fullmatch(r"[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?", prefix) else name


def namespace() -> str:
    return os.environ.get("GKE_NAMESPACE") or "sandboxes"


def pod_manifest(name: str, spec, namespace: str) -> dict:
    """The pod for one sandbox. A hard lifetime (activeDeadlineSeconds), no service account token, no restarts."""
    x = spec.extra or {}
    labels = {k8s_label_key(k): k8s_label_value(v) for k, v in (spec.labels or {}).items()}
    labels = {k: v for k, v in labels.items() if v}
    labels["sandboxes.managed"] = "true"
    cpu_request = round(float(spec.cpu) * float(x.get("cpu_request", 0.95)), 3)
    resources = {"requests": {"cpu": str(cpu_request), "memory": f"{int(spec.mem_gb)}Gi", "ephemeral-storage": f"{int(spec.disk_gb)}Gi"},
                 "limits": {"cpu": str(float(spec.cpu)), "memory": f"{int(spec.mem_gb)}Gi", "ephemeral-storage": f"{int(spec.disk_gb)}Gi"}}
    pod_spec = {
        "restartPolicy": "Never",
        "activeDeadlineSeconds": int(spec.lifetime_s),
        "terminationGracePeriodSeconds": 5,
        "automountServiceAccountToken": False,
        "enableServiceLinks": False,
        "containers": [{"name": CONTAINER, "image": spec.image, "imagePullPolicy": "IfNotPresent",
                        "command": ["/bin/sh", "-c", KEEPALIVE],
                        "env": [{"name": str(k), "value": str(v)} for k, v in (spec.env or {}).items()],
                        "resources": resources}],
    }
    if x.get("node_selector"):
        pod_spec["nodeSelector"] = dict(x["node_selector"])
    if x.get("tolerations"):
        pod_spec["tolerations"] = list(x["tolerations"])
    if x.get("runtime_class"):
        pod_spec["runtimeClassName"] = str(x["runtime_class"])
    return {"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": namespace, "labels": labels,
                         "annotations": {LABELS_ANNOTATION: json.dumps(spec.labels or {}, sort_keys=True)}},
            "spec": pod_spec}


def pod_start_state(pod, now: float, created_at: float) -> tuple[str, str]:
    """Where a starting pod stands: ("ready" | "pending" | "unschedulable" | "image_error" | "failed", detail)."""
    st = pod.status
    phase = getattr(st, "phase", None) or "Pending"
    if phase in ("Failed", "Succeeded"):
        return "failed", f"phase {phase}: {getattr(st, 'reason', None) or ''} {getattr(st, 'message', None) or ''}".strip()
    for cs in getattr(st, "container_statuses", None) or []:
        waiting = getattr(getattr(cs, "state", None), "waiting", None)
        terminated = getattr(getattr(cs, "state", None), "terminated", None)
        if terminated is not None:
            return "failed", f"container exited ({getattr(terminated, 'reason', '')}, code {getattr(terminated, 'exit_code', '?')})"
        if waiting is None:
            continue
        reason, msg = getattr(waiting, "reason", "") or "", getattr(waiting, "message", "") or ""
        if reason in IMAGE_WAIT_REASONS:
            if reason in ("InvalidImageName", "ErrImageNeverPull") or any(h in msg.lower() for h in PERMANENT_PULL_HINTS) \
                    or now - created_at > PULL_GRACE_S:
                return "image_error", f"{reason}: {msg[:300]}"
            return "pending", f"{reason} (retrying)"
        if reason in START_FAILURE_REASONS:
            return "failed", f"{reason}: {msg[:300]}"
    if phase == "Running" and (getattr(st, "container_statuses", None) or []) \
            and all(getattr(cs, "ready", False) for cs in st.container_statuses):
        return "ready", "running"
    for c in getattr(st, "conditions", None) or []:
        if getattr(c, "type", "") == "PodScheduled" and getattr(c, "status", "") == "False" \
                and getattr(c, "reason", "") == "Unschedulable":
            return "unschedulable", (getattr(c, "message", "") or "")[:300]
    return "pending", phase


def exec_argv(command: str, cwd: Optional[str], env: Optional[dict], timeout: int) -> list:
    """`bash -lc command` (sh if the image has no bash) inside coreutils/busybox `timeout`, which kills it at `timeout`: the
    Kubernetes exec API has no deadline of its own and a dropped stream does not stop the process."""
    script = "".join(f"export {k}={shlex.quote(str(v))}; " for k, v in (env or {}).items())
    if cwd:
        script += f"cd {shlex.quote(cwd)} || exit 1; "
    script += command
    q = shlex.quote(script)
    t = max(1, int(timeout))
    return ["/bin/sh", "-c",
            f"if command -v bash >/dev/null 2>&1; then SH=bash; else SH=sh; fi; "
            f"if command -v timeout >/dev/null 2>&1; then exec timeout -s KILL {t} $SH -lc {q}; else exec $SH -lc {q}; fi"]


def epoch(ts) -> Optional[float]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return float(ts)
    if isinstance(ts, str):
        import datetime as dt
        try:
            ts = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            return None
    return ts.timestamp()


def labels_of(pod) -> dict:
    """The exact labels a sandbox was created with (the annotation), else its Kubernetes labels."""
    md = pod.metadata
    raw = (getattr(md, "annotations", None) or {}).get(LABELS_ANNOTATION)
    try:
        labels = json.loads(raw) if raw else None
    except ValueError:
        labels = None
    out = dict(getattr(md, "labels", None) or {})
    out.update(labels or {})
    return out


def live_state(pod) -> str:
    """Phase, plus the reason for a finished pod ("Failed:DeadlineExceeded"), plus "deleting" once a delete was accepted."""
    st = pod.status
    phase = getattr(st, "phase", None) or "Pending"
    reason = getattr(st, "reason", None)
    s = f"{phase}:{reason}" if reason else phase
    return s + (":deleting" if getattr(pod.metadata, "deletion_timestamp", None) else "")


# -- exec seam (the one place that talks to the exec websocket; tests replace it) ----------------------------------------------
def _stream_exec(api, name: str, namespace: str, argv: list, *, stdin: Optional[bytes], timeout: float,
                 stdout_only: bool) -> tuple[Optional[int], bytes]:
    """Run argv in the pod; returns (exit code, or None if the stream ended without one; output bytes)."""
    from kubernetes.stream import stream
    resp = stream(api.connect_get_namespaced_pod_exec, name, namespace, command=argv, container=CONTAINER,
                  stderr=True, stdin=stdin is not None, stdout=True, tty=False, _preload_content=False, binary=True)
    try:
        if stdin is not None:
            for i in range(0, len(stdin), STDIN_CHUNK):
                resp.write_stdin(stdin[i:i + STDIN_CHUNK])
        resp.run_forever(timeout=timeout)
        out = resp.read_stdout() if stdout_only else resp.read_all()
        out = out if isinstance(out, (bytes, bytearray)) else (out or "").encode()
        try:
            rc = resp.returncode
        except Exception:  # noqa: BLE001 — no status frame: the stream ended before the process did
            rc = None
        return rc, bytes(out)
    finally:
        try:
            resp.close()
        except Exception:  # noqa: BLE001
            pass


# -- deletion --------------------------------------------------------------------------------------------------------------------
def _pending_path() -> Path:
    from ..cost import cache_dir
    return cache_dir() / PENDING_DELETES


def queue_pending_delete(name: str, namespace: str, error: str) -> None:
    """A pod that could not be deleted: the watchdog retries it (its deadline is the backstop under that)."""
    p = _pending_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a") as f:
        f.write(json.dumps({"t": time.time(), "name": name, "namespace": namespace, "error": error[:300]}) + "\n")


def delete_pod_sync(api, name: str, namespace: str, tries: int = DELETE_TRIES) -> bool:
    """Delete a pod now (grace 0). True when it is deleted or already gone; retried on API errors (1, 2, 4 s apart)."""
    last = ""
    for attempt in range(tries):
        try:
            api.delete_namespaced_pod(name, namespace, grace_period_seconds=0, propagation_policy="Background")
            return True
        except Exception as e:  # noqa: BLE001
            if _status(e) == 404:
                return True
            last = f"{type(e).__name__}: {e}"
            if attempt < tries - 1:
                time.sleep(min(2 ** attempt, 8))
    print(f"[sandboxes] gke: could not delete pod {namespace}/{name} after {tries} tries: {last[:200]}", file=sys.stderr, flush=True)
    queue_pending_delete(name, namespace, last)
    return False


async def _delete_pod(name: str, namespace: str) -> bool:
    """Delete, shielded: a cancelled caller (a timed-out start, Ctrl-C) still lets the delete finish."""
    return await asyncio.shield(_call(delete_pod_sync, _core(), name, namespace))


class GKERaw(RawSandbox):
    provider = "gke"
    hard_lifetime = True            # activeDeadlineSeconds: the kubelet ends the pod at its lifetime
    # Commands stream directly up to 2 h (RawSandbox.exec's default timeout is 7200 s, so an exec without a timeout must not
    # fall into detached mode: that costs an upload and 3 s polling per command); anything longer runs detached and is polled.
    max_call_s = 7200

    def __init__(self, spec, model=None):
        # One rate for pods and the meter (the meter subtracts exactly what pod entries charge): config `pricing:` overrides
        # are refused, the rate comes from SANDBOX_GKE_VCPU_HOUR.
        default = cost_model_for("gke")
        if model is not None and model != default:
            print("[sandboxes] gke: ignoring a `pricing:` override (set SANDBOX_GKE_VCPU_HOUR instead; the node meter must "
                  "use the same rate)", file=sys.stderr, flush=True)
        super().__init__(spec, default)
        self._ns = namespace()

    # -- fleet -------------------------------------------------------------------------------------------------------------------
    @classmethod
    async def list_live(cls):
        """Every pod of ours in the namespace, finished ones included (they still hold the pod object; reap/sweep delete them)."""
        ns = namespace()
        out, token = [], None
        while True:
            kw = {"label_selector": MANAGED_SELECTOR, "limit": 500}
            if token:
                kw["_continue"] = token
            res = await _call(_core().list_namespaced_pod, ns, **kw)
            for p in res.items or []:
                out.append(LiveSandbox("gke", p.metadata.name, epoch(p.metadata.creation_timestamp), live_state(p), labels_of(p)))
            token = getattr(res.metadata, "_continue", None) if getattr(res, "metadata", None) else None
            if not token:
                return out

    @classmethod
    async def kill(cls, sandbox_id):
        """Delete one pod. Raises only when it could not be deleted (the caller then leaves its ledger entry open)."""
        if not await _delete_pod(sandbox_id, namespace()):
            raise SandboxError(f"gke: could not delete pod {sandbox_id} (queued for the watchdog)")

    @classmethod
    async def meter(cls, seconds: float = 0.0) -> float:
        """Put the node bill into the cost ledger (see gke_meter). Called by the guard/janitor cadence and by every create."""
        from .. import gke_meter
        return await _call(gke_meter.meter_once, _core())

    # -- one sandbox -------------------------------------------------------------------------------------------------------------
    async def _create(self) -> None:
        s = self.spec
        if not s.image:
            raise ImageBuildError("gke: a sandbox needs a registry image; GKE does not build Dockerfiles (build and push it first)")
        name = "sbx-" + uuid.uuid4().hex[:16]
        self.sandbox_id = name                          # fixed before the API call: a failure from here on deletes this name
        try:
            await self.meter()                          # the node bill is current whenever the fleet grows
        except Exception:  # noqa: BLE001 — metering must never block a sandbox
            pass
        try:
            await _call(_core().create_namespaced_pod, self._ns, pod_manifest(name, s, self._ns))
            await self._wait_ready(name)
        except BaseException:
            await _delete_pod(name, self._ns)
            self.sandbox_id = ""
            raise

    async def _wait_ready(self, name: str) -> None:
        x = self.spec.extra or {}
        capacity_wait = float(x.get("capacity_wait_s", 900))
        ready_timeout = float(x.get("ready_timeout_s", max(1200, self.spec.create_timeout)))
        t0 = time.time()
        unschedulable_since = None
        detail = ""
        while True:
            pod = await _call(_core().read_namespaced_pod, name, self._ns)
            now = time.time()
            state, detail = pod_start_state(pod, now, t0)
            if state == "ready":
                return
            if state == "image_error":
                raise ImageBuildError(f"gke: image {self.spec.image} cannot be pulled: {detail}")
            if state == "failed":
                raise SandboxCreateError(f"gke: pod {name} failed to start: {detail}")
            if state == "unschedulable":
                unschedulable_since = unschedulable_since or now
                if now - unschedulable_since > capacity_wait:
                    raise CapacityError("gke", f"pod unschedulable for {capacity_wait:.0f}s: {detail}")
            else:
                unschedulable_since = None
            if now - t0 > ready_timeout:
                raise SandboxCreateError(f"gke: pod {name} not ready after {ready_timeout:.0f}s ({detail})")
            await asyncio.sleep(POLL_S)

    async def _run(self, argv: list, *, stdin: Optional[bytes] = None, timeout: float, stdout_only: bool = False):
        return await _call(_stream_exec, _core(), self.sandbox_id, self._ns, argv, stdin=stdin, timeout=timeout,
                           stdout_only=stdout_only)

    async def _exec(self, command, cwd, env, timeout) -> ExecResult:
        t0 = time.time()
        rc, out = await self._run(exec_argv(command, cwd, env, timeout), timeout=float(timeout) + 30)
        text = out[-MAX_OUTPUT_BYTES:].decode("utf-8", "replace")
        if rc is None:
            return ExecResult(-1, text + "\n[gke: exec stream ended before the command finished]")
        if rc in (124, 137) and time.time() - t0 >= timeout - 1:
            return ExecResult(-1, f"Execution timed out after {timeout} seconds.\n" + text)
        return ExecResult(rc, text)

    async def _upload(self, data: bytes, remote_path: str) -> None:
        """`head -c N` ends after exactly N bytes (stdin cannot be closed on every exec protocol), then the size is checked."""
        n, q = len(data), shlex.quote(remote_path)
        argv = ["/bin/sh", "-c", f"head -c {n} > {q} && test \"$(wc -c < {q} | tr -d ' ')\" = {n}"]
        rc, out = await self._run(argv, stdin=bytes(data), timeout=max(120.0, n / 2e6 + 60))
        if rc != 0:
            raise SandboxError(f"gke: upload of {n} bytes to {remote_path} failed (rc={rc}): {out[-300:].decode('utf-8', 'replace')}")

    async def _download(self, remote_path: str) -> bytes:
        q = shlex.quote(remote_path)
        rc, size = await self._run(["/bin/sh", "-c", f"wc -c < {q} | tr -d ' '"], timeout=60, stdout_only=True)
        if rc != 0 or not size.strip().isdigit():
            raise SandboxError(f"gke: cannot read {remote_path} (rc={rc})")
        want = int(size.strip())
        rc, data = await self._run(["/bin/sh", "-c", f"cat {q}"], timeout=max(120.0, want / 2e6 + 60), stdout_only=True)
        if rc != 0 or len(data) != want:
            raise SandboxError(f"gke: download of {remote_path} failed (rc={rc}, {len(data)} of {want} bytes)")
        return data

    async def _delete(self) -> None:
        if self.sandbox_id:
            await _delete_pod(self.sandbox_id, self._ns)
