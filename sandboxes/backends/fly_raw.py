"""Fly.io Machines: a Firecracker microVM (own kernel, real disk) per sandbox, created from a registry image through the Machines
REST API. The image's own entrypoint is replaced by `sleep <lifetime>` and `auto_destroy` is on, so a machine ends itself at its
lifetime (hard) and a crashed client cannot leave it running.

Credentials: FLY_API_KEY (the value may be `FlyV1 fm2_...`, used as the Authorization header as is; a plain token gets `Bearer`).
Org: FLY_ORG, else the token's first organization. App: FLY_APP (default `sandboxes-lib`), created on first use. Region: FLY_REGION
or extra.region (default iad). CPU preset: extra.cpu_kind (performance by default, or shared).
Disk: a machine's root disk is ~8 GB however large a loop file on it claims to be, so work that needs more (docker builds) sets
extra.volume_gb: a real volume of that size is created for the machine, mounted at extra.volume_path (default /var/lib/docker, where
dockerd's overlay2 runs on it directly) and deleted with it. A volume belongs to its machine for life: it is named after the sandbox
and `kill` removes both, so a leaked machine cannot leave a billed volume behind.

Dockerfile builds: Fly only boots registry images, so a spec with extra["dockerfile"] (+ extra["context_dir"]) is built first, inside a
Fly builder VM with a real volume under docker, pushed to Fly's own registry (registry.fly.io/<app>:b-<hash of the context>, which the
FLY_API_KEY token can write) and booted from there. The hash makes a built task image reusable by every sandbox of that task, in this
process (a lock) or another (a file lock); a failed build raises ImageBuildError, which is not retried.

Big files: the exec API moves ~11 KB per call, so a 100+ MB harness tarball would take thousands of calls per sandbox. With
SANDBOX_STAGE_S3_URI set (s3://bucket/prefix, S3_* credentials as euphoria's mirror uses) a file over 1 MB is stored there once, named by its
hash, and the sandbox downloads it with curl from a presigned URL (1 hour): the same bytes are never uploaded twice, and the sandbox never
sees a credential. Unset, or if staging fails, uploads fall back to the chunked path.

API limits worked around here (measured): one exec call is cut off at ~60 s (long commands run detached and are polled, see
RawSandbox._exec_detached), exec ignores stdin, a command may be at most ~100 KB and a response 10 MB, so files move through
chunked commands (uploads in 11 KB parts, downloads in 700 KB parts).
"""

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import fcntl
import gzip
import hashlib
import os
import shlex
import time
from pathlib import Path

from ..errors import CapacityError, ImageBuildError
from ..raw import ExecResult, LiveSandbox, RawSandbox

API = "https://api.machines.dev/v1"
UP_CHUNK, DOWN_CHUNK = 11_000, 700_000     # the exec API rejects a request over ~16 KB (PayloadTooLarge): 11 KB base64s to ~14.7 KB
_clients: dict = {}
_app_ready: dict = {}
_org_cache: dict = {}


def _auth() -> str:
    raw = os.environ["FLY_API_KEY"].strip().strip('"').strip("'")
    return raw if raw.startswith("FlyV1") else "Bearer " + raw


def _client():
    import httpx
    loop = asyncio.get_running_loop()
    if id(loop) not in _clients:
        _clients[id(loop)] = httpx.AsyncClient(base_url=API, timeout=90, headers={"Authorization": _auth()},
                                               limits=httpx.Limits(max_connections=300, max_keepalive_connections=100))
    return _clients[id(loop)]


async def _org() -> str:
    if os.environ.get("FLY_ORG"):
        return os.environ["FLY_ORG"]
    if "org" not in _org_cache:
        r = await _client().post("https://api.fly.io/graphql", json={"query": "{ organizations { nodes { slug } } }"})
        _org_cache["org"] = r.json()["data"]["organizations"]["nodes"][0]["slug"]
    return _org_cache["org"]


def app_name() -> str:
    return os.environ.get("FLY_APP", "sandboxes-lib")


async def ensure_app() -> str:
    app = app_name()
    if app in _app_ready:
        return app
    c = _client()
    r = await c.get(f"/apps/{app}")
    if r.status_code == 404 or r.status_code == 403:
        r = await c.post("/apps", json={"app_name": app, "org_slug": await _org()})
        if r.status_code not in (200, 201, 409, 422):
            raise RuntimeError(f"fly: could not create app {app}: {r.status_code} {r.text[:200]}")
    _app_ready[app] = True
    return app


VOLUME_PREFIX = "sbx"
BUILDER_IMAGE = os.environ.get("SANDBOX_FLY_BUILDER_IMAGE", "registry-b.deepgrove.ai/openenv/swe-env-builder-dind:latest")
BUILDER_VOLUME_GB = 60                 # docker's storage while building: a toolchain image is 10-15 GB, plus layers and cache
BUILD_TIMEOUT_S = 3300
_build_locks: dict = {}
_ACCEPT = ", ".join(["application/vnd.oci.image.manifest.v1+json", "application/vnd.oci.image.index.v1+json",
                     "application/vnd.docker.distribution.manifest.v2+json", "application/vnd.docker.distribution.manifest.list.v2+json"])


STAGE_MIN_BYTES = 1_000_000
STAGE_ENV = "SANDBOX_STAGE_S3_URI"


def _stage_sync(data: bytes) -> "str | None":
    """Store `data` under its hash in the staging bucket (once) and return a presigned GET URL, or None when staging is not configured."""
    uri = os.environ.get(STAGE_ENV, "")
    if not uri.startswith("s3://"):
        return None
    import boto3
    from botocore.config import Config
    bucket, _, prefix = uri[5:].strip("/").partition("/")
    key = f"{prefix.strip('/')}/{hashlib.sha256(data).hexdigest()}".lstrip("/")
    endpoint = os.environ.get("S3_ENDPOINT_URL") or None
    c = boto3.client("s3", endpoint_url=endpoint, region_name=os.environ.get("S3_REGION") or ("auto" if endpoint else None),
                     aws_access_key_id=os.environ.get("S3_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID"),
                     aws_secret_access_key=os.environ.get("S3_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY"),
                     config=Config(connect_timeout=10, read_timeout=120, retries={"max_attempts": 3}))
    try:
        c.head_object(Bucket=bucket, Key=key)
    except Exception:  # noqa: BLE001 — not there yet
        c.put_object(Bucket=bucket, Key=key, Body=data)
    return c.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600)


def context_tag(dockerfile: str, context_dir: str) -> str:
    """A tag naming the exact build context: the same files give the same tag, so a built image is found again."""
    root = Path(context_dir)
    h = hashlib.sha256(Path(dockerfile).name.encode())
    for f in sorted(p for p in root.rglob("*") if p.is_file()):
        h.update(str(f.relative_to(root)).encode())
        h.update(f.read_bytes())
    return "b-" + h.hexdigest()[:24]


async def _image_exists(app: str, tag: str) -> bool:
    import httpx
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(f"https://registry.fly.io/v2/{app}/manifests/{tag}", headers={"Accept": _ACCEPT}, auth=("x", _auth()))
    return r.status_code == 200


BUILD_LOG_DIR = Path(os.path.expanduser("~/.cache/sandboxes/fly-build-logs"))
BUILD_SLOTS = int(os.environ.get("SANDBOX_FLY_BUILD_SLOTS", "12"))      # builder VMs at once, across every process on this host
BUILDER_CPU = int(os.environ.get("SANDBOX_FLY_BUILDER_CPU", "8"))
BUILDER_MEM_GB = int(os.environ.get("SANDBOX_FLY_BUILDER_MEM_GB", "16"))


async def _flock(path: Path):
    """An exclusive lock on `path` held in a file handle (taken in a thread so the loop keeps running)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(path, "w")
    await asyncio.get_running_loop().run_in_executor(None, fcntl.flock, fh, fcntl.LOCK_EX)
    return fh


async def _build_slot():
    """One of BUILD_SLOTS cross-process slots (lock files), so a big run cannot start hundreds of builder VMs and volumes at once."""
    base = Path(os.path.expanduser("~/.cache/sandboxes"))
    while True:
        for k in range(BUILD_SLOTS):
            fh = open(base / f"fly-build-slot-{k}.lock", "w")
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fh
            except OSError:
                fh.close()
        await asyncio.sleep(5)


async def _build_image(spec, app: str, region: str) -> str:
    """The registry.fly.io reference of `spec`'s Dockerfile image, building and pushing it first if it is not there yet.
    Progress goes to ~/.cache/sandboxes/fly-build-logs/<task>-<tag>.log (what the builder is doing, a line per step or every ~20 s)."""
    dockerfile, context_dir = str(spec.extra["dockerfile"]), str(spec.extra.get("context_dir") or Path(spec.extra["dockerfile"]).parent)
    tag = context_tag(dockerfile, context_dir)
    ref = f"registry.fly.io/{app}:{tag}"
    name = Path(context_dir).parent.name if Path(context_dir).name == "environment" else Path(context_dir).name
    log_path = BUILD_LOG_DIR / f"{name}-{tag}.log"
    BUILD_LOG_DIR.mkdir(parents=True, exist_ok=True)

    def log(msg: str) -> None:
        with open(log_path, "a") as f:
            f.write(f"{time.strftime('%H:%M:%S')} {msg}\n")

    lock = _build_locks.setdefault(tag, asyncio.Lock())
    async with lock:
        fh = await _flock(Path(os.path.expanduser("~/.cache/sandboxes")) / f"fly-build-{tag}.lock")    # another process may be building it
        try:
            if await _image_exists(app, tag):
                return ref
            t0 = time.time()
            log("waiting for a build slot")
            slot = await _build_slot()
            try:
                log(f"opening builder VM ({BUILDER_CPU} cpu / {BUILDER_MEM_GB} GiB, {BUILDER_VOLUME_GB} GB volume)")
                builder = FlyRaw(type(spec)(image=BUILDER_IMAGE, cpu=BUILDER_CPU, mem_gb=BUILDER_MEM_GB, boot_cmd="dind-start.sh", lifetime_s=BUILD_TIMEOUT_S + 600,
                                            labels=dict(spec.labels or {}), extra={"volume_gb": BUILDER_VOLUME_GB, "region": region, "cpu_kind": "performance"}))
                await builder.start()
                try:
                    log(f"builder {builder.sandbox_id} up after {time.time() - t0:.0f}s; uploading the build context")
                    await builder.put_file(context_dir, "/work/ctx")
                    rel = os.path.relpath(dockerfile, context_dir)
                    script = (f"cd /work/ctx && docker build --network=host --progress=plain -t {shlex.quote(ref)} -f {shlex.quote(rel)} . > /tmp/build.log 2>&1 "
                              f"&& echo built >> /tmp/build.log && printf %s {shlex.quote(_auth())} | docker login registry.fly.io -u x --password-stdin >> /tmp/build.log 2>&1 "
                              f"&& docker push {shlex.quote(ref)} >> /tmp/build.log 2>&1; echo $? > /tmp/build.rc")
                    await builder.exec("rm -f /tmp/build.rc; nohup sh -c " + shlex.quote(script) + " >/dev/null 2>&1 &", timeout=30)
                    seen, deadline = 0, time.time() + BUILD_TIMEOUT_S
                    while time.time() < deadline:
                        await asyncio.sleep(20)
                        r = await builder.exec("cat /tmp/build.rc 2>/dev/null; echo ::LOG::; grep -E '^#[0-9]+ \\[|^#[0-9]+ DONE|built|error|ERROR|Pushed|digest' /tmp/build.log | tail -n 40 "
                                               "| cut -c1-160", timeout=30)
                        head, _, lines = r.stdout.partition("::LOG::")
                        new = [l for l in lines.splitlines()][seen:]
                        seen += len(new)
                        for l in new[-6:]:
                            log("  " + l.strip())
                        if head.strip().isdigit():
                            if int(head.strip()) != 0:
                                tail = (await builder.exec("tail -c 1500 /tmp/build.log", timeout=30)).stdout
                                log("FAILED:\n" + tail)
                                raise ImageBuildError(f"fly: building/pushing {dockerfile} failed (see {log_path}): {tail[-500:]}")
                            break
                    else:
                        raise ImageBuildError(f"fly: building {dockerfile} did not finish in {BUILD_TIMEOUT_S}s (see {log_path})")
                    log(f"done and pushed {ref} in {time.time() - t0:.0f}s")
                finally:
                    await builder.stop()
            finally:
                fcntl.flock(slot, fcntl.LOCK_UN)
                slot.close()
            return ref
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
            fh.close()


def _ts(iso):
    try:
        return dt.datetime.fromisoformat(str(iso).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


async def _delete_volume(app: str, vid: str) -> None:
    """Delete a volume once its machine is gone (Fly refuses while it is still attached, which can take a few seconds)."""
    c = _client()
    for attempt in range(10):
        r = await c.delete(f"/apps/{app}/volumes/{vid}")
        if r.status_code in (200, 202, 204, 404):
            return
        await asyncio.sleep(2 + attempt)
    raise RuntimeError(f"fly: could not delete volume {vid}: {r.status_code} {r.text[:120]}")


def _is_capacity(status: int, text: str) -> bool:
    t = text.lower()
    return status == 429 or "insufficient" in t or "capacity" in t or "quota" in t or "limit" in t and "machine" in t or "could not reserve" in t


class FlyRaw(RawSandbox):
    provider = "fly"
    hard_lifetime = True            # `sleep <lifetime>` + auto_destroy
    max_call_s = 55

    def __init__(self, spec, model=None):
        super().__init__(spec, model)
        self._app = ""
        self._volume_id = ""

    # -- fleet interface ----------------------------------------------------------------------------------
    @classmethod
    async def list_live(cls):
        app = await ensure_app()
        r = await _client().get(f"/apps/{app}/machines")
        out = []
        for m in r.json() if r.status_code == 200 else []:
            if m.get("state") in ("destroyed", "destroying"):
                continue
            ts = m.get("created_at")
            try:
                created = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()
            except ValueError:
                created = None
            out.append(LiveSandbox("fly", m["id"], created, m.get("state", ""), dict((m.get("config") or {}).get("metadata") or {})))
        return out

    @classmethod
    async def kill(cls, sandbox_id):
        app = await ensure_app()
        c = _client()
        m = await c.get(f"/apps/{app}/machines/{sandbox_id}")
        volumes = [x["volume"] for x in ((m.json().get("config") or {}).get("mounts") or []) if x.get("volume")] if m.status_code == 200 else []
        r = await c.delete(f"/apps/{app}/machines/{sandbox_id}", params={"force": "true"})
        if r.status_code not in (200, 404):
            raise RuntimeError(f"fly: destroy {sandbox_id}: {r.status_code} {r.text[:120]}")
        for vid in volumes:
            await _delete_volume(app, vid)

    @classmethod
    async def stored_artifacts(cls):
        """Volumes we hold (billed per GB-month whether or not a machine uses them), including orphans whose machine is gone."""
        from ..raw import StoredArtifact
        app = await ensure_app()
        r = await _client().get(f"/apps/{app}/volumes")
        out = []
        for v in r.json() if r.status_code == 200 else []:
            if v.get("name", "").startswith(VOLUME_PREFIX) and v.get("state") not in ("destroyed", "destroying"):
                out.append(StoredArtifact("fly", "volume", v["id"], float(v.get("size_gb") or 0), _ts(v.get("created_at"))))
        return out

    # -- per-sandbox primitives ---------------------------------------------------------------------------
    async def _create(self) -> None:
        s = self.spec
        self._app = await ensure_app()
        guest = {"cpu_kind": s.extra.get("cpu_kind", "performance"), "cpus": int(s.cpu), "memory_mb": int(s.mem_gb) * 1024}
        region = s.extra.get("region") or os.environ.get("FLY_REGION", "iad")
        mounts = []
        if s.extra.get("volume_gb"):
            v = await _client().post(f"/apps/{self._app}/volumes", json={"name": f"{VOLUME_PREFIX}{os.urandom(6).hex()}", "region": region, "size_gb": int(s.extra["volume_gb"])})
            if v.status_code not in (200, 201):
                if _is_capacity(v.status_code, v.text):
                    raise CapacityError("fly", f"volume: {v.status_code} {v.text[:160]}")
                raise RuntimeError(f"fly: volume create failed: {v.status_code} {v.text[:200]}")
            self._volume_id = v.json()["id"]
            mounts = [{"volume": self._volume_id, "path": s.extra.get("volume_path", "/var/lib/docker")}]
        image = await _build_image(s, self._app, region) if s.extra.get("dockerfile") else s.image
        cfg = {"image": image, "guest": guest, **({"mounts": mounts} if mounts else {}), "auto_destroy": True, "restart": {"policy": "no"},
               "init": {"exec": ["/bin/sh", "-c", f"sleep {int(s.lifetime_s)}"]},
               "env": {k: str(v) for k, v in (s.env or {}).items()},
               "metadata": {"sandboxes": "1", **{k: str(v) for k, v in (s.labels or {}).items()}}}
        r = await _client().post(f"/apps/{self._app}/machines", json={"region": region, "config": cfg})
        if r.status_code != 200:
            await self._drop_volume()
            if _is_capacity(r.status_code, r.text):
                raise CapacityError("fly", f"{r.status_code} {r.text[:160]}")
            raise RuntimeError(f"fly: create failed: {r.status_code} {r.text[:200]}")
        self.sandbox_id = r.json()["id"]
        deadline = time.time() + s.create_timeout
        while time.time() < deadline:
            w = await _client().get(f"/apps/{self._app}/machines/{self.sandbox_id}/wait", params={"state": "started", "timeout": 30}, timeout=45)
            if w.status_code == 200:
                return
            await asyncio.sleep(1)
        await self._delete()
        raise TimeoutError(f"fly machine {self.sandbox_id} not started in {s.create_timeout}s")

    async def _exec(self, command, cwd, env, timeout) -> ExecResult:
        pre = "".join(f"export {k}={shlex.quote(str(v))}; " for k, v in (env or {}).items()) + (f"cd {shlex.quote(cwd)} && " if cwd else "")
        body = {"command": ["bash", "-lc", pre + command], "timeout": min(int(timeout), self.max_call_s)}
        for attempt in range(8):
            r = await _client().post(f"/apps/{self._app}/machines/{self.sandbox_id}/exec", json=body, timeout=min(int(timeout), self.max_call_s) + 25)
            if r.status_code != 429:
                break
            await asyncio.sleep(min(10.0, 1.0 * 2 ** attempt))     # per-machine exec rate limit ("resource_exhausted"): rejected before it ran, so retrying is safe
        if r.status_code == 408:
            return ExecResult(-1, f"Execution timed out after {timeout} seconds.")
        if r.status_code != 200:
            return ExecResult(-1, f"fly exec {r.status_code}: {r.text[:300]}")
        j = r.json()
        out = (j.get("stdout") or "") + (j.get("stderr") or "")
        if out.startswith("Unhandled rejection"):       # e.g. PayloadTooLarge: HTTP 200 and exit_code 0, with the error as stdout
            return ExecResult(-1, f"fly exec rejected: {out[:300]}")
        return ExecResult(j.get("exit_code", -1), out)

    async def _exec_detached(self, command, cwd, env, timeout) -> ExecResult:
        """A command that may outlive one exec call (~55 s) runs in the background, but the FIRST call also waits for it: the script is
        written inline, started, and polled inside the machine for up to ~45 s, so a short command (nearly every agent command) costs
        one API call instead of upload + start + poll + fetch. Past that, the caller polls with the same in-machine wait (no 3 s sleeps)."""
        tag = os.urandom(5).hex()
        script, out, rcf = f"/tmp/.x-{tag}.sh", f"/tmp/.x-{tag}.out", f"/tmp/.x-{tag}.rc"
        body = "".join(f"export {k}={shlex.quote(str(v))}\n" for k, v in (env or {}).items()) + (f"cd {shlex.quote(cwd)} || exit 1\n" if cwd else "") + command + "\n"
        b64 = base64.b64encode(body.encode()).decode()
        wait = ('for i in $(seq 1 {n}); do [ -s {rc} ] && break; sleep 0.1; done; '
                'if [ -s {rc} ]; then echo "::RC::$(cat {rc})"; tail -c 200000 {out}; rm -f {out} {rc} {sh}; elif [ -e {sh} ]; then echo "::RUNNING::"; else echo "::LOST::"; fi')
        if len(b64) < 9000:
            start = (f"printf %s '{b64}' | base64 -d > {script}; "
                     f"nohup sh -c 'bash -l {script} > {out} 2>&1; echo $? > {rcf}' >/dev/null 2>&1 & ")
        else:
            await self._upload(body.encode(), script)
            start = f"nohup sh -c 'bash -l {script} > {out} 2>&1; echo $? > {rcf}' >/dev/null 2>&1 & "
        deadline = time.time() + timeout
        step = start + wait.format(n=350, rc=rcf, out=out, sh=script)
        while True:
            r = await self._exec(step, None, None, 50)
            text = r.stdout
            if r.rc == 0 and text.startswith("::RC::"):
                head, _, rest = text.partition("\n")
                return ExecResult(int(head[6:] or -1), rest)
            timed_out = r.rc == -1 and "timed out" in text.lower()      # the API call (not the command) ran over: the script is already started, keep waiting
            if text.startswith("::LOST::") or (r.rc != 0 and not timed_out and not text.startswith("::RUNNING::")):
                return ExecResult(-1, "could not run the command: " + text[-300:])
            if time.time() >= deadline:
                await self._exec(f"pkill -f {script}; true", None, None, 20)
                return ExecResult(-1, f"Execution timed out after {timeout} seconds.")
            step = wait.format(n=350, rc=rcf, out=out, sh=script)        # keep waiting inside the machine, no client-side sleep

    async def _upload(self, data: bytes, remote_path: str) -> None:
        if len(data) >= STAGE_MIN_BYTES:
            try:
                url = await asyncio.get_running_loop().run_in_executor(None, _stage_sync, data)
            except Exception:  # noqa: BLE001 — staging is an optimisation: fall back to the chunked path
                url = None
            if url:
                d = shlex.quote(os.path.dirname(remote_path) or "/")
                r = await self.exec(f"mkdir -p {d} && (curl -fsSL --retry 3 -o {shlex.quote(remote_path)} {shlex.quote(url)} || "
                                    f"wget -q -O {shlex.quote(remote_path)} {shlex.quote(url)})", timeout=900)
                if r.rc == 0:
                    return
        d = f"/tmp/.up-{self.env_id[:8]}-{abs(hash(remote_path)) % 10**8}"
        await self._exec(f"mkdir -p {d} {shlex.quote(os.path.dirname(remote_path) or '/')}", None, None, 30)
        sem = asyncio.Semaphore(2)       # the exec API rate-limits per machine
        packed = gzip.compress(data, 6)     # a request carries ~11 KB, so compress first: a task's build context is a few KB of text
        parts = [packed[i:i + UP_CHUNK] for i in range(0, len(packed), UP_CHUNK)] or [b""]

        async def put(i, chunk):
            async with sem:
                b64 = base64.b64encode(chunk).decode()
                r = await self._exec(f"printf %s '{b64}' | base64 -d > {d}/{i:06d}", None, None, 40)
                if r.rc != 0:
                    raise RuntimeError(f"fly upload part {i}: {r.stdout[:150]}")
        await asyncio.gather(*(put(i, c) for i, c in enumerate(parts)))
        r = await self._exec(f"cat {d}/* | gunzip > {shlex.quote(remote_path)} && rm -rf {d}", None, None, 55)
        if r.rc != 0:
            raise RuntimeError(f"fly upload assemble: {r.stdout[:150]}")

    async def _download(self, remote_path: str) -> bytes:
        r = await self._exec(f"stat -c %s {shlex.quote(remote_path)}", None, None, 30)
        size = int(r.stdout.strip().splitlines()[-1])
        sem = asyncio.Semaphore(6)

        async def get(i):
            async with sem:
                r = await self._exec(f"dd if={shlex.quote(remote_path)} bs={DOWN_CHUNK} skip={i} count=1 2>/dev/null | base64 -w0", None, None, 50)
                if r.rc != 0:
                    raise RuntimeError(f"fly download chunk {i}: {r.stdout[:150]}")
                return base64.b64decode(r.stdout.strip())
        return b"".join(await asyncio.gather(*(get(i) for i in range(max(1, -(-size // DOWN_CHUNK))))))

    async def _drop_volume(self) -> None:
        vid, self._volume_id = self._volume_id, ""
        if vid:
            await _delete_volume(self._app, vid)

    async def _delete(self) -> None:
        r = await _client().delete(f"/apps/{self._app}/machines/{self.sandbox_id}", params={"force": "true"})
        if r.status_code not in (200, 404):
            raise RuntimeError(f"fly destroy: {r.status_code}")
        await self._drop_volume()
