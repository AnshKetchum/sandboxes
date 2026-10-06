"""Fly.io Machines: a Firecracker microVM (own kernel, real disk) per sandbox, created from a registry image through the Machines
REST API. The image's own entrypoint is replaced by `sleep <lifetime>` and `auto_destroy` is on, so a machine ends itself at its
lifetime (hard) and a crashed client cannot leave it running.

Credentials: FLY_API_KEY (the value may be `FlyV1 fm2_...`, used as the Authorization header as is; a plain token gets `Bearer`).
Org: FLY_ORG, else the token's first organization. App: FLY_APP (default `sandboxes-lib`), created on first use. Region: FLY_REGION
or extra.region (default iad). CPU preset: extra.cpu_kind (performance by default, or shared).

API limits worked around here (measured): one exec call is cut off at ~60 s (long commands run detached and are polled, see
RawSandbox._exec_detached), exec ignores stdin, a command may be at most ~100 KB and a response 10 MB, so files move through
chunked commands (uploads in 40 KB parts, downloads in 700 KB parts).
"""

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import os
import shlex
import time

from ..errors import CapacityError
from ..raw import ExecResult, LiveSandbox, RawSandbox

API = "https://api.machines.dev/v1"
UP_CHUNK, DOWN_CHUNK = 40_000, 700_000
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
        r = await _client().delete(f"/apps/{app}/machines/{sandbox_id}", params={"force": "true"})
        if r.status_code not in (200, 404):
            raise RuntimeError(f"fly: destroy {sandbox_id}: {r.status_code} {r.text[:120]}")

    # -- per-sandbox primitives ---------------------------------------------------------------------------
    async def _create(self) -> None:
        s = self.spec
        self._app = await ensure_app()
        guest = {"cpu_kind": s.extra.get("cpu_kind", "performance"), "cpus": int(s.cpu), "memory_mb": int(s.mem_gb) * 1024}
        cfg = {"image": s.image, "guest": guest, "auto_destroy": True, "restart": {"policy": "no"},
               "init": {"exec": ["/bin/sh", "-c", f"sleep {int(s.lifetime_s)}"]},
               "env": {k: str(v) for k, v in (s.env or {}).items()},
               "metadata": {"sandboxes": "1", **{k: str(v) for k, v in (s.labels or {}).items()}}}
        r = await _client().post(f"/apps/{self._app}/machines", json={"region": s.extra.get("region") or os.environ.get("FLY_REGION", "iad"), "config": cfg})
        if r.status_code != 200:
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
        r = await _client().post(f"/apps/{self._app}/machines/{self.sandbox_id}/exec",
                                 json={"command": ["bash", "-lc", pre + command], "timeout": min(int(timeout), self.max_call_s)}, timeout=min(int(timeout), self.max_call_s) + 25)
        if r.status_code == 408:
            return ExecResult(-1, f"Execution timed out after {timeout} seconds.")
        if r.status_code != 200:
            return ExecResult(-1, f"fly exec {r.status_code}: {r.text[:300]}")
        j = r.json()
        return ExecResult(j.get("exit_code", -1), (j.get("stdout") or "") + (j.get("stderr") or ""))

    async def _upload(self, data: bytes, remote_path: str) -> None:
        d = f"/tmp/.up-{self.env_id[:8]}-{abs(hash(remote_path)) % 10**8}"
        await self._exec(f"mkdir -p {d} {shlex.quote(os.path.dirname(remote_path) or '/')}", None, None, 30)
        sem = asyncio.Semaphore(8)
        parts = [data[i:i + UP_CHUNK] for i in range(0, len(data), UP_CHUNK)] or [b""]

        async def put(i, chunk):
            async with sem:
                b64 = base64.b64encode(chunk).decode()
                r = await self._exec(f"printf %s '{b64}' | base64 -d > {d}/{i:06d}", None, None, 40)
                if r.rc != 0:
                    raise RuntimeError(f"fly upload part {i}: {r.stdout[:150]}")
        await asyncio.gather(*(put(i, c) for i, c in enumerate(parts)))
        r = await self._exec(f"cat {d}/* > {shlex.quote(remote_path)} && rm -rf {d}", None, None, 55)
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

    async def _delete(self) -> None:
        r = await _client().delete(f"/apps/{self._app}/machines/{self.sandbox_id}", params={"force": "true"})
        if r.status_code not in (200, 404):
            raise RuntimeError(f"fly destroy: {r.status_code}")
