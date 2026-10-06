"""Blaxel: sandboxes from an image Blaxel builds once from the registry image (it injects its own
sandbox API). Billed per GB-RAM-second with CPU included; snapshots/images/volumes are billed storage,
so snapshots are off. The root filesystem is a RAM-backed overlay (50% of memory), so docker needs an
ext4 loop file on it (`extra.docker_loop_gb`); that makes docker usable but small. A single command
call is capped at 60 s by the API, so longer commands run in the background and are polled."""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import shlex
import time
import uuid
from pathlib import Path

from ..compat import ensure_blaxel_importable
from ..errors import CapacityError
from ..raw import ExecResult, RawSandbox

ensure_blaxel_importable()

_CACHE = Path(os.environ.get("SANDBOX_CACHE_DIR", Path.home() / ".cache" / "sandboxes")) / "blaxel_images.json"
_locks: dict = {}


def _key(image: str, mem_gb: int) -> str:
    return f"{image}|{mem_gb}"


def _load() -> dict:
    try:
        return json.loads(_CACHE.read_text())
    except Exception:  # noqa: BLE001
        return {}


async def ensure_image(image: str, mem_gb: int, region: str) -> str:
    """Build (once) the Blaxel sandbox image for `image` and return its reference."""
    key = _key(image, mem_gb)
    ref = _load().get(key)
    if ref:
        return ref
    loop_lock = _locks.setdefault(id(asyncio.get_running_loop()), asyncio.Lock())
    async with loop_lock:
        _CACHE.parent.mkdir(parents=True, exist_ok=True)
        with open(_CACHE.with_suffix(".lock"), "w") as lf:      # one build at a time across processes
            fcntl.flock(lf, fcntl.LOCK_EX)
            return await _build_locked(image, mem_gb, key)


async def _build_locked(image: str, mem_gb: int, key: str) -> str:
    from blaxel.core import ImageInstance, SandboxInstance

    ref = _load().get(key)
    if ref:
        return ref
    name = "sbximg-" + uuid.uuid4().hex[:8]
    sb = await ImageInstance.from_registry(image).build(name=name, memory=mem_gb * 1024, timeout=1500.0)
    ref = sb.spec.runtime.image
    cache = _load()
    cache[key] = ref
    _CACHE.write_text(json.dumps(cache))
    try:   # the build leaves a running sandbox behind: it holds a concurrency slot and bills RAM
        await SandboxInstance.delete(name)
    except Exception:  # noqa: BLE001
        pass
    return ref


class BlaxelRaw(RawSandbox):
    provider = "blaxel"

    def __init__(self, spec, model=None):
        super().__init__(spec, model)
        self._sb = None
        self._name = ""

    async def _create(self) -> None:
        from blaxel.core import SandboxInstance

        s = self.spec
        region = s.extra.get("region") or os.environ.get("BL_REGION") or "us-pdx-1"
        self._name = "sbx-" + uuid.uuid4().hex[:14]
        try:
            ref = s.extra.get("image_ref") or await ensure_image(s.image, int(s.mem_gb), region)
            await SandboxInstance.create({
                "name": self._name, "image": ref, "memory": int(s.mem_gb) * 1024, "region": region,
                "labels": {k: str(v) for k, v in {"sandboxes": "1", **(s.labels or {})}.items()},
                "ttl": f"{max(1, int(s.lifetime_s) // 60)}m", "snapshot_enabled": False,
                "envs": [{"name": k, "value": str(v)} for k, v in (s.env or {}).items()] or None})
        except Exception as e:  # noqa: BLE001
            m = str(e).lower()
            if "429" in m or "quota_exceeded" in m or "quota exceeded" in m or "ratelimit" in type(e).__name__.lower() or "too many" in m:
                raise CapacityError("blaxel", str(e)[:200]) from e
            raise
        t0 = time.time()
        while time.time() - t0 < s.create_timeout:
            g = await SandboxInstance.get(self._name)
            if str(g.status).endswith("DEPLOYED"):
                self._sb = g
                break
            if str(g.status).split(".")[-1] in ("FAILED", "TERMINATED", "DELETING"):
                raise RuntimeError(f"blaxel sandbox {self._name} entered {g.status}")
            await asyncio.sleep(1)
        else:
            await self._cleanup_name()
            raise TimeoutError(f"blaxel sandbox {self._name} not DEPLOYED in {s.create_timeout}s")
        self.sandbox_id = self._name
        loop_gb = int(s.extra.get("docker_loop_gb", 0) or 0)
        if loop_gb:
            r = await self._exec(f"truncate -s {loop_gb}G /mnt/docker.img && mkfs.ext4 -q -F /mnt/docker.img && "
                                 f"mkdir -p /var/lib/docker && mount -o loop /mnt/docker.img /var/lib/docker", None, None, 120)
            if r.rc != 0:
                raise RuntimeError(f"blaxel docker loop setup failed: {r.stdout[-300:]}")

    async def _cleanup_name(self) -> None:
        from blaxel.core import SandboxInstance
        try:
            await SandboxInstance.delete(self._name)
        except Exception:  # noqa: BLE001
            pass

    async def _run(self, command: str, cwd, env, timeout: int):
        return await self._sb.process.exec({"command": command, "working_dir": cwd, "env": env or None,
                                            "wait_for_completion": True, "timeout": timeout})

    async def _exec(self, command, cwd, env, timeout) -> ExecResult:
        if timeout <= 55:
            r = await self._run(f"bash -lc {shlex.quote(command)}", cwd, {k: str(v) for k, v in (env or {}).items()}, timeout)
            return ExecResult(r.exit_code if r.exit_code is not None else -1, r.logs or "")
        # the API caps one call at 60 s: run detached, poll for the exit code, read the output at the end
        tag = uuid.uuid4().hex[:10]; out, rcf = f"/tmp/.x-{tag}.out", f"/tmp/.x-{tag}.rc"
        script = f"/tmp/.x-{tag}.sh"
        await self._sb.fs.write(script, command)
        await self._run(f"nohup sh -c 'bash -l {script} > {out} 2>&1; echo $? > {rcf}' >/dev/null 2>&1 &", cwd,
                        {k: str(v) for k, v in (env or {}).items()}, 20)
        deadline = time.time() + timeout
        while time.time() < deadline:
            await asyncio.sleep(3)
            r = await self._run(f"cat {rcf} 2>/dev/null", None, None, 20)
            if (r.logs or "").strip().lstrip("-").isdigit():
                rc = int(r.logs.strip())
                o = await self._run(f"tail -c 200000 {out}; rm -f {out} {rcf} {script}", None, None, 30)
                return ExecResult(rc, o.logs or "")
        await self._run(f"pkill -f {script}; true", None, None, 20)
        return ExecResult(-1, f"Execution timed out after {timeout} seconds.")

    async def _upload(self, data: bytes, remote_path: str) -> None:
        await self._sb.fs.write_binary(remote_path, data)

    async def _download(self, remote_path: str) -> bytes:
        return await self._sb.fs.read_binary(remote_path)

    async def _delete(self) -> None:
        await self._cleanup_name()
