"""harbor's Blaxel environment, with sandboxes' standard labels on the sandbox.

harbor hardcodes the labels it puts on a Blaxel sandbox (`created-by: harbor`, environment-name, session-id), so `sandboxes ls/stop
--owner X` and the janitor could not recognise one as ours. This adds the standard managed/owner/run labels (plus whatever the caller
passes) to the create config; the `ttl` harbor already takes is the lifetime.

It also deletes the content-addressed task image once the sandbox is gone. harbor never does: every task leaves a `harbor-img-*` image
(1-4 GiB) in the workspace, and Blaxel's image storage quota (200 GiB) fills up after ~110 tasks, after which every build fails with
429 QUOTA_EXCEEDED. Parallel tries of one task share the image, so Blaxel refuses the delete ("image in use") while a sibling's sandbox
still exists and the last one to stop removes it; a sibling that starts after the delete just rebuilds.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from pathlib import Path
from typing import Optional

from .compat import ensure_blaxel_importable

ensure_blaxel_importable()

from harbor.environments import blaxel as _harbor_blaxel     # noqa: E402  (needs the shim above)
from harbor.environments.blaxel import BlaxelEnvironment, _sanitize_blaxel_name     # noqa: E402

from .labels import standard_labels                          # noqa: E402


# Blaxel builds an image from a Dockerfile in a 16 GiB worker whose scratch is a tmpfs of ~14.7 GiB, so a task whose layers total more than
# that fails with "the build ran out of room". The images API only accepts `memoryMb`/`volumeMb` for registry imports, not Dockerfile
# builds, so there is no way to size a Dockerfile build: heavy tasks must be built elsewhere and brought in as a prebuilt image.


CREATE_CONCURRENCY = 2                   # simultaneous SandboxInstance.create calls per process: Blaxel's API gateway 504s a burst of 30+
CREATE_TRIES = 8
READY_TIMEOUT_S = 300                    # harbor waits 60 s for a new sandbox; under a burst of creates DEPLOYING takes minutes
SCRATCH_MB = 0                           # >0: attach a disk-backed ephemeral volume per sandbox. OFF: Blaxel rate-limits volume creation (3 concurrent creates
                                         # with a volume all got HTTP 429, without one all succeeded), which failed most attempts. Use memory_mb for room instead:
                                         # the root fs is RAM-backed, half the memory (4 GiB root at 8192).
SCRATCH_MOUNT = "/scratch"
# Directories moved onto the volume (copied, then bind-mounted over the original): everything the harness and the agent write to.
SCRATCH_DIRS = ("/workspace", "/tmp", "/root", "/opt", "/logs")
POLL_INTERVAL_S = 15.0                   # 200 attempts polling every 5 s plus image lists got HTTP 429s from Blaxel's API
IMAGE_DELETE_DELAY_S = 60
IMAGE_QUOTA_GIB = 200                    # Blaxel's image storage cap; a build over it is refused with 429 and costs the try
IMAGE_HEADROOM_GIB = 25                  # start a build only while storage is under QUOTA - HEADROOM (one task image is 0-5.5 GiB, median ~2)
HEADROOM_WAIT_S = 600
BUILD_TIMEOUT_S = 600                    # a build still BUILDING after this many seconds of actually building is stuck: free the slot, fail the try
BUILD_QUEUE_TIMEOUT_S = 2400             # Blaxel runs ~10 builds at once; the rest sit in UPLOADING (queued), which is not a failure, only a wait
_pending_deletes: set = set()           # keeps the delayed delete tasks alive until they finish


_create_sems: dict = {}


def _transient(exc: Exception) -> bool:
    """A gateway-level failure worth another try: 502/503/504 from the Blaxel API, or a dropped connection."""
    status = getattr(exc, "status_code", None)
    if status is not None:
        return int(status) in (429, 500, 502, 503, 504)
    m = f"{type(exc).__name__} {exc}".lower()
    return any(t in m for t in ("504", "503", "502", "429", "too many requests", "ratelimit", "rate limit", "server disconnected",
                                "remoteprotocolerror", "readtimeout", "connecterror"))


class _ThrottledSandboxInstance:
    """Stands in for blaxel's SandboxInstance inside harbor's blaxel module: `create` is limited to CREATE_CONCURRENCY at a time and retried
    on transient gateway errors (504 under a burst of creates); everything else is the real class."""

    def __init__(self, real):
        self._real = real

    def __getattr__(self, name):
        return getattr(self._real, name)

    async def create(self, *args, **kwargs):
        loop = asyncio.get_running_loop()
        sem = _create_sems.setdefault(id(loop), asyncio.Semaphore(CREATE_CONCURRENCY))
        config = args[0] if args else kwargs.get("config") or kwargs
        name = config.get("name") if isinstance(config, dict) else getattr(getattr(config, "metadata", None), "name", None)
        for attempt in range(CREATE_TRIES):
            async with sem:
                try:
                    if attempt and name:      # a 504 often means the create went through: adopt that sandbox rather than replaying it ("already exists")
                        try:
                            return await self._real.get(name)
                        except Exception:  # noqa: BLE001 — not there: fall through to create it
                            pass
                    return await self._real.create(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    if attempt == CREATE_TRIES - 1 or not (_transient(exc) or "already exists" in str(exc).lower()):
                        raise
            await asyncio.sleep(min(60.0, 3.0 * 2 ** attempt) * (0.75 + 0.5 * random.random()))


if not isinstance(_harbor_blaxel.SandboxInstance, _ThrottledSandboxInstance):
    _harbor_blaxel.SandboxInstance = _ThrottledSandboxInstance(_harbor_blaxel.SandboxInstance)


def _harbor_blaxel_volume():
    from blaxel.core.sandbox.types import VolumeBinding
    return VolumeBinding(name=f"scratch-{uuid.uuid4().hex[:12]}", mount_path=SCRATCH_MOUNT, type="ephemeral", size_mb=SCRATCH_MB)


def scratch_script() -> str:
    """Move SCRATCH_DIRS onto the volume: copy what is there (permissions kept), then bind-mount the copy over the original."""
    lines = ["set -e"]
    for d in SCRATCH_DIRS:
        n = d.strip("/").replace("/", "_")
        lines.append(f'mkdir -p {d} {SCRATCH_MOUNT}/{n} && chmod --reference={d} {SCRATCH_MOUNT}/{n} && chown --reference={d} {SCRATCH_MOUNT}/{n} '
                     f'&& cp -a {d}/. {SCRATCH_MOUNT}/{n}/ && mount --bind {SCRATCH_MOUNT}/{n} {d}')
    return "\n".join(lines)


async def _api(client, method: str, url: str, **kw):
    """One Blaxel API call, retried with backoff while Blaxel answers 429 (its API rate-limits a burst of attempts polling at once)."""
    for attempt in range(8):
        r = await getattr(client, method)(url, **kw)
        if r.status_code != 429:
            return r
        await asyncio.sleep(min(30.0, 2.0 * 2 ** attempt) * (0.75 + 0.5 * random.random()))
    return r


class LabelledBlaxelEnvironment(BlaxelEnvironment):
    def __init__(self, *args, labels: Optional[dict] = None, **kwargs):
        self._labels = dict(labels or {})
        super().__init__(*args, **kwargs)
        # harbor names the sandbox from the task and the session id, and euphoria gives all parallel tries of a task the same session
        # id, so they collide ("Sandbox ... already exists"). The image name is task-only and stays shared; only the sandbox is unique.
        self._sandbox_name = _sanitize_blaxel_name(f"{self._sandbox_name}-{uuid.uuid4().hex[:8]}")

    def _create_sandbox_config(self, *args, **kwargs):
        config = super()._create_sandbox_config(*args, **kwargs)
        if SCRATCH_MB:
            config["volumes"] = [_harbor_blaxel_volume()]
        config["labels"] = {**config.get("labels", {}), **standard_labels(extra=self._labels)}
        return config

    async def _move_dirs_to_scratch(self) -> None:
        if not SCRATCH_MB:
            return
        r = await self.exec(scratch_script(), timeout_sec=300)
        if r.return_code != 0:
            raise RuntimeError(f"Blaxel scratch volume setup failed (exit {r.return_code}): {(r.stderr or r.stdout or '')[-300:]}")

    async def _sandbox_status(self):
        try:
            r = await _harbor_blaxel.blaxel_client.get_async_httpx_client().get(f"/sandboxes/{self._sandbox_name}")
            return r.json().get("status") if r.status_code == 200 else None
        except Exception:  # noqa: BLE001
            return None

    async def _wait_until_ready(self) -> None:
        sandbox = self._require_sandbox()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + READY_TIMEOUT_S
        last_error = None
        failed = False
        while loop.time() < deadline:
            try:
                await sandbox.fs.ls("/")
                await self._move_dirs_to_scratch()
                return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
            if await self._sandbox_status() == "FAILED":      # the deployment failed: waiting longer cannot help
                failed = True
                break
            await asyncio.sleep(2)
        # dead or stuck: the retry (euphoria's start_env stops this env, then starts it again) must not meet the same sandbox name
        self._sandbox_name = _sanitize_blaxel_name(f"{self._sandbox_name}-{uuid.uuid4().hex[:8]}")
        raise RuntimeError("Blaxel sandbox deployment failed" if failed else "Blaxel sandbox did not become ready") from last_error

    async def _stop_direct(self, delete: bool) -> None:
        had_sandbox = self._sandbox is not None
        await super()._stop_direct(delete)
        if delete and had_sandbox:
            # Not now: a sibling try can have passed its "image is built" check and be about to create a sandbox from it, which then
            # fails in ~1 s because the image is gone (seen: 23 of 29 failed sandboxes). Blaxel refuses the delete while any sandbox uses
            # the image, so waiting a few minutes leaves only the case where nobody is going to use it again.
            task = asyncio.ensure_future(self._delete_image_later())
            _pending_deletes.add(task)
            task.add_done_callback(_pending_deletes.discard)

    async def _delete_image_later(self) -> None:
        await asyncio.sleep(IMAGE_DELETE_DELAY_S)
        await self._delete_image()

    async def _delete_image(self) -> None:
        name = self._require_image_name()
        try:
            r = await _harbor_blaxel.blaxel_client.get_async_httpx_client().delete(f"/images/sandbox/{name}")
            if r.status_code >= 300 and r.status_code != 404 and "in use" not in r.text:
                self.logger.warning("Failed to delete Blaxel image %s: %s %s", name, r.status_code, r.text[:200])
        except Exception as exc:  # noqa: BLE001 — a leftover image is a cleanup problem, never a trial failure
            self.logger.warning("Failed to delete Blaxel image %s: %s", name, exc)

    async def _wait_for_image_headroom(self, client) -> None:
        """Block while image storage is near the cap, sweeping unused images; fail (retryably) only after HEADROOM_WAIT_S."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + HEADROOM_WAIT_S
        swept = False
        while True:
            try:
                r = await _api(client, "get", "/images")
                used = sum(((i.get("spec") or {}).get("size") or 0) for i in r.json() if i["metadata"].get("resourceType") == "sandbox") / 2**30
            except Exception:  # noqa: BLE001 — cannot tell: let the build try
                return
            if used < IMAGE_QUOTA_GIB - IMAGE_HEADROOM_GIB:
                return
            if not swept:       # free what nothing uses (Blaxel refuses the rest), once per wait
                swept = True
                try:
                    from .blaxel_images import purge_images
                    await purge_images(2.0)
                except Exception:  # noqa: BLE001
                    pass
                continue
            if loop.time() >= deadline:
                raise RuntimeError(f"Blaxel image storage is full ({used:.0f} GiB of {IMAGE_QUOTA_GIB}) and nothing could be freed")
            await asyncio.sleep(15)

    async def _build_image(self, *, wait_for_concurrent_build: bool) -> None:
        import shutil
        import tempfile

        client = _harbor_blaxel.blaxel_client.get_async_httpx_client()
        name = self._require_image_name()
        await self._wait_for_image_headroom(client)
        with tempfile.TemporaryDirectory(prefix="harbor-blaxel-") as tmp:
            no_slim = self._no_slim_context_file(Path(tmp))
            docker_image = self.task_env_config.docker_image
            image = (self._build_image_from_docker_image(docker_image, no_slim) if docker_image
                     else self._build_image_from_dockerfile(no_slim))
            prepared = image._prepare_for_sandbox(self._sandbox_version)
            build_dir = prepared.write_temp()
            try:
                zip_content = prepared._create_zip(build_dir)
            finally:
                shutil.rmtree(build_dir.parent, ignore_errors=True)
        body = {"name": name, "resourceType": "sandbox", "labels": {"x-blaxel-auto-generated": "true"}}
        r = await _api(client, "post", "/images", params={"upload": "true"}, json=body)
        if r.status_code >= 400:
            if r.status_code == 409 and wait_for_concurrent_build:       # a sibling try is building the same image
                if await self._wait_for_image_built():
                    return
            raise RuntimeError(f"Failed to create Blaxel image {name}: {r.status_code} {r.text[:300]}")
        upload_url = r.headers.get("x-blaxel-upload-url")
        if not upload_url:
            raise RuntimeError(f"Blaxel returned no upload URL for image {name}")
        await image._upload_zip(upload_url, zip_content)
        loop = asyncio.get_running_loop()
        started = loop.time()
        building_since = None
        while True:
            st = await _api(client, "get", f"/images/sandbox/{name}")
            status = (st.json().get("metadata") or {}).get("status") if st.status_code == 200 else None
            if status == "BUILT" and await self._image_built():
                return
            if status == "FAILED":                # fail now: harbor's own wait polls for BUILT until the deadline and holds the slot for 15+ min
                events = [e.get("message", "") for e in (st.json().get("metadata") or {}).get("events", []) if "failed" in e.get("type", "")]
                raise RuntimeError(f"Blaxel image build {name} failed: {(events[-1] if events else '')[:400]}")
            if status == "BUILDING" and building_since is None:
                building_since = loop.time()
            now = loop.time()
            stuck = building_since is not None and now - building_since >= BUILD_TIMEOUT_S
            if stuck or now - started >= BUILD_QUEUE_TIMEOUT_S:
                await self._delete_image()        # best effort: a stuck build's image would otherwise sit in BUILDING and hold quota
                what = f"building for {now - building_since:.0f}s" if stuck else f"queued/building for {now - started:.0f}s"
                raise TimeoutError(f"Blaxel image build {name} not finished after {what}")
            await asyncio.sleep(POLL_INTERVAL_S * (0.75 + 0.5 * random.random()))
