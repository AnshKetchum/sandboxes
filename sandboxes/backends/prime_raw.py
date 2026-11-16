"""Prime Intellect: microVM sandboxes through the `prime_sandboxes` SDK, from a Docker Hub image, a Prime image (`prime/...`), or a
task's Dockerfile that Prime's cloud builder turns into a Prime image. Billed per vCPU-, GiB-RAM- and GiB-disk-hour; the lifetime
(`timeout_minutes`) is enforced by Prime. A create needs one hour of the sandbox's runtime in the wallet (else HTTP 402).

Credentials: PRIME_API_KEY (the SDK's own variable). PRIME_TEAM_ID (or spec.extra["team_id"]) bills a team's wallet instead of the
personal one, and is also where list/kill look. Labels are plain strings on Prime, so a label dict travels as "key=value" strings.
Images from registries other than Docker Hub must be copied in first (`prime images push --source-image`); Google-hosted
registries are refused. A Dockerfile (spec.extra["dockerfile"], ["context_dir"]) is built once per context digest
(buildctx.context_digest) and found again by that tag from any process, so the 3 tries of a task and later runs reuse it.
"""

from __future__ import annotations

import asyncio
import os
import shlex
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Optional

from ..buildctx import context_digest, resolve_dockerfile
from ..errors import CapacityError, ImageBuildError
from ..raw import ExecResult, LiveSandbox, RawSandbox, StoredArtifact

IMAGE_NAME = "sandboxes-ctx"                    # every Dockerfile build lands under this image name, tagged by its context digest
PACKAGED_DOCKERFILE = ".__prime_dockerfile__"   # where Prime's builder reads the Dockerfile inside the context tarball (as the prime CLI packs it)
BUILD_POLL_S = 10
BUILD_TIMEOUT_S = 1800
MANAGED_LABEL = "sandboxes.managed=true"
_builds: dict = {}          # context digest -> the Task building (or finding) its image, shared by every sandbox of that context in this process


def get_team_id(extra: Optional[dict] = None) -> Optional[str]:
    return (extra or {}).get("team_id") or os.environ.get("PRIME_TEAM_ID") or None


def make_client():
    from prime_sandboxes import AsyncSandboxClient
    return AsyncSandboxClient(api_key=os.environ.get("PRIME_API_KEY"))


def encode_labels(labels: dict) -> list:
    return [f"{k}={v}" for k, v in (labels or {}).items()]


def decode_labels(labels: list) -> dict:
    return dict(label.split("=", 1) if "=" in label else (label, "") for label in labels or [])


def to_capacity_error(e: Exception) -> Optional[CapacityError]:
    """A refusal that means "Prime cannot take this sandbox now", so the router can send it elsewhere; None for other errors."""
    from prime_sandboxes import PaymentRequiredError
    text = f"{type(e).__name__}: {e}"
    if isinstance(e, PaymentRequiredError) or "402" in text or "insufficient funds" in text.lower():
        team = get_team_id()
        return CapacityError("prime", f"insufficient funds on the {'team' if team else 'personal'} wallet "
                                      f"(a create needs one hour of runtime; set PRIME_TEAM_ID to bill a team): {text[:150]}")
    low = text.lower()
    if "429" in text or "rate limit" in low or "too many" in low:
        return CapacityError("prime", f"rate limit: {text[:200]}")
    if "limit" in low and ("sandbox" in low or "concurren" in low or "quota" in low):
        return CapacityError("prime", text[:200])
    return None


def pack_context(context_dir: str, dockerfile: Path) -> str:
    """The build context as the prime CLI uploads it: a tar.gz of the directory with the Dockerfile at PACKAGED_DOCKERFILE.
    Returns a temp path the caller removes. (.dockerignore files are not applied: task contexts are small and exact.)"""
    fd, path = tempfile.mkstemp(suffix=".tar.gz")
    os.close(fd)
    with tarfile.open(path, "w:gz") as tar:
        tar.add(context_dir, arcname=".")
        tar.add(str(dockerfile), arcname=PACKAGED_DOCKERFILE)
    return path


async def find_built_image(images, tag: str, team_id: Optional[str]):
    """The image row this process or another one already made for `tag`, if any."""
    rows = (await images.list(team_id=team_id, search=IMAGE_NAME, limit=100)).data
    return next((row for row in rows if row.image_name == IMAGE_NAME and row.image_tag == tag), None)


async def build_image(context_dir: str, dockerfile: Path, team_id: Optional[str]) -> str:
    """The Prime image reference for this context: an existing build of its digest, else a new cloud build. Raises
    ImageBuildError when the build fails or runs past BUILD_TIMEOUT_S."""
    import httpx
    from prime_sandboxes import AsyncAPIClient, AsyncImageClient, BuildImageRequest

    tag = context_digest(context_dir, str(dockerfile))
    images = AsyncImageClient(AsyncAPIClient(api_key=os.environ.get("PRIME_API_KEY")))
    try:
        row = await find_built_image(images, tag, team_id)
        if row is None or row.status.value in ("FAILED", "CANCELLED"):
            path = pack_context(context_dir, dockerfile)
            try:
                build = await images.initiate_build(BuildImageRequest(
                    image_name=IMAGE_NAME, image_tag=tag, dockerfile_path=PACKAGED_DOCKERFILE, platform="linux/amd64",
                    team_id=team_id))
                async with httpx.AsyncClient(timeout=600) as http:
                    with open(path, "rb") as f:
                        (await http.put(build.upload_url, content=f.read(),
                                        headers={"Content-Type": "application/octet-stream"})).raise_for_status()
                await images.start_build(build.build_id)
            finally:
                os.unlink(path)
        deadline = time.time() + BUILD_TIMEOUT_S
        while time.time() < deadline:
            row = await find_built_image(images, tag, team_id)
            status = row.status.value if row else "PENDING"
            if status == "COMPLETED":
                # display_ref is the bootable `prime/<owner>/<name>:<tag>`; full_image_path is the raw rootfs artifact, and a
                # create from it starts a second VM build that fails (VM_IMAGE_BUILD_FAILED)
                return row.display_ref or f"prime/{f'team-{team_id}' if team_id else row.full_image_path.split('/')[0]}/{IMAGE_NAME}:{tag}"
            if status in ("FAILED", "CANCELLED"):
                raise ImageBuildError(f"prime: image build {status.lower()} for {dockerfile}: {(row.error_message or '')[:1500]}")
            await asyncio.sleep(BUILD_POLL_S)
        raise ImageBuildError(f"prime: image build for {dockerfile} did not finish in {BUILD_TIMEOUT_S} s")
    finally:
        await images.aclose()


class PrimeRaw(RawSandbox):
    provider = "prime"
    hard_lifetime = True           # `timeout_minutes` at create ends the sandbox

    def __init__(self, spec, model=None):
        super().__init__(spec, model)
        self._client = None

    @classmethod
    async def list_live(cls):
        out, page = [], 1
        async with make_client() as client:
            while True:
                listed = await client.list(team_id=get_team_id(), labels=[MANAGED_LABEL], exclude_terminated=True,
                                           page=page, per_page=100)
                for sb in listed.sandboxes:
                    created = sb.started_at or sb.created_at
                    out.append(LiveSandbox("prime", sb.id, created.timestamp() if created else None, str(sb.status),
                                           decode_labels(sb.labels)))
                if not listed.has_next:
                    return out
                page += 1

    @classmethod
    async def kill(cls, sandbox_id):
        async with make_client() as client:
            try:
                await client.delete(sandbox_id)
            except Exception as e:  # noqa: BLE001
                text = str(e).lower()
                if "not found" not in text and "404" not in text and "terminated" not in text:
                    raise

    @classmethod
    async def stored_artifacts(cls):
        from prime_sandboxes import AsyncAPIClient, AsyncImageClient
        images = AsyncImageClient(AsyncAPIClient(api_key=os.environ.get("PRIME_API_KEY")))
        try:
            rows = (await images.list(team_id=get_team_id(), search=IMAGE_NAME, limit=100)).data
        finally:
            await images.aclose()
        return [StoredArtifact("prime", "image", row.id, (row.size_bytes or 0) / 2 ** 30,
                               row.created_at.timestamp() if row.created_at else None)
                for row in rows if row.image_name == IMAGE_NAME and row.status.value == "COMPLETED"]

    async def get_image(self) -> str:
        s = self.spec
        dockerfile = s.extra.get("dockerfile")
        if not dockerfile:
            return s.image
        context_dir = str(s.extra.get("context_dir") or Path(dockerfile).parent)
        path = resolve_dockerfile(context_dir, str(dockerfile))
        key = context_digest(context_dir, str(dockerfile))
        if key not in _builds:
            _builds[key] = asyncio.ensure_future(build_image(context_dir, path, get_team_id(s.extra)))
        return await asyncio.shield(_builds[key])

    async def _create(self) -> None:
        from prime_sandboxes import CreateSandboxRequest, SandboxImagePullError
        s = self.spec
        image = await self.get_image()
        self._client = self._client or make_client()
        request = CreateSandboxRequest(
            name=f"sbx-{self.env_id[:12]}", docker_image=image, cpu_cores=float(s.cpu), memory_gb=float(s.mem_gb),
            disk_size_gb=float(s.disk_gb), timeout_minutes=max(1, int(s.lifetime_s) // 60),
            labels=encode_labels({"sandboxes": "1", **(s.labels or {})}),
            environment_vars={k: str(v) for k, v in (s.env or {}).items()} or None,
            team_id=get_team_id(s.extra), region=s.extra.get("region"), idempotency_key=self.env_id)
        try:
            sandbox = await self._client.create(request)
        except Exception as e:  # noqa: BLE001
            capacity = to_capacity_error(e)
            if capacity is not None:
                raise capacity from e
            raise
        try:
            await self._client.wait_for_creation(sandbox.id, max_attempts=max(60, int(s.create_timeout) // 2))
        except BaseException as e:
            await self.delete_quietly(sandbox.id)        # start() retries _create: a half-made sandbox must not be left billing
            if isinstance(e, SandboxImagePullError) or "IMAGE_BUILD_FAILED" in str(e) or "IMAGE_PULL" in str(e):
                raise ImageBuildError(f"prime: cannot boot {image}: {str(e)[:300]}") from e     # the same image fails again: no retry
            raise
        self.sandbox_id = sandbox.id

    async def delete_quietly(self, sandbox_id: str) -> None:
        try:
            await self._client.delete(sandbox_id)
        except Exception:  # noqa: BLE001 — the lifetime is the backstop
            pass

    async def _exec(self, command, cwd, env, timeout) -> ExecResult:
        r = await self._client.execute_command(self.sandbox_id, f"bash -lc {shlex.quote(command)}", working_dir=cwd,
                                               env={k: str(v) for k, v in (env or {}).items()} or None, timeout=int(timeout))
        return ExecResult(r.exit_code, (r.stdout or "") + (r.stderr or ""))

    async def _upload(self, data: bytes, remote_path: str) -> None:
        await self._client.upload_bytes(self.sandbox_id, remote_path, data, Path(remote_path).name, timeout=600)

    async def _download(self, remote_path: str) -> bytes:
        fd, local = tempfile.mkstemp()
        os.close(fd)
        try:
            await self._client.download_file(self.sandbox_id, remote_path, local, timeout=600)
            return Path(local).read_bytes()
        finally:
            os.unlink(local)

    async def _delete(self) -> None:
        try:
            await self._client.delete(self.sandbox_id)
        finally:
            await self._client.aclose()
            self._client = None
