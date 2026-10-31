"""Blaxel's image store: sweep the `harbor-img-*` images (and their builder sandboxes) that harbor leaves behind.

harbor builds one content-addressed image per task and never deletes it; Blaxel caps image storage (200 GiB), after which every build
fails with 429 QUOTA_EXCEEDED (it took ~110 tasks). LabelledBlaxelEnvironment deletes an image 10 minutes after its last sandbox stops,
but that runs inside the process, so a killed run leaves them. This is the backstop the janitor calls.

Blaxel refuses to delete an image a sandbox still uses ("image in use"), so a live run's images are safe: those deletes just fail and
are skipped. Builder sandboxes (named like the image, unlabelled) are deleted first because they are what keeps an image "in use".
"""

from __future__ import annotations

import asyncio
import datetime as dt

PREFIX = "harbor-img-"


def _age_min(ts: str) -> float:
    return (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))).total_seconds() / 60


async def purge_images(older_than_min: float = 30.0, dry_run: bool = False) -> dict:
    """Delete `harbor-img-*` builder sandboxes and images older than `older_than_min`. Returns counts."""
    from .compat import ensure_blaxel_importable
    from .janitor import load_provider_keys
    load_provider_keys()
    ensure_blaxel_importable()
    import blaxel.core.client as client
    from blaxel.core import SandboxInstance

    http = client.get_async_httpx_client()
    sem = asyncio.Semaphore(8)
    out = {"builders": 0, "images": 0, "in_use": 0}

    async def guarded(coro):
        async with sem:
            return await coro

    old_sandboxes = [s.metadata.name for s in await SandboxInstance.list()
                     if s.metadata.name.startswith(PREFIX) and s.metadata.created_at and _age_min(str(s.metadata.created_at)) > older_than_min]
    if not dry_run:
        await asyncio.gather(*(guarded(SandboxInstance.delete(n)) for n in old_sandboxes), return_exceptions=True)
    out["builders"] = len(old_sandboxes)

    images = [i["metadata"]["name"] for i in (await http.get("/images")).json()
              if i["metadata"].get("resourceType") == "sandbox" and i["metadata"]["name"].startswith(PREFIX)
              and _age_min(i["metadata"].get("createdAt") or i["metadata"].get("updatedAt")) > older_than_min]
    if dry_run:
        out["images"] = len(images)
        return out

    async def delete(name):
        r = await guarded(http.delete(f"/images/sandbox/{name}"))
        if r.status_code < 300:
            out["images"] += 1
        elif "in use" in r.text:
            out["in_use"] += 1
    await asyncio.gather(*(delete(n) for n in images), return_exceptions=True)
    return out
