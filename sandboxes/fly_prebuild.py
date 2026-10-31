"""Build task images on Fly ahead of a run, with live progress, so a run starts with every image already in registry.fly.io.

    python -m sandboxes.fly_prebuild /ansh/data/pr-task-synthesis-v0/tasks [--workers 8] [--limit 50] [--filter substr] [--dry-run]

Each task's `environment/` is built in a Fly builder VM (fly_raw._build_image) and pushed as registry.fly.io/<app>:b-<hash of the context>;
a run finds it by that hash, so nothing needs to be recorded. A task already in the registry is skipped. Per-task logs:
~/.cache/sandboxes/fly-build-logs/<task>-<tag>.log. `--workers` is capped by SANDBOX_FLY_BUILD_SLOTS (default 12) across all processes.
"""

from __future__ import annotations

import argparse
import asyncio
import time
from pathlib import Path


async def prebuild(tasks_dir: Path, workers: int, limit: int, name_filter: str, dry_run: bool) -> int:
    from .backends import fly_raw
    from .errors import ImageBuildError
    from .janitor import load_provider_keys
    from .raw import SandboxSpec
    load_provider_keys()
    app = await fly_raw.ensure_app()
    tasks = sorted(p for p in tasks_dir.iterdir() if (p / "environment" / "Dockerfile").exists() and name_filter in p.name)
    pending = []
    for t in tasks:
        env = t / "environment"
        tag = fly_raw.context_tag(str(env / "Dockerfile"), str(env))
        if not await fly_raw._image_exists(app, tag):
            pending.append(t)
    already = len(tasks) - len(pending)
    if limit:
        pending = pending[:limit]
    print(f"{len(tasks)} task(s): {already} already built, {len(pending)} to build now ({min(workers, fly_raw.BUILD_SLOTS)} at a time)", flush=True)
    if dry_run or not pending:
        return 0
    sem = asyncio.Semaphore(min(workers, fly_raw.BUILD_SLOTS))
    done = failed = 0
    t0 = time.time()
    failures: list = []

    async def one(t: Path):
        nonlocal done, failed
        env = t / "environment"
        async with sem:
            s = time.time()
            try:
                await fly_raw._build_image(SandboxSpec(image="", labels={"harness": "fly-prebuild"},
                                                       extra={"dockerfile": str(env / "Dockerfile"), "context_dir": str(env)}), app, "iad")
                done += 1
                status = "ok  "
            except ImageBuildError as e:
                failed += 1
                failures.append((t.name, str(e)[:200]))
                status = "FAIL"
            n = done + failed
            eta = (time.time() - t0) / n * (len(pending) - n)
            print(f"[{n}/{len(pending)}] {status} {t.name} ({time.time() - s:.0f}s)  elapsed {time.time() - t0:.0f}s  eta ~{eta:.0f}s", flush=True)

    await asyncio.gather(*(one(t) for t in pending))
    print(f"\nbuilt {done}, failed {failed} in {time.time() - t0:.0f}s")
    for name, err in failures:
        print(f"  FAIL {name}: {err}")
    return 1 if failed else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("tasks_dir", type=Path)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--filter", default="")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    return asyncio.run(prebuild(a.tasks_dir, a.workers, a.limit, a.filter, a.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
