#!/usr/bin/env python3
"""Live check of the `prime` backend: one small sandbox through every primitive, then a Dockerfile build reused by a second
sandbox and by a new process (found by its digest tag). Each sandbox has a 30-minute lifetime and is stopped in a `finally`.
Cost: about $0.01 (plus $0.006 with --long).

    python scripts/prime/live_check.py --team TEAM_ID [--env-file .env] [--ledger costs.jsonl] [--long] [--skip-build]

--long adds a 7-minute command (checks that exec has no per-call cap). Results print as JSON and go to --out if given.
"""
import argparse
import asyncio
import json
import os
import tempfile
import time
from pathlib import Path

from common import add_common_args, apply_common_args

RESULTS = {}


def note(key, value):
    RESULTS[key] = value
    print(f"{key}: {value}", flush=True)


async def check_basics(long_command: bool) -> None:
    from sandboxes.backends.prime_raw import PrimeRaw
    from sandboxes.raw import SandboxSpec
    sbx = PrimeRaw(SandboxSpec(image="python:3.12-slim", cpu=1, mem_gb=2, disk_gb=5, lifetime_s=1800))
    t = time.time()
    await sbx.start()
    note("start_s", round(time.time() - t, 2))
    try:
        r = await sbx.exec("echo out; echo err >&2; exit 7")
        note("exit_code_and_streams", (r.rc, r.stdout.split()))
        r = await sbx.exec("pwd; echo $FOO; whoami", cwd="/usr", env={"FOO": "bar baz"})
        note("cwd_env_user", r.stdout.split("\n")[:3])
        t = time.time()
        r = await sbx.exec("sleep 30", timeout=5)
        note("timeout_5s_on_sleep_30", (r.rc, r.stdout[:60], round(time.time() - t, 1)))
        local = Path(tempfile.mkdtemp())
        (local / "a").mkdir()
        (local / "a" / "x.txt").write_text("hello\n")
        (local / "run.sh").write_text("#!/bin/sh\necho ran\n")
        os.chmod(local / "run.sh", 0o755)
        await sbx.put_file(local, "/work/in")
        r = await sbx.exec("cat /work/in/a/x.txt; /work/in/run.sh; mkdir -p /work/out && echo result > /work/out/r.txt")
        note("dir_upload_and_exec_bit", r.stdout.split())
        await sbx.get_file("/work/out", local / "back")
        note("dir_download", (local / "back" / "r.txt").read_text().strip())
        mine = [x for x in await PrimeRaw.list_live() if x.id == sbx.sandbox_id]
        note("list_live_labels", mine[0].labels if mine else "NOT LISTED")
        if long_command:
            t = time.time()
            r = await sbx.exec("sleep 420; echo long-done", timeout=600)
            note("exec_7min", (r.rc, r.stdout.strip()[-20:], round(time.time() - t, 1)))
    finally:
        note("basics_cost_usd", round(await sbx.stop(), 5))


async def check_build() -> None:
    from sandboxes.backends import prime_raw
    from sandboxes.raw import SandboxSpec
    ctx = Path(tempfile.mkdtemp())
    (ctx / "hello.txt").write_text(f"built at {time.time()}\n")        # a fresh digest, so this really builds
    (ctx / "Dockerfile").write_text("FROM python:3.12-slim\nRUN pip install --no-cache-dir requests==2.32.3\n"
                                    "COPY hello.txt /hello.txt\n")
    make_spec = lambda: SandboxSpec(image="", cpu=1, mem_gb=2, disk_gb=5, lifetime_s=1800,     # noqa: E731
                                    extra={"dockerfile": str(ctx / "Dockerfile"), "context_dir": str(ctx)})
    first = prime_raw.PrimeRaw(make_spec())
    t = time.time()
    await first.start()
    note("build_and_start_s", round(time.time() - t, 1))
    try:
        r = await first.exec("cat /hello.txt; python -c 'import requests; print(requests.__version__)'")
        note("built_image_contents", r.stdout.split())
        for label in ("second_start_same_process_s", "start_after_cache_clear_s"):
            if label == "start_after_cache_clear_s":
                prime_raw._builds.clear()                  # as a new process would: find the image by its tag, not build again
            other = prime_raw.PrimeRaw(make_spec())
            t = time.time()
            await other.start()
            note(label, round(time.time() - t, 1))
            await other.stop()
        note("stored_images", [(a.id, round(a.gib, 3)) for a in await prime_raw.PrimeRaw.stored_artifacts()])
    finally:
        await first.stop()


async def main(args) -> None:
    await check_basics(args.long)
    if not args.skip_build:
        await check_build()
    from sandboxes.backends.prime_raw import PrimeRaw
    run_id = os.environ["SANDBOX_RUN_ID"]
    note("leftover_sandboxes", [x.id for x in await PrimeRaw.list_live() if x.labels.get("sandboxes.run") == run_id])
    if args.out:
        Path(args.out).write_text(json.dumps(RESULTS, indent=1, default=str))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_common_args(parser)
    parser.add_argument("--long", action="store_true", help="also run a 7-minute command")
    parser.add_argument("--skip-build", action="store_true", help="skip the Dockerfile build checks")
    parser.add_argument("--out", help="write the results as JSON here")
    arguments = parser.parse_args()
    apply_common_args(arguments, "prime-live-check")
    asyncio.run(main(arguments))
