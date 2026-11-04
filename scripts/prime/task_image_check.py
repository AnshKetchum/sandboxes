#!/usr/bin/env python3
"""Build a task's environment directory (a Dockerfile and its context, e.g. a harbor task's environment/) on Prime, boot it,
and run a few commands in it. The directory is copied first, so the source is never touched.

    python scripts/prime/task_image_check.py ENV_DIR --team TEAM_ID [--env-file .env] [--ledger costs.jsonl]
        [--cmd "cd /app && ls"] [--cmd ...] [--cpu 2 --mem-gb 4 --disk-gb 10]
"""
import argparse
import asyncio
import json
import shutil
import tempfile
import time
from pathlib import Path

from common import add_common_args, apply_common_args


async def main(args) -> None:
    from sandboxes.backends.prime_raw import PrimeRaw
    from sandboxes.raw import SandboxSpec
    ctx = Path(tempfile.mkdtemp()) / "environment"
    shutil.copytree(args.env_dir, ctx)
    sbx = PrimeRaw(SandboxSpec(image="", cpu=args.cpu, mem_gb=args.mem_gb, disk_gb=args.disk_gb, lifetime_s=1800,
                               extra={"dockerfile": str(ctx / "Dockerfile"), "context_dir": str(ctx)}))
    results, t = {"commands": []}, time.time()
    await sbx.start()
    results["build_and_start_s"] = round(time.time() - t, 1)
    try:
        for command in args.cmd or ["cat /etc/os-release | head -1; pwd; ls | head -20"]:
            r = await sbx.exec(command, timeout=args.timeout)
            results["commands"].append({"cmd": command, "rc": r.rc, "tail": r.stdout.strip().splitlines()[-8:]})
    finally:
        results["cost_usd"] = round(await sbx.stop(), 5)
    print(json.dumps(results, indent=1))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("env_dir", help="directory with the Dockerfile and its build context")
    add_common_args(parser)
    parser.add_argument("--cmd", action="append", help="a command to run in the booted sandbox (repeatable)")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--cpu", type=int, default=2)
    parser.add_argument("--mem-gb", type=int, default=4)
    parser.add_argument("--disk-gb", type=int, default=10)
    arguments = parser.parse_args()
    apply_common_args(arguments, "prime-task-image")
    asyncio.run(main(arguments))
