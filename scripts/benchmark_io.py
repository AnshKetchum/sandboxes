#!/usr/bin/env python3
"""Disk, network and CPU of one sandbox per provider, side by side: is a provider's disk the slow part?

For each provider and trial it opens one sandbox, uploads io_workload.py, runs it in each --dir (the root filesystem by
default, plus e.g. a separate workspace mount), and stops the sandbox. The workload measures sequential and 4 KiB random
I/O with O_DIRECT, the fsync rate, a small-file tree, a 100 MB download, a pip install and a CPU loop.
Each sandbox lives at most 20 minutes; cost is a few cents per provider.

    python scripts/benchmark_io.py --provider prime modal --trials 2 [--dir /root/iob --dir /sandbox-workspace/iob]
        [--extra modal:runtime=vm] [--image python:3.12-slim] [--cpu 2 --mem-gb 4 --disk-gb 10] [--quick] [--out FILE]
"""
import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sandboxes import fleet                         # noqa: E402
from sandboxes.raw import SandboxSpec               # noqa: E402

WORKLOAD = Path(__file__).resolve().parent / "io_workload.py"
COLUMNS = ["seq_write_MBps", "seq_read_MBps", "rand_read_4k_iops", "rand_write_4k_iops", "fsync_per_s",
           "small_files_create_sync_s", "small_files_read_s", "small_files_delete_s", "download_MBps",
           "pip_numpy_pandas_s", "cpu_loop_M_per_s"]


async def run_trial(provider: str, extra: dict, args) -> list:
    cls = fleet.raw_class(provider)
    sbx = cls(SandboxSpec(image=args.image, cpu=args.cpu, mem_gb=args.mem_gb, disk_gb=args.disk_gb, lifetime_s=1200,
                          extra=dict(extra)))
    rows = []
    started = time.time()
    await sbx.start()
    try:
        await sbx.put_file(WORKLOAD, "/tmp/io_workload.py")
        for directory in args.dir:
            r = await sbx.exec(f"python3 /tmp/io_workload.py {directory}" + (" --quick" if args.quick else ""),
                               timeout=900)
            try:
                row = json.loads(r.stdout.strip().splitlines()[-1])
            except (ValueError, IndexError):
                row = {"dir": directory, "error": r.stdout[-300:]}
            rows.append({"provider": provider, "start_s": round(time.time() - started, 1), **row})
    finally:
        cost = await sbx.stop()
    for row in rows:
        row["sandbox_cost_usd"] = round(cost, 5)
    return rows


async def main(args) -> None:
    from sandboxes.janitor import load_provider_keys
    load_provider_keys()
    os.environ.setdefault("SANDBOX_RUN_ID", "bench-io-" + time.strftime("%Y%m%d-%H%M%S", time.gmtime()))
    extras = {}
    for item in args.extra:
        provider, _, kv = item.partition(":")
        key, _, value = kv.partition("=")
        extras.setdefault(provider, {})[key] = value
    rows = []
    for provider in args.provider:
        for trial in range(args.trials):
            try:
                found = await run_trial(provider, extras.get(provider, {}), args)
            except Exception as e:  # noqa: BLE001
                found = [{"provider": provider, "error": f"{type(e).__name__}: {str(e)[:200]}"}]
            for row in found:
                row["trial"] = trial + 1
                print(json.dumps(row), flush=True)
            rows += found
    print()
    print(f"{'provider':8} {'dir':22} " + " ".join(f"{c[:14]:>14}" for c in COLUMNS))
    for row in rows:
        print(f"{row['provider']:8} {str(row.get('dir', '-'))[:22]:22} " + " ".join(f"{str(row.get(c, '-'))[:14]:>14}" for c in COLUMNS))
    if args.out:
        Path(args.out).write_text(json.dumps(rows, indent=1))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--provider", nargs="+", required=True)
    parser.add_argument("--trials", type=int, default=1)
    parser.add_argument("--dir", action="append", help="directory to test inside the sandbox (repeatable; default /root/iob)")
    parser.add_argument("--extra", action="append", default=[], help="provider:key=value (e.g. modal:runtime=vm)")
    parser.add_argument("--image", default="python:3.12-slim")
    parser.add_argument("--cpu", type=int, default=2)
    parser.add_argument("--mem-gb", type=int, default=4)
    parser.add_argument("--disk-gb", type=int, default=10)
    parser.add_argument("--quick", action="store_true", help="smaller sizes (256 MiB, 5,000 files, 5 s random)")
    parser.add_argument("--out", help="write every row as JSON here")
    arguments = parser.parse_args()
    arguments.dir = arguments.dir or ["/root/iob"]
    asyncio.run(main(arguments))
