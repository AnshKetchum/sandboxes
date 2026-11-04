"""The I/O workload benchmark_io.py runs inside a sandbox (stdlib only, so any image with python3 works). Prints one JSON
object: what the disk is, then sequential and random I/O with O_DIRECT (past the page cache), the fsync rate, a
small-file tree, a download, a pip install, and a single-thread CPU loop to tell CPU from I/O.

    python3 io_workload.py DIR [--quick]
"""
import json
import mmap
import os
import random
import shutil
import subprocess
import sys
import time
import urllib.request

DIR = sys.argv[1]
QUICK = "--quick" in sys.argv
SEQ_MIB = 256 if QUICK else 1024
RANDOM_S = 5 if QUICK else 10
SMALL_FILES = 5000 if QUICK else 20000
# a fixed 40 MB wheel on PyPI's CDN: the network pip and uv installs use (speed.cloudflare.com refuses cloud IPs)
DOWNLOAD_URL = ("https://files.pythonhosted.org/packages/3a/2e/3b99f8a3d9e0ccae0e961978a0d0089b25fb46ebbcfb5ebae3cca179a5b3/"
                "pyarrow-18.1.0-cp312-cp312-manylinux_2_28_x86_64.whl")
OUT = {}


def run(cmd: str) -> str:
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()


def describe_disk() -> None:
    OUT["dir"] = DIR
    OUT["df"] = run(f"df -hT {DIR} | tail -1")
    device = run(f"df {DIR} | tail -1").split()[0]
    OUT["device"] = device
    name = os.path.basename(device).rstrip("0123456789") if device.startswith("/dev/") else ""
    OUT["rotational"] = run(f"cat /sys/block/{name}/queue/rotational 2>/dev/null") if name else ""
    OUT["mount"] = run(f"grep ' {run(f'stat -c %m {DIR}')} ' /proc/mounts | head -1")
    OUT["nproc"] = os.cpu_count()


def open_direct(path: str, flags: int):
    """An O_DIRECT descriptor, or a plain one when the filesystem refuses it (tmpfs, overlay on some kernels)."""
    try:
        return os.open(path, flags | os.O_DIRECT, 0o644), True
    except OSError:
        return os.open(path, flags, 0o644), False


def measure_sequential() -> None:
    path = os.path.join(DIR, "seq.bin")
    block = mmap.mmap(-1, 1 << 20)                  # page-aligned, as O_DIRECT needs
    block.write(os.urandom(1 << 20))
    fd, direct = open_direct(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    start = time.time()
    for _ in range(SEQ_MIB):
        os.write(fd, block)
    os.fsync(fd)
    os.close(fd)
    OUT["seq_write_MBps"] = round(SEQ_MIB / (time.time() - start), 1)
    OUT["o_direct"] = direct
    fd, _ = open_direct(path, os.O_RDONLY)
    start = time.time()
    while os.readv(fd, [block]) > 0:
        pass
    os.close(fd)
    OUT["seq_read_MBps"] = round(SEQ_MIB / (time.time() - start), 1)


def measure_random() -> None:
    path = os.path.join(DIR, "seq.bin")
    blocks = (SEQ_MIB << 20) // 4096
    buf = mmap.mmap(-1, 4096)
    for mode, label in ((os.O_RDONLY, "rand_read_4k_iops"), (os.O_WRONLY, "rand_write_4k_iops")):
        fd, _ = open_direct(path, mode)
        done, deadline = 0, time.time() + RANDOM_S
        while time.time() < deadline:
            offset = random.randrange(blocks) * 4096
            if mode == os.O_RDONLY:
                os.preadv(fd, [buf], offset)
            else:
                os.pwritev(fd, [buf], offset)
            done += 1
        os.close(fd)
        OUT[label] = round(done / RANDOM_S)
    fd = os.open(os.path.join(DIR, "fsync.bin"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    done, deadline = 0, time.time() + RANDOM_S / 2
    while time.time() < deadline:
        os.write(fd, b"x" * 4096)
        os.fsync(fd)
        done += 1
    os.close(fd)
    OUT["fsync_per_s"] = round(done / (RANDOM_S / 2))
    os.remove(path)


def measure_small_files() -> None:
    root = os.path.join(DIR, "tree")
    payload = os.urandom(1024)
    start = time.time()
    for i in range(SMALL_FILES):
        sub = os.path.join(root, f"d{i % 200}")
        if i < 200:
            os.makedirs(sub, exist_ok=True)
        with open(os.path.join(sub, f"f{i}.txt"), "wb") as f:
            f.write(payload)
    os.sync()
    OUT["small_files_create_sync_s"] = round(time.time() - start, 2)
    run("sync; echo 3 > /proc/sys/vm/drop_caches")    # read from the disk, not the cache, where allowed
    start = time.time()
    for dirpath, _, files in os.walk(root):
        for name in files:
            with open(os.path.join(dirpath, name), "rb") as f:
                f.read()
    OUT["small_files_read_s"] = round(time.time() - start, 2)
    start = time.time()
    shutil.rmtree(root)
    os.sync()
    OUT["small_files_delete_s"] = round(time.time() - start, 2)


def measure_network() -> None:
    start = time.time()
    try:
        path = os.path.join(DIR, "dl.bin")
        with urllib.request.urlopen(DOWNLOAD_URL, timeout=120) as response, open(path, "wb") as f:
            shutil.copyfileobj(response, f, 1 << 20)
        OUT["download_MBps"] = round(os.path.getsize(path) / 1e6 / (time.time() - start), 1)
    except Exception as e:  # noqa: BLE001
        OUT["download_MBps"] = f"failed: {str(e)[:80]}"
    start = time.time()
    r = subprocess.run([sys.executable, "-m", "pip", "install", "-q", "--no-cache-dir", "--disable-pip-version-check",
                        "--target", os.path.join(DIR, "pip"), "numpy==2.1.3", "pandas==2.2.3"],
                       capture_output=True, text=True)
    OUT["pip_numpy_pandas_s"] = round(time.time() - start, 1) if r.returncode == 0 else f"failed: {r.stderr[-80:]}"


def measure_cpu() -> None:
    n, deadline = 0, time.time() + 3
    while time.time() < deadline:
        n += 1
    OUT["cpu_loop_M_per_s"] = round(n / 3 / 1e6, 2)


os.makedirs(DIR, exist_ok=True)
for step in (describe_disk, measure_cpu, measure_sequential, measure_random, measure_small_files, measure_network):
    try:
        step()
    except Exception as e:  # noqa: BLE001
        OUT[step.__name__ + "_error"] = str(e)[:200]
shutil.rmtree(DIR, ignore_errors=True)
print(json.dumps(OUT))
