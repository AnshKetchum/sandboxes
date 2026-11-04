# Prime Intellect sandboxes (`name: prime`)

Backend: `sandboxes/backends/prime_raw.py` (SDK `prime-sandboxes`, `pip install -e ".[prime]"`). Provider docs:
docs.primeintellect.ai/sandboxes/overview.

## Setup

- **Key:** `PRIME_API_KEY` is the API key (the SDK's and CLI's own variable name). It needs the sandbox read/write scope.
- **Billing:** `PRIME_TEAM_ID` bills a team's wallet instead of the key's personal one. List, kill and stored images use the
  same team.
- **Wallet check:** a create needs one hour of the sandbox's runtime in the wallet; otherwise it gets HTTP 402. The backend
  raises that as a `CapacityError` naming the wallet, so a router falls back to another provider.
- **Config:** `configs/prime.yaml`, sized like the e2b and daytona configs (2 vCPU / 4 GiB / 10 GB).

## What the backend does

- **Images:** a registry reference boots as is: Docker Hub (`python:3.12-slim`) or Prime (`prime/<owner>/<name>:<tag>`).
  Other registries must be copied in first with `prime images push --source-image`. Prime refuses Google-hosted registries,
  so GKE's Cloud Build images cannot be used directly.
- **Dockerfiles:** `extra["dockerfile"]` (and `["context_dir"]`) is built by Prime's cloud builder. The image is named
  `sandboxes-ctx` and tagged with `buildctx.context_digest`, so any process finds an earlier build by its tag. The build
  context is packed the way the `prime` CLI packs it: a tar.gz with the Dockerfile at `.__prime_dockerfile__`.
  - Boot a finished build by its `display_ref`. Its `full_image_path` is the raw rootfs artifact, and booting that starts a
    second VM build that fails with `VM_IMAGE_BUILD_FAILED`.
  - Image boot failures raise `ImageBuildError`, which is not retried.
- **Lifetime:** `timeout_minutes` is enforced by Prime (`hard_lifetime = True`).
- **Labels:** Prime labels are plain strings, so a label dict travels as `key=value` strings. `list_live` lists by
  `sandboxes.managed=true` and decodes them.
- **Exec:** commands run as `bash -lc` as root, with `working_dir`, `env` and `timeout` passed through. A 7-minute command
  returned normally, so `max_call_s` is unset. An image without bash (Alpine) will not work.
- **Files:** `upload_bytes` creates parent directories; downloads go through a temp file.
- **Costs:** `CostModel("prime")` bills list prices per vCPU-, GiB-RAM- and GiB-disk-hour. `stored_artifacts` reports the
  `sandboxes-ctx` images ($0.10/GiB-month).
- **Not tested:** docker-in-docker (`dind: False`).

## Scripts

Each script takes `--env-file` (a file with `PRIME_API_KEY` or `PRIME_INTELLECT_API_KEY`, used when the shell has none),
`--team` (the team to bill) and `--ledger` (where costs go). The key is never printed.

```bash
python scripts/prime/live_check.py --team T --env-file .env [--long]       # every primitive, plus a Dockerfile build and its reuse (~$0.01)
python scripts/prime/task_image_check.py TASK/environment --team T --cmd "cd /app && ls"   # build and boot a real task image
python scripts/benchmark_concurrency.py --provider prime --concurrencies 1 10 50 100 --hold 30 --budget-usd 1.5
python scripts/benchmark_concurrency.py --provider modal --extra runtime=vm --concurrencies 1 10 50 --hold 30
python scripts/compare_rates.py scripts/benchmark_results/bench-*.json     # $/hour per provider, and $/hour per unit of CPU work
python scripts/benchmark_io.py --provider prime modal --trials 2 --extra modal:runtime=vm [--quick]   # disk, network, CPU per sandbox
python scripts/prime/cleanup.py --team T [--delete-images] [--kill-run RUN] # what we hold on Prime; delete built images
```

`benchmark_concurrency.py` reads `PRIME_API_KEY` and `PRIME_TEAM_ID` from the environment.

## Measured (2026-11-04)

Settings: `scripts/benchmark_concurrency.py`, 2 vCPU / 4 GiB / 10 GB, `python:3.12-slim`, 30 s CPU burn.

| provider | $/h | N | ok | first command p50 / max | CPU burn p50 |
|---|---|---|---|---|---|
| prime | 0.092 | 1 / 10 / 50 / 100 | all | 2.6 / 3.1 / 4.5 / 9.1 s, max 13.6 s | 998M / 916M / 448M / 447M |
| modal (`runtime: vm`) | 0.380 | 1 / 10 / 50 | all | 0.5 / 0.9 / 5.7 s, max 10.8 s | 1,059M / 746M / 730M |

- **Cost per unit of CPU work:** Prime is 3.9x cheaper at N=1, 5.1x at N=10 and 2.5x at N=50.
- **CPU at scale:** Prime's per-sandbox CPU halved from N=50. This was one run; repeat it before relying on it.

Other timings from the same day:
- smoke test: create to RUNNING 3.1 s, exec round trip 0.21 s, 5 MiB upload / download 1.0 s / 0.9 s, delete 0.85 s;
- a small Dockerfile: build and first start 38 s, then 2-3 s;
- a real greenhouse task image (ubuntu, apt, BuildKit heredocs): 92 s to build and start.

The full write-up, scripts and raw results are in `/ansh/prime-tests/` (RESULTS.md).

## Disk, network and per-call overhead (2026-11-04)

Settings: `scripts/benchmark_io.py`, one sandbox per trial, 2 vCPU / 4 GiB / 10 GB, `python:3.12-slim`; ranges cover 2 full
and 2 quick trials.

**Disk.** Prime's disk is a virtio ext4 volume (`/dev/vda`, the whole root filesystem; `/sandbox-workspace` is not a
separate mount).

| | Prime | Modal `runtime: vm` |
|---|---|---|
| sequential write, O_DIRECT | 0.9-1.3 GB/s | 3.2-5.8 GB/s |
| fsyncs per second | 1,900-7,400 (noisy) | 7,200-9,900 |
| 20,000 small files: create + sync / cold read / delete | 0.26-0.34 / 0.35-0.51 / 0.08-0.10 s | 1.17-1.36 / 0.76-1.45 / 0.36-0.41 s |
| 4 KiB random read / write IOPS (one at a time) | 56-94k / 11-69k | 40-58k / 38-50k |

- Small-file work, the kind that made GKE's network-disk pods slow (`docs/gke.md`), is 2.5-4x faster on Prime than on
  Modal, and far ahead of GKE's pd-balanced numbers.
- No local-NVMe work is needed. Prime does not offer a disk choice anyway.

**Network and per-call overhead, the real gap.**

| | Prime | Modal `runtime: vm` |
|---|---|---|
| download from PyPI's CDN | 14.6-95.7 MB/s (varies by sandbox) | 140-142 MB/s |
| `pip install numpy pandas` | 4.4-9.6 s | 5.5-5.7 s |
| exec round trip | 0.24 s | 0.06 s |
| DNS lookup / HTTPS request (incl. exec) | 0.25-0.46 s | 0.06-0.12 s |
| upload a task's `solution/` + `tests/` (two `put_file` calls) | 3.0 s | 0.2 s |
| open from an already-built image | 7.8 s | 1.1 s |

A greenhouse oracle (t-0019) splits the same way:
- the task's own `solve.sh` took 6.9 s on Prime and 7.2 s on Modal;
- `test.sh` took 35.6 s and 32.8 s; it downloads its test tools.

Prime's extra time sits in opening, uploads, and command and network latency.

**Where to optimize if it matters:**
- fewer exec calls per step, batching small commands;
- one tarball upload per step instead of several `put_file` calls;
- test tooling preinstalled in task images rather than downloaded by every `test.sh`.

## Pricing and limits (launch prices valid through 2026-12-22)

**Pricing:**
- $0.02 per vCPU-hour, $0.0125 per GiB-RAM-hour, $0.0002 per GiB-disk-hour.
- Images and checkpoints: $0.10 per GiB-month, plus one day of storage at upload.
- A paused sandbox bills its disk only.

**Limits:**
- **Per sandbox:** at most 16 vCPU, 64 GiB RAM and 128 GiB disk.
- **Per account, by default:** 1,024 active sandboxes, 4,096 vCPUs, 192 creations per 10 s, 102,400 creations per hour.
