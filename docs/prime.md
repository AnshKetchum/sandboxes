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

## Pricing and limits (launch prices valid through 2026-12-22)

**Pricing:**
- $0.02 per vCPU-hour, $0.0125 per GiB-RAM-hour, $0.0002 per GiB-disk-hour.
- Images and checkpoints: $0.10 per GiB-month, plus one day of storage at upload.
- A paused sandbox bills its disk only.

**Limits:**
- **Per sandbox:** at most 16 vCPU, 64 GiB RAM and 128 GiB disk.
- **Per account, by default:** 1,024 active sandboxes, 4,096 vCPUs, 192 creations per 10 s, 102,400 creations per hour.
