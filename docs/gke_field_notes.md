# GKE sandboxes: field notes from greenhouse evolve (2026-10-30)

> Status, later on 2026-10-30: the local-SSD fix proposed below is applied (catalog pools with `local_ssd`), and so are the
> other findings here; measurements after the fix, and what to expect now, are in [gke.md](gke.md) ("What to expect",
> "Performance and scale"). These notes are kept as the record of what the first production runs found.

Notes on the GKE backend (branch gke-backend, worktree /ansh/sandboxes/.claude/worktrees/gke-backend; its
docs/gke.md describes the backend itself), from running greenhouse evolve on it. Commits named below are on that
branch.

Five greenhouse evolve runs (r-20261030-NS1d1..3, NS2b, NS2c, then SP1) ran on this backend for ~5 h: up to ~45 sandbox
pods of 2 vCPU / 4 GiB / 20 GB (`cpu_request` 1.0, spot preferred) on one n2d-standard-96 spot node, with an
e2-standard-32 spot node also running. What it took and what it showed:

## Sandbox disk is the bottleneck

A pod writes its workspace to overlay on the node's boot disk (pd-balanced, network-attached, shared by every pod on the
node). On the same task (one oracle run of greenhouse t-0035; `oracle_timing.py` in
/ansh/data/greenhouse-evo-benchmark/experiments/2026-10-30-north-star), one sandbox per backend:

| step | GKE (n2d-96-spot, pd-balanced) | Modal |
|---|---|---|
| open (image cached) | 7.1 s | 4.4 s |
| exec round trip | 0.20 s | 0.05 s |
| the task's solve.sh | 12.7-15.3 s | 6.9-7.9 s |
| the task's test.sh | 9.8-16.2 s | 3.4-5.3 s |
| python CPU loop (10^7 steps) | 0.56-0.58 s | 0.29-0.51 s |
| 256 MB write + fsync | 0.84 s | 0.09 s |
| 3,000 small files write+sync+stat+read | 1.43 s | 0.21 s |

Pod start was 1-2 s and the nodes were 6-20% busy, so neither CPU contention nor scheduling explains it. In the evolve run
logs, an oracle step (put files, run solve.sh, run the tests) took a median 85 s on GKE vs 46 s on Modal, and 56% of the
implementer's time. File-heavy workloads (builds, pip installs, test suites) pay this on every run.

**Fix (not applied yet): put pod storage on local NVMe SSD.** `--ephemeral-storage-local-ssd count=N` on a node pool moves
the container writable layers and emptyDir onto local SSDs. A pool's disk cannot change in place, so this is a new pool (or a
replace of an empty one), for example:

    gcloud container node-pools create n2d-96-lssd-spot --cluster sandboxes --location us-central1-a \
      --project agentic-rollouts --machine-type n2d-standard-96 --spot --ephemeral-storage-local-ssd count=16 \
      --num-nodes 0 --enable-autoscaling --total-min-nodes 0 --total-max-nodes 2 --location-policy ANY \
      --disk-type pd-balanced --disk-size 200 \
      --node-labels sandboxes.pool=n2d-96-lssd-spot --node-taints sandboxes.pool=n2d-96-lssd-spot:NoSchedule \
      --workload-metadata GKE_METADATA --enable-image-streaming

Before applying it:
- **SSD count.** gcloud accepts only certain local-SSD counts per machine type; 16 is unchecked, and gcloud rejects an
  invalid count and lists the valid ones.
- **CPU quota.** The regional quota is 200 vCPUs, and n2d-96-spot plus n2d-96-od already allow two nodes each. Lower
  n2d-96-spot's `max_nodes` to 0 while the new pool runs, or replace n2d-96-spot.
- **Catalog.** Add the pool to `configs/gke.yaml` `pools:`, otherwise `gke_ops pools apply` deletes it as unknown. Give
  it a `local_ssd` field, and have `gke_ops.create_pool_args` pass `--ephemeral-storage-local-ssd count=<local_ssd>`.
- **Prices.** Local SSDs bill per GB while the node runs. Re-run `scripts/gke/prices.py` so placement and the meter
  count them.

Re-measure with `oracle_timing.py` afterwards; the target is Modal's numbers above.

## Credentials

- Builds, the registry and kubectl all use the operator's gcloud login: `gcloud auth login --update-adc` (user
  credentials and ADC). The org enforces `iam.disableServiceAccountKeyCreation`, so a service-account key file is not
  an option. A refresh token from a normal login keeps working; when the account needs re-authentication, everything
  fails at once. `gcloud_watch.sh` in the experiment dir checks `gcloud auth print-access-token` every 5 minutes and
  prints `GCLOUD_LOGIN_NEEDED` once per failure.
- The registry token was cached in-process for 30 minutes and expired mid-run. The resulting 401s failed every sandbox
  open of that process: four greenhouse units were reverted with "precheck failed". Since 9f8e3d0, a registry 401 fetches
  a fresh gcloud token and retries once.

## Builds

- Cloud Build's docker builder used the legacy Dockerfile frontend, which cannot parse `RUN <<EOF` heredocs. Since
  c4b9cf1, builds pass `--build-arg BUILDKIT_SYNTAX=docker/dockerfile:1`.
- A failed build shows up in the caller as a sandbox that cannot open. Read the Cloud Build history (`gcloud builds list`)
  for the pattern before blaming the backend. On 2026-10-30, ten failures in a row were greenhouse rewriting the task's
  Dockerfile (dropping the COPY lines a chmod needed), not GKE.

## Running greenhouse against it

- PYTHONPATH: the greenhouse checkout, the gke-backend worktree (/ansh/sandboxes/.claude/worktrees/gke-backend), and a directory holding the kubernetes Python client
  (`pip install --target DIR kubernetes`; greenhouse's venv does not ship it). Also gcloud on PATH, and the provider keys
  file sourced. The experiment's `launch_evolve_gke.sh` asserts all of these before starting.
- Sandbox config used: `base: configs/gke.yaml` with cpu 2, memory_mb 4096, disk_gb 20, and `extra: {cpu_request: 1.0,
  spot: prefer, ready_timeout_s: 1200, ephemeral_request_gb: 5}`.
- Run `python -m sandboxes.gke_ops watch` beside the run. After killing a greenhouse process, `python -m sandboxes stop
  --run R` closes its pods.

## Known limit

Pod storage is overlay on the node's network boot disk: 7-9x slower than Modal's local disk. Use a local-SSD pool for
file-heavy workloads.
