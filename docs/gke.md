# GKE sandboxes

One Kubernetes pod per sandbox on our own GKE cluster. A sandbox asks for a size (vCPUs, memory, disk); it lands on the node
pool that runs that size most cheaply; a harbor task's Dockerfile is built once per context and every sandbox of that task runs
the image; pools sit at zero nodes (free) until sandboxes need them; after a run nothing is left that bills.

1,000 sandboxes of 1 vCPU / 4 GiB / 10 GB (disk reserved, not over-committed), us-central1 catalog prices on 2026-10-27
(`python -m sandboxes.gke_place plan --cpu 1 --mem 4 --disk 10 --count 1000 --max-usd-h 75`):

| machine | per node | nodes | $/h with boot disks |
|---|---|---|---|
| c3d-standard-180 spot | 174 | 6 | 13.65 |
| c3d-standard-90 spot | 86 | 12 | 13.81 |
| n2d-standard-96 spot | 91 | 11 | 23.41 |
| c4d-standard-192 spot | 180 | 6 | 26.03 |
| c3d-standard-180 on demand | 174 | 6 | 50.84 |

Plus the cluster fee and a small system node (~$0.17/h), and image builds once per distinct task (~$0.06 each). Modal measured
$0.238 per sandbox-hour at 1 vCPU / 4 GiB: $238/h for the same fleet.

**Quota decides what runs.** In us-central1 the project has CPUS 200 (all families, regional), N2D 200, E2 72, C3D 24, C4D 24
(2026-10-27). The catalog's C3D/C4D pools are `enabled: false` until those are raised (~1,100 C3D vCPUs, and CPUS / CPUS for all
regions to match, for 1,000 sandboxes); today two n2d-standard-96 spot nodes, ~180 sandboxes, is the ceiling.

## Live verification (2026-10-27, agentic-rollouts, us-central1)

`scripts/gke/live_check.py` against the real cluster: each sandbox built from a harbor task's Dockerfile, every exec / file /
network check, the task's verifier on its reference solution, then the bill compared with observed node lives, then `down` and
an independent gcloud listing of everything billable.

| run | sandboxes | checks | reward | cold start | warm start | ledger vs observed | left after down |
|---|---|---|---|---|---|---|---|
| gke-live-1d | 1 (+1 warm) | all pass | 1 | 255 s | 6.0 s | 1.08x (over) | control plane only |
| gke-live-8 | 8 concurrent (+1 warm) | all pass | 1 | 268 s | 6.2 s | 1.13x (over) | control plane only |

Cold start, from the pod's creation: node scale-up ~48 s, image pull ~8 s (one pull shared by all 8 pods), container start at
~62 s; the rest of the 255-270 s is the cluster coming back up from `down` (~2 min) and the Cloud Build (~40 s). Warm (node up,
image on the node): 6 s, all of it our own create path. The VM was what the catalog said: n2d-standard-96, provisioning model
SPOT, 1,250 GB pd-balanced. Builds: ledger = Cloud Build's own duration x rate, to the cent. The ledger's excess over observed
node lives is measurement edges (the observer polls every 10 s, the meter charges to the deletion event; the cluster fee
between runs), always over, never under.

Found only on the real cluster, fixed, each with a test that would have caught it:
- BuildKit rejects `--network=cloudbuild`
- NodeLocal DNSCache (on by default) put DNS outside the network policy: sandboxes had no DNS
- the Kubernetes client's `read_all()` empties the exit-status channel: every exit code came back None
- `kubernetes.stream` swaps `request` on the shared client: concurrent API calls failed, overlapping execs could break it for good
- GKE runs one cluster operation at a time (a second pool resize failed); its first system-pool resize after an autoscaling
  change surge-replaces the node instead of removing it
- deleting an image leaves its layers billed in Artifact Registry
- a deleted node was charged until the next meter pass instead of its deletion event
- pods saw the node's 96 CPUs (`nproc`): sandboxes are now whole-CPU Guaranteed pods on static-CPU-manager pools (`nproc` = 1)

Still visible inside a sandbox: the node's total memory and disk (`free`, `df`); limits are enforced (OOM at 4 GiB, eviction
past 10 GB) but not reflected there.

## Placement (sandboxes/gke_place.py)

A sandbox's cost on a pool = the node's price (machine + boot disk) / how many sandboxes of its size fit a node: GKE's
allocatable (CPU: 6% of the first core, 1% of the next, 0.5% of the next two, 0.25% above; memory: 25% of the first 4 GiB, 20%
of the next 4, 10% of the next 8, 6% of the next 112, 2% above, plus 100 MiB) minus the DaemonSets (`node_overhead`), then the
tightest of CPU, memory, disk and the pod limit. Each start: room on a running node first (already paid for), else the pool
cheapest per sandbox that can still grow (its `max_nodes`), spot preferred, on demand as the fallback. A pool whose pod stays
unschedulable for 300 s (spot stockout, quota) is cooled down for 10 min and the next pool tried; only when every pool that
fits has failed does the start raise CapacityError (the router then tries another provider). The sandbox's ledger entry
records its share of the node; the meter charges the rest of the node (idle room) to run "".

The catalog is `gke.pools` in configs/gke.yaml; `python -m sandboxes.gke_ops pools plan|apply` makes the cluster match it
(creates pools at 0 nodes, updates ceilings; replacing or deleting a pool needs `--replace` / `--prune` and an empty pool).
`python scripts/gke/prices.py --pools --write` refreshes machine shapes and prices from Compute Engine and the Billing Catalog.

## Dockerfiles (sandboxes/gke_build.py)

GKE is a `builds_dockerfile` provider: a sandbox opened with `extra.dockerfile` + `extra.context_dir` (what harbor_raw passes for
a task without a prebuilt image) runs `<gke.registry>/ctx:<digest>`, the digest being the content hash euphoria and greenhouse
tag their images with. Missing, it is built by Cloud Build (BuildKit; the task's own Dockerfile path; logs in Cloud Logging),
once: concurrent starts of one task wait for the one build (per process, and per machine through a lock), at most
`max_concurrent` builds at a time. The sandbox then has its whole 1 vCPU / 4 GiB / 10 GB: image layers live in the node's shared
image cache, not in the sandbox's disk. A failed build raises ImageBuildError with the build log's tail.

## Nothing left that bills

- build minutes: charged when each build ends (`image_build`)
- build source uploads: deleted when the build ends; the bucket's lifecycle rule deletes leftovers after a day
- images: every push recorded; deleted by `down`, by the watchdog once no pod runs it and it is unused for `image_ttl_h`, and
  by the repository's cleanup policy after `cleanup_days` whatever happens to this machine. Storage is charged for each image's
  whole life when it is deleted (or found deleted by the policy)
- nodes: sandbox pools go to 0 after `idle_scale_down_min`; `down` (also automatic after `auto_down_min` idle) takes every pool
  to 0, the system pool too; a sandbox start on a cluster that is down brings it back up (`auto_up`, ~2 min)
- `python -m sandboxes.gke_ops residue` lists everything in the project that bills while idle (clusters, VMs, disks and
  orphaned disks, snapshots, VM images, addresses, load balancers, registry and bucket storage), priced per month, and what it
  could not list. `down` ends with it.
- the control plane: GKE's free tier ($74.40/month per billing account, zonal and Autopilot clusters) covers one zonal cluster's
  $0.10/h fee, so it costs no money while `sandboxes` is the only such cluster on the billing account; the ledger then charges
  it at 0 (`cluster_fee_hour`). A second zonal/Autopilot cluster on the account shares the credit: set the fee back to 0.10, or
  `teardown --delete-cluster --yes`
- promotional credits (startup programs, trials) pay for nodes, disks, builds and storage after the free tier: that spends
  credit, not cash, until they run out. The budget alert counts spend before promotional credits for that reason

## Setup

1. A GCP project with billing, and gcloud authenticated as someone who can create clusters (`roles/container.admin`,
   `roles/iam.serviceAccountAdmin`, `roles/resourcemanager.projectIamAdmin`, `roles/artifactregistry.admin`).
2. `scripts/gke/setup.sh` (names from configs/gke.yaml) prints every command; read it, then add `--apply`. It creates the node
   and build service accounts, the build bucket (1-day lifecycle), the image repository (cleanup policy), the cluster (zonal
   control plane: free tier; Dataplane V2, Workload Identity, image streaming, cost allocation; a small system pool), the
   catalog's sandbox pools at 0 nodes, and the namespace with its quota, limits and network policy.
3. `python scripts/gke/prices.py --project <id> --region us-central1 --write` puts the real on-demand and spot prices into
   `configs/gke.yaml`, and prints the `SANDBOX_GKE_VCPU_HOUR` to export (the pods' rate: spot price / vCPUs).
4. Billing export to BigQuery (console only: Billing > Billing export > Detailed usage cost) for the reconciliation.
5. `export GKE_PROJECT=<id> GKE_LOCATION=<zone> GKE_CLUSTER=sandboxes` and run the watchdog beside any workload:
   `python -m sandboxes.gke_ops watch &` (or the janitor, which runs the same pass).
6. Open sandboxes with a harbor task's `environment/` (built once per context) or a registry image. After the run:
   `python -m sandboxes.gke_ops down` (or let `auto_down_min` do it).

The provider entry is `configs/gke.yaml` (1 vCPU / 4 GiB / 10 GB by default) and starts with a $50 provider spend cap.

### Capacity, per node

- **pods**: `--max-pods-per-node 256` (GKE's default is 110). 192 sandboxes + DaemonSets fit.
- **CPU**: pods request `cpu_request` x cpu (0.95 vCPU for a 1-vCPU sandbox) so 192 fit a 192-vCPU node's allocatable; the limit
  is the full vCPU.
- **disk**: each pod's ephemeral storage is limited to `disk_gb` (10 GiB) but requests only `ephemeral_request_gb` (2 GiB):
  192 x 10 GiB would need 1.9 TB just to schedule. Boot disk 1 TB Hyperdisk Balanced (C4D boots only from Hyperdisk), for the
  scratch space and image layers; image streaming fetches only the layers a container reads.
- **memory**: 192 x 2 GiB = 384 GiB of the node's ~720 GiB.

## Cost model

Google bills nodes (VMs and their disks) and the cluster fee, not pods. The ledger carries both views and they add up:

- every pod has a ledger entry at its share of a node: `usd_per_hour` = `SANDBOX_GKE_VCPU_HOUR` x vCPUs, recorded in the entry
  when it opens. Every later reader (its close, a reconcile or janitor in another process, the meter) charges the entry at that
  recorded rate, so processes with different settings cannot disagree about it.
- the node meter (`sandboxes/gke_meter.py`) charges each node's cost minus what pod entries already cover, split across the
  runs that had pods on it; an empty node's cost goes to run "" (idle capacity). Plus the cluster fee and boot disks.

Pods + meter = the node bill exactly, plus the charges of pods that lived entirely between two meter passes (the meter cannot
know their node; at most one pass interval each). `test_pods_and_meter_add_up_to_the_node_bill_through_random_churn` checks the
identity through the real ledger and meter.

The meter runs on every sandbox create (at most every 30 s), on the guard and janitor cadence, and in `gke_ops watch`. One pass
per machine at a time (file lock); the state file is a cache of the ledger, rebuilt from it if lost. Rules, each tested:

- a node alive now is charged from where the last pass left it (or from its creation): never under-charged, however long the
  meter was down.
- a node gone since the last pass: up to now if passes were close together, else up to its removal event (Kubernetes keeps
  events about an hour), else up to 5 min past its last sighting with a `meter_gap` record saying how much more it may have cost.
- a node that lived and died between two passes (a spot node preempted within minutes) is charged from its registration and
  removal events. A name can come back (a managed instance group recreates a preempted VM under its old name); events fold
  repeats into one (first and last timestamps), so a new life is charged from the last point the name was accounted for:
  at most one pass interval too much, never too little.
- fuzzed over 20,000 random cluster histories (lives from 1 s, name reuse, NotReady flaps, meter outages up to 2.5 h): every
  life is charged in full or flagged, except lives no meter could know of (born and gone inside an outage longer than the
  event retention). Those, and any price drift, are what the billing reconciliation is for.

Prices come from `configs/gke.yaml` `gke_meter:`. An unknown machine type is charged at a deliberately high per-vCPU price; a
node seen only in events at its pool's last price, else the highest price known. The meter warns when the pods' rate and
the node price differ by more than 5% (attribution between runs is then off; totals are not).

### Spend caps

The machine-wide caps (`~/.cache/sandboxes/caps.json`) and the provider's `spend_cap_usd` see GKE spend like any other: node
charges count, and a start that would exceed a cap creates no pod (`test_the_node_bill_counts_against_the_spend_cap...`). The
hard ceiling on the burn rate is the sandbox pool's maximum node count (`SANDBOX_MAX_PER_ZONE` x zones), printed by setup.sh.

## Deletion: what removes a pod

1. `stop()` deletes the pod (grace 0, retried 1/2/4 s; a 404 counts as deleted). A delete that still fails is queued in
   `~/.cache/sandboxes/gke_pending_deletes.jsonl`.
2. `activeDeadlineSeconds` = the sandbox's lifetime (default 240 min): the kubelet ends the pod even if every process of ours
   died.
3. A create that fails at any point after the API call (unschedulable past `capacity_wait_s`, image pull error, timeout,
   cancellation, a lost API response) deletes the pod before raising: the name is fixed before the call.
4. The watchdog (`gke_ops`, also run by the janitor) each pass: retries queued deletes; deletes our pods that finished, outran
   their deadline, are stuck terminating, or stayed Pending past their creator's wait (`sandboxes/ready-within-s` annotation:
   an unscheduled pod never starts its deadline clock, and keeps the autoscaler adding nodes); closes the ledger entries of
   pods that are gone; resizes the sandbox pools to 0 after `idle_scale_down_min` with none of our pods (only with no foreign
   workload on the nodes and a successful listing).
5. The janitor's generic sweep deletes anything of ours older than 6 h; `sandboxes stop --run R` / `gke_ops teardown`.

The queue is safe against concurrent writers (lock) and crashes (a taken queue survives until handled). `fleet.reconcile` no
longer closes an entry opened after the listing it reconciles against (a race that affected every provider).

## Operations

    python -m sandboxes.gke_ops status                    # nodes per pool, $/h, pods per phase and run, queued deletes
    python -m sandboxes.gke_ops sweep [--dry-run]         # one pass
    python -m sandboxes.gke_ops watch --every 60
    python -m sandboxes.gke_ops teardown --run R          # one run's pods
    python -m sandboxes.gke_ops teardown --pools          # all pods, pools to 0, metered until the nodes are gone
    python -m sandboxes.gke_ops teardown --delete-cluster --yes    # and the cluster; the meter charges up to the end of it

`sandboxes reap --creating-older-than` must be at least 20 for GKE: a pod can wait 15 min for a spot node.

## Verifying costs

    python scripts/verify_costs_gke.py --table P.billing.gcp_billing_export_v1_X --project P --cluster sandboxes \
        --since <run start, hour boundary> --until <after teardown, hour boundary>

Compares the ledger's GKE spend in the window with the export's gross cost of resources labelled with the cluster plus the GKE
fee. Fails when the ledger is under 98% of the bill. Run it a day after the window (the export lags) on windows covering whole
runs.

## Security

Sandbox code is untrusted (agent-written). Pods run without a service account token, on nodes whose service account has only
node logging/monitoring and registry-read roles; the GKE metadata server gives pods no node credentials; the network policy
denies all ingress and all egress except the internet and DNS (no metadata server, no VPC or cluster addresses). Pods share
the node's kernel: `GVISOR=1 scripts/gke/setup.sh` (and `runtime_class: gvisor`) adds GKE Sandbox isolation at some CPU and
I/O cost. That trade is the operator's call; it is off by default.

## Field notes: greenhouse evolve on GKE (2026-10-30)

Five greenhouse evolve runs (r-20261030-NS1d1..3, NS2b, NS2c, then SP1) ran on this backend for ~5 h: up to ~45 sandbox
pods of 2 vCPU / 4 GiB / 20 GB (`cpu_request` 1.0, spot preferred) on one n2d-standard-96 spot node, with an
e2-standard-32 spot node also running. What it took and what it showed:

### Sandbox disk is the bottleneck

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

### Credentials

- Builds, the registry and kubectl all use the operator's gcloud login: `gcloud auth login --update-adc` (user
  credentials and ADC). The org enforces `iam.disableServiceAccountKeyCreation`, so a service-account key file is not
  an option. A refresh token from a normal login keeps working; when the account needs re-authentication, everything
  fails at once. `gcloud_watch.sh` in the experiment dir checks `gcloud auth print-access-token` every 5 minutes and
  prints `GCLOUD_LOGIN_NEEDED` once per failure.
- The registry token was cached in-process for 30 minutes and expired mid-run. The resulting 401s failed every sandbox
  open of that process: four greenhouse units were reverted with "precheck failed". Since 9f8e3d0, a registry 401 fetches
  a fresh gcloud token and retries once.

### Builds

- Cloud Build's docker builder used the legacy Dockerfile frontend, which cannot parse `RUN <<EOF` heredocs. Since
  c4b9cf1, builds pass `--build-arg BUILDKIT_SYNTAX=docker/dockerfile:1`.
- A failed build shows up in the caller as a sandbox that cannot open. Read the Cloud Build history (`gcloud builds list`)
  for the pattern before blaming the backend. On 2026-10-30, ten failures in a row were greenhouse rewriting the task's
  Dockerfile (dropping the COPY lines a chmod needed), not GKE.

### Running greenhouse against it

- PYTHONPATH: the greenhouse checkout, this branch's worktree, and a directory holding the kubernetes Python client
  (`pip install --target DIR kubernetes`; greenhouse's venv does not ship it). Also gcloud on PATH, and the provider keys
  file sourced. The experiment's `launch_evolve_gke.sh` asserts all of these before starting.
- Sandbox config used: `base: configs/gke.yaml` with cpu 2, memory_mb 4096, disk_gb 20, and `extra: {cpu_request: 1.0,
  spot: prefer, ready_timeout_s: 1200, ephemeral_request_gb: 5}`.
- Run `python -m sandboxes.gke_ops watch` beside the run. After killing a greenhouse process, `python -m sandboxes stop
  --run R` closes its pods.

## Known limits

- Pod storage is overlay on the node's network boot disk: 7-9x slower than Modal's local disk (see Field notes). Use a
  local-SSD pool for file-heavy workloads.
- A node that lives and dies entirely while no meter runs for more than the event retention (~1 h) is invisible to the meter:
  run the watchdog, and reconcile against billing.
- Pods that live entirely between two meter passes are charged twice (their entry and the node), at most one interval each.
- Prices in `configs/gke.yaml` are static; spot prices move up to once a day. Re-run prices.py, and reconcile.
