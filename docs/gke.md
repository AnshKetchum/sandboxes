# GKE sandboxes

One Kubernetes pod per sandbox on our own GKE cluster. A sandbox asks for a size (vCPUs, memory, disk); it lands on the node
pool that runs that size most cheaply; a harbor task's Dockerfile is built once per context and every sandbox of that task runs
the image; pools sit at zero nodes (free) until sandboxes need them; after a run nothing is left that bills.

1,000 sandboxes of 1 vCPU / 4 GiB / 10 GB (a whole pinned vCPU and 4 GiB guaranteed each, disk reserved), us-central1 spot
prices of 2026-10-30, pods' storage on local NVMe (`python -m sandboxes.gke_place plan --cpu 1 --mem 4 --disk 10 --count 1000`
prices the boot-disk variants; `gke_place pools` the catalog):

| pools | sandboxes | $/h | needs |
|---|---|---|---|
| 6 x c3d-standard-180-lssd | 1,026 | ~14.5 | C3D quota ~1,100 (granted: 150) |
| today's catalog without N2D/E2 (`bench.py ramp`, measured) | 1,000 | 31.87 | C3D, C2D, N4D, N4, N2, C4N quotas |
| today's catalog with every spot pool (`configs/gke.yaml`, nothing else running) | 1,159 | 34.42 | the quotas below; CPUS_ALL_REGIONS binds |
| 2 x n2d-standard-96 (before 2026-10-30) | ~180 | ~4.3 | N2D 200 |

Plus a small system node (~$0.07/h; the control plane fee is covered by GKE's free tier) and image builds once per distinct
task (~$0.06 each). Modal measured $0.238 per sandbox-hour at 1 vCPU / 4 GiB: $238/h for the same fleet.

**Quota decides what runs.** The ~180 ceiling was the N2D vCPU quota (200: two n2d-standard-96 nodes), not the backend. The
regional `CPUS` quota (200) counts only N1/E2; every other family has its own (us-central1, 2026-10-30: C3D 150, N2D 200,
N2 200, C2D 100, N4D 200, N4 200, E2 72, C3/C4/C4D/T2D 24) and all of them sit under `CPUS_ALL_REGIONS` 1,300. So 1,000
guaranteed vCPUs fit today only spread over six to eight families (measured: 1,000 of 1,000 at once, below) (`gke.quotas` + the catalog's pools: placement fills the
cheapest first and skips a family whose next node would pass its quota). The cheaper single-family fleet needs the C3D quota
raised (it halves the fleet's cost, and the C3D pools were ready first in every ramp): a request for 1,100 on 2026-10-27
was granted 150; ask again (`gcloud beta quotas preferences create
--service=compute.googleapis.com --quota-id=CPUS-PER-VM-FAMILY-per-project-region --dimensions=region=us-central1,vm_family=C3D
--preferred-value=1100 --project=agentic-rollouts --preference-id=c3d-cpus-us-central1-2`), then enable `c3d-180-lssd-spot`. Do
not request `PREEMPTIBLE_CPUS`: once a project has it, spot VMs of every family draw from it alone, and a partial grant would
cut capacity. The namespace's ResourceQuota (setup.sh: 1,500 pods / 2,000 requested vCPUs; it was 1,000 / 1,000) has to cover
the sandboxes plus whatever else runs: `kubectl patch resourcequota sandboxes-ceiling -n sandboxes --type merge -p
'{"spec":{"hard":{"pods":"1500","requests.cpu":"2000"}}}'` on a cluster set up before.

## What to expect

What a run on this backend sees today (us-central1, measured 2026-10-30 with `scripts/gke/bench.py`, details below).

**How many.** Up to ~1,150 sandboxes of 1 vCPU / 4 GiB / 10 GB at once, if nothing else runs on the project: the catalog's spot
pools across seven machine families, capped by the project's 1,300 vCPUs. 1,000 at once was measured with other runs beside it.
The namespace quota (1,000 pods / 1,000 requested vCPUs unless raised) and the spend caps can stop a run first: the
machine-wide caps (`~/.cache/sandboxes/caps.json`), and the provider entry's `spend_cap_usd` (configs/gke.yaml: $50), which
the router checks against the run's own ledger (its session) and then stops choosing GKE: $50 is ~1.5 hours of a
1,000-sandbox fleet, so raise it in the run's sandbox config for a large run.
Past the ceiling, a start raises `CapacityError` (immediately when every pool is at its node or quota limit, after up to 300 s
when a pool cannot get a node), and the router can send it to another provider. A sandbox asking for more than a node holds
is refused at once.

**How fast it starts.**
- Onto a node already up with the image cached: ~0.5 s (p50 0.54-0.64 s, max ~3 s).
- Onto a new node: 1-2 minutes (E2 ~70 s, C3D/C2D/N4D with local SSD or Hyperdisk 90-120 s), plus the image's first pull on that
  node (image streaming fetches only what the container reads). A task image is built once per Dockerfile context by Cloud
  Build before its first start (~40 s for a small image).
- A burst of 1,000: ~3.5 minutes until every one runs (p50 113 s).
- When a spot machine type is out in a zone (seen: n2-standard-96 in all four zones, n4d-standard-96 in one), the sandboxes
  placed on it wait: the autoscaler tries the other zones, and after 300 s the start moves to another pool (an on-demand
  fallback pool for N2 and C4N). Expect up to 5-15 minutes for the last part of a large burst on such a day. The N2 pools
  are the least reliable: later the same day n2-standard-96 with 16 local SSDs was out on demand too, in all four zones.
- After `down` (or 60 idle minutes: `auto_down_min`) the first start brings the cluster back up: ~2 more minutes.

**What a sandbox gets.**
- CPU: one vCPU of its own (pinned; `nproc` = 1). A vCPU is a hyperthread: with its neighbour busy it runs ~45% slower (python
  loop 0.71 s alone, 1.26 s on a full node). Single-thread speed is 1.1-2x below Modal's (field notes, same task image: 0.56 s
  vs 0.29-0.51 s); C3D measured ~25% faster than N2D (0.71 vs 0.90 s), the other families not yet.
- Memory: 4 GiB guaranteed, the limit too: past it the process is OOM-killed. `free` and `df` inside show the node's totals.
- Disk: 10 GiB (past it the pod is evicted), on local NVMe on most pools: small-file work at about Modal's speed when the node is
  quiet (3,000 files 0.24 s vs 0.21 s), 3-4x slower than that with every sandbox of the node writing at once, still 4x faster
  than the network boot disk was. The N4/N4D/C4N, E2 and `n2d-96-spot` pools keep pods' storage on the boot disk (provisioned
  Hyperdisk or pd-balanced): slower under load.
- Network: the internet and DNS only (no cluster, VPC or metadata addresses); no inbound connections.

**Commands and files.** Every exec is a new stream through the API server: ~0.2 s per call from New Jersey (us-central1),
whatever the command. 1,000 sandboxes running a command at once: ~5 s for all. A 16 MiB upload takes ~0.85 s, a download
2-4 s; 26 sandboxes transferring at once ~100 MB/s together through this machine. A command longer than 2 hours runs
detached and is polled. Batch many small commands into one exec where latency matters.

**What it costs.** Google bills nodes, not sandboxes: a full node costs the same with 1 sandbox or 90. Per sandbox-hour at full
nodes (spot): C3D with local SSD $0.014, N2D $0.024, C2D/E2 $0.025-0.026, N4D $0.032, N4 $0.035, N2 $0.036, C4N $0.047;
on-demand fallbacks ~2-4x that. Placement fills the cheapest pools first, so a fleet's average rises with its size: 100
sandboxes ~$2/h, 800 ~$23.6/h, 1,000 ~$31.9/h. Idle nodes keep billing until they go: the autoscaler removes an empty node,
typically within ~10 minutes of the last scale-up; the watchdog resizes idle pools after 15 minutes; `gke_ops down` at once.
The ledger charges each sandbox its share of its node and the meter charges the rest (idle room) to run "". Image builds
~$0.06 each.

**What can go wrong.**
- Spot nodes can be reclaimed by Google at ~30 s notice: every sandbox on that node is gone, and its commands return rc -1.
  Callers that cannot redo a sandbox's work should use the on-demand pools (`spot: never`).
- A failed image build surfaces as a sandbox that cannot open (`ImageBuildError` with the build log's tail): check
  `gcloud builds list` before suspecting the backend.
- Builds, the registry and kubectl use the operator's gcloud login: when it needs re-authentication everything fails at once
  (`gcloud auth login --update-adc`).
- A process killed mid-run leaves its pods until the watchdog or their lifetime (`auto_delete_minutes`, default 240) ends them:
  run `python -m sandboxes.gke_ops watch` beside workloads, or `sandboxes stop --run R` after a crash.

**Several jobs at once.** Runs share the cluster and its pools; each sandbox carries its caller's labels, so any one job can be
stopped without touching the others: a run (`sandboxes stop --run R`), or one unit of a run (`sandboxes stop --label
greenhouse.run=R --label greenhouse.task=T`). `sandboxes hold` with the same labels first keeps the caller from opening
replacements (its next open raises `SandboxHeld`); `sandboxes release` undoes it. A job that needs its own pod quota or must not
share nodes can run in its own namespace (`GKE_NAMESPACE`; the 1,000 benchmark did) or with `extra.pool` / `extra.exclude_pools`.

**After a run.** `python -m sandboxes.gke_ops down` leaves nothing billing but the free control plane (pods, nodes, running
builds, images, uploads; it ends with the residue audit). `python -m sandboxes.gke_ops destroy --yes` also removes the
cluster, the image repository and the build bucket.

## Performance and scale (2026-10-30, `scripts/gke/bench.py`)

The greenhouse evolve runs on GKE found sandboxes ~2x slower than Modal on file-heavy work
([gke_field_notes.md](gke_field_notes.md)). Measured since, with python:3.12-slim sandboxes of 1 vCPU / 4 GiB / 10 GB, from this machine (New Jersey, ~32 ms to
us-central1):

**Disk.** Pods wrote to overlay on the node's network boot disk, shared by every pod of the node. Same machine (c3d-standard-30),
pods' storage on 2 local NVMe SSDs vs on a 400 GB pd-balanced boot disk:

| | local SSD, alone | pd-balanced, alone | local SSD, 26 at once | pd-balanced, 26 at once | Modal (field notes) |
|---|---|---|---|---|---|
| 256 MB write + fsync | 0.46 s | 1.13 s | 7.95 s | 28.95 s | 0.09 s |
| 3,000 small files written, synced, read | 0.24 s | 0.48 s | 0.73 s (p90 1.20) | 2.85 s (p90 3.95) | 0.21 s |
| fsync of 4 KiB, p50 / p99 | 0.27 / 1.2 ms | 1.2 / 9.3 ms | 0.71 / 4.2 ms | 2.6 / 27 ms | |
| venv with pip (CPU-bound) | 1.87 s | 1.98 s | 3.21 s | 3.31 s | |

The live n2d-96 node (pd-balanced 1,250 GB, ~50 greenhouse pods) gave 2.38 s for the small files alone. So every family that
has local SSD gets `local_ssd` in the catalog; N4/N4D/C4N (Hyperdisk only) get provisioned IOPS and throughput.

**Starts.** A start on a node already up with the image cached took 7.1-7.6 s; now 0.54-0.64 s p50 (Modal: 4.4 s). It was
spent outside Kubernetes: the spend check re-read the machine's ~60 MB of ledgers (3.4 s, every 30 s, on the event loop: now
read incrementally), a node meter pass was awaited (2.1 s: now in the background), and the pod was polled every 2 s (now one
pod watch per process wakes it). A cold start (node scale-up) is 64-120 s when spot capacity is there.

**Exec and files.** Commands run through the API server's exec websocket: ~0.2 s a round trip from here, of which most is
the TLS and websocket setup a new stream needs (a region nearer the caller, e.g. us-east4 for this machine, would cut it).
The client's v5 protocol goes through the API server's stream translator, which under load returned short output with exit
code 0 (3 of 40 concurrent 16 MiB reads) and the client's channel buffering is quadratic; exec now asks for v4 and drains the
channels as it reads: 40 concurrent 16 MiB reads 27 s -> 6.5 s each, none short.

**Scale.** One process, sandboxes opened at once through placement, `bench.py ramp`:

| run | sandboxes | pools (nodes) | all ready | exec on all at once | warm open p50 | fleet $/h (spot) |
|---|---|---|---|---|---|---|
| gke-bench-ramp100 | 100 / 100 | c3d-60-lssd, c3d-90-lssd | 106 s (99), 318 s (1: model over-packed, fixed) | 1.1 s | 0.54 s | 1.97 |
| gke-bench-ramp800 | 800 / 800 | 8 pools, 13 nodes | 64-119 s on 6 pools; N4 ~400 s, N2 ~840 s (spot stockouts) | 4.4 s | 0.64 s | 23.63 |
| gke-bench-ramp1000 | 576 / 1,000 (stopped) | 8 pools | placer bug: each snapshot refresh forgot pods waiting for a node, so 372 went to a 2-node N2 pool (spot N2 out in all zones) and none to C4N; fixed | | | |
| gke-bench-ramp1000b | **1,000 / 1,000** | 8 pools, 15 nodes (C3D, C2D, N4D, N4, N2, C4N; all spot) | **196 s** (open p50 113 s, p90 180 s) | 5.0 s (p50 2.2 s) | 0.56 s | **31.87** |

The 1,000 run used its own namespace (`GKE_NAMESPACE=sandboxes-scale`, quota 1,100 pods) beside live greenhouse runs, kept
off their N2D and E2 pools and under 1,106 of the project's 1,300 vCPUs (`quotas.ALL`), so their own scale-ups had room.
$31.87/h is the sum of the 1,000 pods' recorded shares of their nodes (~$0.032 per sandbox-hour); C4N, the last family in,
costs $0.047 a sandbox-hour against C3D's $0.014.

Spot stockouts (`RESOURCE_POOL_EXHAUSTED`: n2-standard-96 in three zones, n4d-standard-96 in one) are what slowed the large
ramps: the autoscaler retries other zones, and placement moves a pod after `capacity_wait_s` (300 s). Pools of smaller machines
of the same family, or an on-demand pool per family as the fallback, shorten that; more C3D quota avoids it (the C3D pools
were ready first in every run).

## Live verification (2026-10-27, agentic-rollouts, us-central1)

`scripts/gke/live_check.py` against the real cluster: each sandbox built from a harbor task's Dockerfile, every exec / file /
network check, the task's verifier on its reference solution, then the bill compared with observed node lives, then `down` and
an independent gcloud listing of everything billable.

| run | sandboxes | checks | reward | cold start | warm start | ledger vs observed | left after down |
|---|---|---|---|---|---|---|---|
| gke-live-1d | 1 (+1 warm) | all pass | 1 | 255 s | 6.0 s | 1.08x (over) | control plane only |
| gke-live-8 | 8 concurrent (+1 warm) | all pass | 1 | 268 s | 6.2 s | 1.13x (over) | control plane only |
| gke-live-merged2 (2026-10-30, merged gke-backend) | 1 (+1 warm), c3d-60-lssd | all pass | 1 | 85 s (node up) | 1.2 s | not comparable: live runs shared the window | (no `down`: live runs) |

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

- build minutes: charged when each build ends (`image_build`); `down` cancels builds still queued or running before it purges
  the registry (one finishing afterwards would push an image that bills until the cleanup policy)
- build source uploads: deleted when the build ends; the bucket's lifecycle rule deletes leftovers after a day
- images: every push recorded; deleted by `down`, by the watchdog once no pod runs it and it is unused for `image_ttl_h`, and
  by the repository's cleanup policy after `cleanup_days` whatever happens to this machine. Storage is charged for each image's
  whole life when it is deleted (or found deleted by the policy)
- nodes (with their boot disks, local SSDs and external IPs): sandbox pools go to 0 after `idle_scale_down_min`; `down` (also
  automatic after `auto_down_min` idle) takes every pool to 0, the system pool too; a sandbox start on a cluster that is down
  brings it back up (`auto_up`, ~2 min). "Every pool" is every pool whose nodes carry the `sandboxes.pool` label, not only the
  current catalog's: a pool made from another worktree's catalog (a benchmark's) or disabled since bills all the same
- `python -m sandboxes.gke_ops residue` lists everything in the project that bills while idle (clusters, VMs with their local
  SSDs and external IPs, disks and orphaned disks, snapshots, VM images, addresses, load balancers, registry and bucket
  storage, builds still running), priced per month, and what it could not list. `down` ends with it.
- `python -m sandboxes.gke_ops destroy --yes` undoes everything `scripts/gke/setup.sh` made that can bill: `down`, then the
  cluster, the image repository and the build bucket with whatever they hold, then `residue`, which must list nothing (exit 1
  otherwise). Service accounts, IAM bindings, enabled APIs and the budget alert stay: none of them bills, and setup.sh reuses
  them. Not logging or monitoring either: the cluster sends system components only, Managed Prometheus off
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

- **pods**: `--max-pods-per-node 256` (GKE's default is 110).
- **CPU**: `cpu_request` 1.0 (configs/gke.yaml): whole-CPU Guaranteed pods, pinned to their own vCPU by the static CPU manager
  (`nproc` = 1, cgroup quota unthrottled). A vCPU is a hyperthread: with every sandbox of a node busy, a sandbox gets ~0.55 of a
  core (python loop 0.71 s alone, 1.26 s with 26 neighbours busy). The kubelet keeps 0.4 vCPU more than GKE's table and the
  DaemonSets request 1.25 (fluentbit's 256-pod variant: 850m): `node_overhead.cpu` 1.7.
- **memory**: 4 GiB guaranteed (request = limit). The kernel keeps ~1.7% of a VM's memory before GKE reserves its share
  (`MEM_VISIBLE` 0.983), so a standard-shape node (4 GiB per vCPU) holds ~0.93 sandboxes per vCPU: c3d-60 55, c3d-90 84,
  any 96-vCPU node 90, c3d-180 171 (checked against live nodes).
- **disk**: each pod's ephemeral storage is limited to `disk_gb` (10 GiB) and requests `ephemeral_request_gb` (default: all of
  it). On a `local_ssd` pool it lives on the node's NVMe SSDs (with the image cache): allocatable 90% of the SSDs less 50/75/100
  GiB for 1/2/3+ SSDs. Otherwise on the boot disk (pd-balanced, or Hyperdisk Balanced with `boot_iops`/`boot_mbps` for N4,
  N4D, C4, C4D, C4N): size it for the pods' requests plus `image_cache_gib`.

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
    python -m sandboxes.gke_ops destroy --yes             # everything setup.sh made that can bill, then the residue audit
    sandboxes ls|stop --label greenhouse.run=R --label greenhouse.task=T   # one unit of a run (any provider)
    sandboxes hold|release --label ...; sandboxes holds                    # refuse / allow that unit's new sandboxes
    python -m sandboxes.gke_ops pools plan|apply [--only NAME...] [--replace] [--prune]

    python scripts/gke/bench.py probe --pool A B [--count N --load]     # what a sandbox gets on each pool (disk, CPU, exec)
    python scripts/gke/bench.py ramp --count 1000 --hold 60              # N at once through placement; GKE_NAMESPACE to isolate

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

## Known limits

- A node that lives and dies entirely while no meter runs for more than the event retention (~1 h) is invisible to the meter:
  run the watchdog, and reconcile against billing.
- Pods that live entirely between two meter passes are charged twice (their entry and the node), at most one interval each.
- Prices in `configs/gke.yaml` are static; spot prices move up to once a day. Re-run prices.py, and reconcile.
- Quotas in `gke.quotas` are static too: re-read them (`gcloud beta quotas info list --service=compute.googleapis.com`) after a
  grant. Spot capacity is not a quota: a family can stock out in a zone for minutes (seen for n2-standard-96, n4d-standard-96).
- A sandbox's vCPU is a hyperthread; a busy sibling costs it ~45% (whole cores mean `cpu: 2` per sandbox, or SMT off, at 2x).
- Exec is ~0.2 s a call from New Jersey to us-central1 (stream setup through the API server, per call).
