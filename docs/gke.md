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
- the control plane: GKE's free tier credits one zonal cluster's fee per billing account; `teardown --delete-cluster --yes`
  removes it if that credit is used elsewhere

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

## Known limits

- A node that lives and dies entirely while no meter runs for more than the event retention (~1 h) is invisible to the meter:
  run the watchdog, and reconcile against billing.
- Pods that live entirely between two meter passes are charged twice (their entry and the node), at most one interval each.
- Prices in `configs/gke.yaml` are static; spot prices move up to once a day. Re-run prices.py, and reconcile.
