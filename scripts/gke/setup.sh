#!/usr/bin/env bash
# Create the GKE cluster that runs sandboxes (docs/gke.md). Prints every command; runs them only with --apply. Re-running is
# safe: a step whose resource already exists is skipped.
#
#   PROJECT=my-project scripts/gke/setup.sh               # dry run: review what would be created and the maximum burn rate
#   PROJECT=my-project scripts/gke/setup.sh --apply
#
# What it creates (all in PROJECT):
#   - a node service account with the minimum node roles + Artifact Registry read (never the Compute default account)
#   - a zonal cluster (control plane covered by the GKE free tier credit) with a small on-demand system pool, Dataplane V2
#     (enforces NetworkPolicy), Workload Identity, image streaming, cost allocation, system-only logging/monitoring
#   - the sandbox pool: spot, autoscaled 0..SANDBOX_MAX_PER_ZONE per zone over NODE_ZONES, 256 pods a node, tainted so only
#     sandboxes land there, GKE metadata server (pods get no node credentials)
#   - namespace NAMESPACE with a ResourceQuota (the hard ceiling on concurrent sandboxes), a LimitRange and a NetworkPolicy
#     (no ingress; egress to the internet and DNS only: no metadata server, no VPC or cluster addresses)
#   - an Artifact Registry docker repo for task images, and optionally a billing budget alert
set -euo pipefail

APPLY=0
[[ "${1:-}" == "--apply" ]] && APPLY=1
: "${PROJECT:?set PROJECT=<gcp project id>}"
ZONE=${ZONE:-us-central1-a}                            # control plane and system pool
NODE_ZONES=${NODE_ZONES:-us-central1-a,us-central1-b,us-central1-c}   # sandbox nodes: wherever spot capacity is
REGION=${ZONE%-*}
CLUSTER=${CLUSTER:-sandboxes}
SYSTEM_MACHINE=${SYSTEM_MACHINE:-e2-standard-4}
SANDBOX_POOL=${SANDBOX_POOL:-sandbox-spot}
SANDBOX_MACHINE=${SANDBOX_MACHINE:-c4d-standard-192}
SANDBOX_MAX_PER_ZONE=${SANDBOX_MAX_PER_ZONE:-2}        # x zones = the most sandbox nodes that can ever run: the burn ceiling
SPOT_PRICE_H=${SPOT_PRICE_H:-3.40}                     # for the printed ceiling only (scripts/gke/prices.py has the real one)
BOOT_DISK_TYPE=${BOOT_DISK_TYPE:-hyperdisk-balanced}   # C4D boots only from Hyperdisk
BOOT_DISK_GB=${BOOT_DISK_GB:-1000}                     # 192 pods x (2 GiB scratch + image layers)
GVISOR=${GVISOR:-0}                                    # 1: GKE Sandbox (gVisor) on the sandbox pool; set runtime_class: gvisor too
NAMESPACE=${NAMESPACE:-sandboxes}
MAX_PODS=${MAX_PODS:-1000}                             # ResourceQuota: pods beyond this are refused by the API server
AR_REPO=${AR_REPO:-envs}
NODE_SA_NAME=${NODE_SA_NAME:-gke-sandbox-nodes}
NODE_SA="${NODE_SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
BILLING_ACCOUNT=${BILLING_ACCOUNT:-}                   # set (XXXXXX-XXXXXX-XXXXXX) for a budget alert
BUDGET_USD=${BUDGET_USD:-200}

run() {
  printf '+ %s\n' "$*"
  if [[ $APPLY == 1 ]]; then "$@"; fi
}
exists() { "$@" >/dev/null 2>&1; }

zones=$(tr ',' '\n' <<<"$NODE_ZONES" | grep -c .)
max_nodes=$((SANDBOX_MAX_PER_ZONE * zones))
echo "project $PROJECT  cluster $CLUSTER ($ZONE)  sandbox pool $SANDBOX_POOL: $SANDBOX_MACHINE spot x 0..$max_nodes over $NODE_ZONES"
awk -v n="$max_nodes" -v p="$SPOT_PRICE_H" 'BEGIN { printf "burn ceiling: %d nodes x $%.2f/h = $%.2f/h for sandbox nodes (+ system node + $0.10/h fee)\n", n, p, n * p }'
[[ $APPLY == 1 ]] || echo "(dry run: nothing is created; add --apply)"
echo

run gcloud services enable container.googleapis.com compute.googleapis.com artifactregistry.googleapis.com \
  cloudbilling.googleapis.com billingbudgets.googleapis.com --project "$PROJECT"

if ! exists gcloud iam service-accounts describe "$NODE_SA" --project "$PROJECT"; then
  run gcloud iam service-accounts create "$NODE_SA_NAME" --project "$PROJECT" --display-name "GKE sandbox nodes"
fi
for role in roles/container.defaultNodeServiceAccount roles/artifactregistry.reader; do
  run gcloud projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:$NODE_SA" --role "$role" --condition None --quiet
done

if ! exists gcloud container clusters describe "$CLUSTER" --zone "$ZONE" --project "$PROJECT"; then
  run gcloud container clusters create "$CLUSTER" --project "$PROJECT" --zone "$ZONE" --node-locations "$ZONE" \
    --release-channel regular --machine-type "$SYSTEM_MACHINE" --disk-type pd-balanced --disk-size 50 \
    --num-nodes 1 --enable-autoscaling --min-nodes 1 --max-nodes 3 --autoscaling-profile optimize-utilization \
    --enable-ip-alias --enable-dataplane-v2 --workload-pool "$PROJECT.svc.id.goog" --service-account "$NODE_SA" \
    --enable-image-streaming --enable-cost-allocation --enable-shielded-nodes \
    --logging SYSTEM --monitoring SYSTEM --no-enable-managed-prometheus
fi

if ! exists gcloud container node-pools describe "$SANDBOX_POOL" --cluster "$CLUSTER" --zone "$ZONE" --project "$PROJECT"; then
  sandbox_flag=()
  if [[ $GVISOR == 1 ]]; then sandbox_flag=(--sandbox type=gvisor); fi
  run gcloud container node-pools create "$SANDBOX_POOL" --project "$PROJECT" --cluster "$CLUSTER" --zone "$ZONE" \
    --node-locations "$NODE_ZONES" --machine-type "$SANDBOX_MACHINE" --spot \
    --num-nodes 0 --enable-autoscaling --min-nodes 0 --max-nodes "$SANDBOX_MAX_PER_ZONE" --location-policy ANY \
    --max-pods-per-node 256 --disk-type "$BOOT_DISK_TYPE" --disk-size "$BOOT_DISK_GB" \
    --node-labels sandboxes.pool=sandbox --node-taints sandboxes.pool=sandbox:NoSchedule \
    --workload-metadata GKE_METADATA --service-account "$NODE_SA" --enable-image-streaming ${sandbox_flag[@]+"${sandbox_flag[@]}"}
fi

run gcloud container clusters get-credentials "$CLUSTER" --zone "$ZONE" --project "$PROJECT"

manifest=$(mktemp)
trap 'rm -f "$manifest"' EXIT
cat >"$manifest" <<EOF
apiVersion: v1
kind: Namespace
metadata: {name: $NAMESPACE}
---
apiVersion: v1
kind: ResourceQuota
metadata: {name: sandboxes-ceiling, namespace: $NAMESPACE}
spec:
  hard: {pods: "$MAX_PODS", requests.cpu: "$MAX_PODS"}
---
apiVersion: v1
kind: LimitRange
metadata: {name: sandboxes-defaults, namespace: $NAMESPACE}
spec:
  limits:
  - type: Container
    default: {cpu: "1", memory: 2Gi, ephemeral-storage: 10Gi}
    defaultRequest: {cpu: 950m, memory: 2Gi, ephemeral-storage: 2Gi}
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: {name: sandboxes-isolate, namespace: $NAMESPACE}
spec:
  podSelector: {matchLabels: {sandboxes.managed: "true"}}
  policyTypes: [Ingress, Egress]
  ingress: []
  egress:
  - to:
    - ipBlock: {cidr: 0.0.0.0/0, except: [169.254.0.0/16, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10]}
  - to:
    - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: kube-system}}
      podSelector: {matchLabels: {k8s-app: kube-dns}}
    ports: [{protocol: UDP, port: 53}, {protocol: TCP, port: 53}]
EOF
echo "+ kubectl apply -f - <<namespace, quota, limits, network policy>>"
if [[ $APPLY == 1 ]]; then
  kubectl apply -f "$manifest"
else
  sed 's/^/    /' "$manifest"
fi

if ! exists gcloud artifacts repositories describe "$AR_REPO" --location "$REGION" --project "$PROJECT"; then
  run gcloud artifacts repositories create "$AR_REPO" --repository-format docker --location "$REGION" --project "$PROJECT" \
    --description "sandbox task images"
fi

if [[ -n $BILLING_ACCOUNT ]]; then
  run gcloud billing budgets create --billing-account "$BILLING_ACCOUNT" --display-name "sandboxes GKE ($PROJECT)" \
    --budget-amount "${BUDGET_USD}USD" --filter-projects "projects/$PROJECT" \
    --threshold-rule percent=0.5 --threshold-rule percent=0.9 --threshold-rule percent=1.0
else
  echo "(no BILLING_ACCOUNT: no budget alert. A budget only emails; the spend caps in ~/.cache/sandboxes/caps.json stop spend.)"
fi

cat <<EOF

Next:
  export GKE_PROJECT=$PROJECT GKE_LOCATION=$ZONE GKE_CLUSTER=$CLUSTER GKE_NAMESPACE=$NAMESPACE
  images: $REGION-docker.pkg.dev/$PROJECT/$AR_REPO/<task>:<tag>   (gcloud auth configure-docker $REGION-docker.pkg.dev)
  python scripts/gke/prices.py --project $PROJECT --region $REGION --machine $SANDBOX_MACHINE    # real prices into configs/gke.yaml
  python -m sandboxes.gke_ops watch &                                                         # the watchdog, beside any workload
  Billing export to BigQuery (console: Billing > Billing export) for scripts/verify_costs_gke.py
EOF
