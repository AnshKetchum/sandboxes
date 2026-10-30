#!/usr/bin/env bash
# Create the GKE cluster that runs sandboxes (docs/gke.md), from configs/gke.yaml. Prints every command; runs them only with
# --apply. Re-running is safe: a step whose resource exists is skipped.
#
#   scripts/gke/setup.sh               # dry run: review what would be created
#   scripts/gke/setup.sh --apply
#
# What it creates (names from configs/gke.yaml `gke:`):
#   - node service account: the minimum node roles + Artifact Registry read (never the Compute default account)
#   - build service account: Artifact Registry write, logs, read of the build bucket
#   - build bucket (Cloud Build source uploads) with a lifecycle rule deleting anything older than a day
#   - Artifact Registry repo for task images with a cleanup policy deleting images older than gke.build.cleanup_days
#   - a zonal cluster (control plane fee covered by the GKE free tier credit) with a small on-demand system pool, Dataplane V2
#     (enforces NetworkPolicy), Workload Identity, image streaming, cost allocation, system-only logging/monitoring
#   - the sandbox node pools of the catalog (gke_ops pools apply): spot or on demand, 0 nodes until a sandbox needs one,
#     256 pods a node, tainted so only sandboxes land there, GKE metadata server (pods get no node credentials)
#   - namespace with a ResourceQuota (the ceiling on concurrent sandboxes), a LimitRange and a NetworkPolicy (no ingress;
#     egress to the internet and DNS only: no metadata server, no VPC or cluster addresses). DNS must be allowed to
#     node-local-dns as well as kube-dns: GKE enables NodeLocal DNSCache, and under Dataplane V2 a pod's DNS goes to the
#     node-local-dns pod on its node (found live: with kube-dns alone every lookup in a sandbox failed)
#   - optionally a billing budget alert (BILLING_ACCOUNT=...): it subtracts the free tier and automatic discounts but NOT
#     promotional credits, so spend that drains credits alerts too (with all credits included it would read ~$0 and stay quiet)
set -euo pipefail

APPLY=0
[[ "${1:-}" == "--apply" ]] && APPLY=1
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PY=${PYTHON:-python3}
CONFIG=${SANDBOX_GKE_CONFIG:-$ROOT/configs/gke.yaml}

cfg() {  # cfg <dotted.key> [default]
  "$PY" - "$CONFIG" "$1" "${2:-}" <<'EOF'
import sys, yaml
d = yaml.safe_load(open(sys.argv[1])) or {}
for k in sys.argv[2].split("."):
    d = d.get(k) if isinstance(d, dict) else None
print(",".join(d) if isinstance(d, list) else (sys.argv[3] if d in (None, "") else d))
EOF
}

PROJECT=${PROJECT:-$(cfg gke.project)}
ZONE=${ZONE:-$(cfg gke.location)}
REGION=${ZONE%-*}
CLUSTER=${CLUSTER:-$(cfg gke.cluster)}
SYSTEM_POOL=$(cfg gke.system_pool default-pool)
SYSTEM_MACHINE=${SYSTEM_MACHINE:-e2-standard-2}
NODE_SA=$(cfg gke.node_service_account)
BUILD_SA=$(cfg gke.build.service_account)
BUCKET=$(cfg gke.build.bucket)
REGISTRY=$(cfg gke.registry)
AR_REPO=${REGISTRY##*/}
CLEANUP_DAYS=$(cfg gke.build.cleanup_days 2)
NAMESPACE=${GKE_NAMESPACE:-sandboxes}
MAX_PODS=${MAX_PODS:-1500}                             # ResourceQuota: pods beyond this are refused by the API server
MAX_CPU=${MAX_CPU:-2000}                               # ResourceQuota: requested vCPUs (1,000 one-vCPU sandboxes beside other runs'
                                                       # 2-vCPU ones); the node pools' max_nodes is the burn ceiling
BILLING_ACCOUNT=${BILLING_ACCOUNT:-}
BUDGET_USD=${BUDGET_USD:-200}
: "${PROJECT:?gke.project}" "${ZONE:?gke.location}" "${CLUSTER:?gke.cluster}" "${NODE_SA:?gke.node_service_account}"
: "${BUILD_SA:?gke.build.service_account}" "${BUCKET:?gke.build.bucket}" "${REGISTRY:?gke.registry}"

run() {
  printf '+ %s\n' "$*"
  if [[ $APPLY == 1 ]]; then "$@"; fi
}
exists() { "$@" >/dev/null 2>&1; }
retry() {  # retry <cmd...>: IAM takes a while to see a service account it just created
  local i
  for i in 1 2 3 4 5 6 7 8; do
    if [[ $APPLY != 1 ]]; then printf '+ %s\n' "$*"; return 0; fi
    printf '+ %s\n' "$*"
    if "$@"; then return 0; fi
    echo "  (attempt $i failed; retrying in $((i * 5)) s)" >&2
    sleep $((i * 5))
  done
  return 1
}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "project $PROJECT  cluster $CLUSTER ($ZONE)  registry $REGISTRY  build bucket gs://$BUCKET"
SANDBOX_GKE_CONFIG="$CONFIG" "$PY" -m sandboxes.gke_place pools --cpu 1 --mem 4 --disk 10 2>/dev/null | sed 's/^/  pool /' || true
[[ $APPLY == 1 ]] || echo "(dry run: nothing is created; add --apply)"
echo

run gcloud services enable container.googleapis.com compute.googleapis.com artifactregistry.googleapis.com \
  cloudbuild.googleapis.com storage.googleapis.com cloudbilling.googleapis.com billingbudgets.googleapis.com --project "$PROJECT"

for sa in "$NODE_SA" "$BUILD_SA"; do
  if ! exists gcloud iam service-accounts describe "$sa" --project "$PROJECT"; then
    run gcloud iam service-accounts create "${sa%%@*}" --project "$PROJECT" --display-name "sandboxes: ${sa%%@*}"
  fi
done
for role in roles/container.defaultNodeServiceAccount roles/artifactregistry.reader; do
  retry gcloud projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:$NODE_SA" --role "$role" --condition None --quiet >/dev/null
done
for role in roles/artifactregistry.writer roles/logging.logWriter; do
  retry gcloud projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:$BUILD_SA" --role "$role" --condition None --quiet >/dev/null
done

echo '{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 1}}]}' >"$tmp/lifecycle.json"
if ! exists gcloud storage buckets describe "gs://$BUCKET" --project "$PROJECT"; then
  run gcloud storage buckets create "gs://$BUCKET" --project "$PROJECT" --location "$REGION" --uniform-bucket-level-access \
    --lifecycle-file "$tmp/lifecycle.json"
else
  run gcloud storage buckets update "gs://$BUCKET" --lifecycle-file "$tmp/lifecycle.json"
fi
retry gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --member "serviceAccount:$BUILD_SA" --role roles/storage.objectViewer >/dev/null

if ! exists gcloud artifacts repositories describe "$AR_REPO" --location "$REGION" --project "$PROJECT"; then
  run gcloud artifacts repositories create "$AR_REPO" --repository-format docker --location "$REGION" --project "$PROJECT" \
    --description "sandbox task images (deleted when unused; cleanup policy as backstop)"
fi
cat >"$tmp/cleanup.json" <<EOF
[{"name": "delete-after-${CLEANUP_DAYS}d", "action": {"type": "Delete"}, "condition": {"tagState": "any", "olderThan": "${CLEANUP_DAYS}d"}}]
EOF
run gcloud artifacts repositories set-cleanup-policies "$AR_REPO" --location "$REGION" --project "$PROJECT" --policy "$tmp/cleanup.json"

if ! exists gcloud container clusters describe "$CLUSTER" --zone "$ZONE" --project "$PROJECT"; then
  run gcloud container clusters create "$CLUSTER" --project "$PROJECT" --zone "$ZONE" --node-locations "$ZONE" \
    --release-channel regular --machine-type "$SYSTEM_MACHINE" --disk-type pd-balanced --disk-size 50 \
    --num-nodes 1 --enable-autoscaling --min-nodes 1 --max-nodes 3 --autoscaling-profile optimize-utilization \
    --enable-ip-alias --enable-dataplane-v2 --workload-pool "$PROJECT.svc.id.goog" --service-account "$NODE_SA" \
    --enable-image-streaming --enable-cost-allocation --enable-shielded-nodes \
    --logging SYSTEM --monitoring SYSTEM --no-enable-managed-prometheus
fi
run gcloud container clusters get-credentials "$CLUSTER" --zone "$ZONE" --project "$PROJECT"

echo "+ python -m sandboxes.gke_ops pools $([[ $APPLY == 1 ]] && echo apply || echo plan)"
if [[ $APPLY == 1 ]]; then
  SANDBOX_GKE_CONFIG="$CONFIG" "$PY" -m sandboxes.gke_ops pools apply
else
  echo "    (the catalog's pools, created at 0 nodes: see 'pool' lines above)"
fi

cat >"$tmp/namespace.yaml" <<EOF
apiVersion: v1
kind: Namespace
metadata: {name: $NAMESPACE}
---
apiVersion: v1
kind: ResourceQuota
metadata: {name: sandboxes-ceiling, namespace: $NAMESPACE}
spec:
  hard: {pods: "$MAX_PODS", requests.cpu: "$MAX_CPU"}
---
apiVersion: v1
kind: LimitRange
metadata: {name: sandboxes-defaults, namespace: $NAMESPACE}
spec:
  limits:
  - type: Container
    default: {cpu: "1", memory: 4Gi, ephemeral-storage: 10Gi}
    defaultRequest: {cpu: 950m, memory: 4Gi, ephemeral-storage: 10Gi}
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
      podSelector: {matchExpressions: [{key: k8s-app, operator: In, values: [kube-dns, node-local-dns]}]}
    ports: [{protocol: UDP, port: 53}, {protocol: TCP, port: 53}]
EOF
echo "+ kubectl apply -f <namespace, quota, limits, network policy>"
if [[ $APPLY == 1 ]]; then
  kubectl apply -f "$tmp/namespace.yaml"
else
  sed 's/^/    /' "$tmp/namespace.yaml"
fi

if [[ -n $BILLING_ACCOUNT ]]; then
  run gcloud billing budgets create --billing-account "$BILLING_ACCOUNT" --display-name "sandboxes GKE ($PROJECT)" \
    --budget-amount "${BUDGET_USD}USD" --filter-projects "projects/$PROJECT" \
    --credit-types-treatment include-specified-credits \
    --filter-credit-types FREE_TIER,SUSTAINED_USAGE_DISCOUNT,COMMITTED_USAGE_DISCOUNT,COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE,DISCOUNT \
    --threshold-rule percent=0.5 --threshold-rule percent=0.9 --threshold-rule percent=1.0
fi

cat <<EOF

Next:
  python -m sandboxes.gke_ops watch &        # the watchdog beside any workload (or the janitor)
  python -m sandboxes.gke_ops down           # after a run: nothing left that bills (then: gke_ops residue)
  Billing export to BigQuery (console: Billing > Billing export) for scripts/verify_costs_gke.py
EOF
