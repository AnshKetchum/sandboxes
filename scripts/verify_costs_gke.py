#!/usr/bin/env python3
"""Reconcile the cost ledger's GKE spend with what Google billed: the Cloud Billing export in BigQuery.

    python scripts/verify_costs_gke.py --table P.billing.gcp_billing_export_v1_XXXXXX --project P --cluster sandboxes \
        --since 2026-10-24T10:00:00Z --until 2026-10-24T14:00:00Z
    python scripts/verify_costs_gke.py --ledger-only --since ... --until ...       # just the ledger side

The export lags (hours, up to a day) and is hourly: check a window that ended a day ago, on hour boundaries, and that covers whole
runs (from before the first sandbox to after the teardown), since a pod's cost is written when it closes.
  billing  Compute Engine and Kubernetes Engine cost, gross (before credits such as the free tier), of resources labelled
           goog-k8s-cluster-name=<cluster> (the nodes' VMs and disks) plus the Kubernetes Engine fee lines of the project
  ledger   every gke record in every ledger with t in the window: pod closes, node meter records, the cluster fee; meter_gap
           records say how much the meter could not see
Passes when ledger >= 98% of billing (the ledger may over-charge, never under-charge). Exit status 1 otherwise.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sandboxes.cost import all_ledger_paths      # noqa: E402

TOLERANCE = 0.98

QUERY = """
SELECT service.description AS service, sku.description AS sku, SUM(cost) AS cost,
       SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) AS credits
FROM `{table}`
WHERE project.id = @project
  AND usage_start_time >= TIMESTAMP(@since) AND usage_end_time <= TIMESTAMP(@until)
  AND (EXISTS (SELECT 1 FROM UNNEST(labels) l WHERE l.key = 'goog-k8s-cluster-name' AND l.value = @cluster)
       OR service.description = 'Kubernetes Engine')
GROUP BY service, sku
ORDER BY cost DESC
"""


def epoch(s: str) -> float:
    try:
        return float(s)
    except ValueError:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def iso(t: float) -> str:
    return dt.datetime.fromtimestamp(t, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ledger_side(since: float, until: float, paths=None) -> dict:
    """USD the ledgers put on gke with t in [since, until): pods, nodes, cluster fee; meter_gap upper bounds; pods still open."""
    out = {"pods": 0.0, "nodes": 0.0, "cluster_fee": 0.0, "builds": 0.0, "image_storage": 0.0, "gap_max_usd": 0.0,
           "gaps_unbounded": 0, "still_open": 0}
    opened, closed = set(), set()
    for p in paths if paths is not None else all_ledger_paths():
        try:
            lines = Path(p).read_text().splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("provider") != "gke" or not since <= float(r.get("t", 0)) < until:
                continue
            ev, kind = r.get("ev"), r.get("kind")
            if ev == "open":
                opened.add(r.get("id"))
            elif ev == "close":
                closed.add(r.get("id"))
                out["pods"] += float(r.get("usd") or 0)
            elif ev == "storage" and kind == "node":
                out["nodes"] += float(r.get("usd") or 0)
            elif ev == "storage" and kind == "cluster_fee":
                out["cluster_fee"] += float(r.get("usd") or 0)
            elif ev == "storage" and kind == "image_build":
                out["builds"] += float(r.get("usd") or 0)
            elif ev == "storage" and kind == "image_storage":
                out["image_storage"] += float(r.get("usd") or 0)
            elif ev == "storage" and kind == "meter_gap":
                if r.get("max_usd") is None:
                    out["gaps_unbounded"] += 1
                else:
                    out["gap_max_usd"] += float(r["max_usd"])
    out["still_open"] = len(opened - closed)
    out["total"] = out["pods"] + out["nodes"] + out["cluster_fee"]          # what the billing export's cluster query covers
    out["all"] = out["total"] + out["builds"] + out["image_storage"]          # + Cloud Build and Artifact Registry
    return out


def billing_side(table: str, project: str, cluster: str, since: float, until: float) -> list:
    """[{"service", "sku", "cost", "credits"}] from the billing export (the `bq` CLI that comes with the gcloud SDK)."""
    cmd = ["bq", "query", "--nouse_legacy_sql", "--format=json", f"--project_id={project}",
           f"--parameter=project:STRING:{project}", f"--parameter=cluster:STRING:{cluster}",
           f"--parameter=since:STRING:{iso(since)}", f"--parameter=until:STRING:{iso(until)}", QUERY.format(table=table)]
    rows = json.loads(subprocess.run(cmd, check=True, capture_output=True, text=True).stdout or "[]")
    return [{"service": r["service"], "sku": r["sku"], "cost": float(r["cost"] or 0), "credits": float(r["credits"] or 0)} for r in rows]


def verdict(ledger_total: float, billed: float, gap_max: float = 0.0) -> tuple[bool, str]:
    """(passed, one line). Under-charging beyond the tolerance fails, unless the meter's own gap records account for it."""
    if billed <= 0:
        return ledger_total >= 0, f"nothing billed in the window; ledger ${ledger_total:.4f}"
    ratio = ledger_total / billed
    if ratio >= TOLERANCE:
        tag = "OK" if ratio <= 1.15 else "OK (ledger over-charges by more than 15%: check the prices in configs/gke.yaml)"
        return True, f"{tag}: ledger ${ledger_total:.4f} / billed ${billed:.4f} = {ratio:.3f}"
    if ledger_total + gap_max >= TOLERANCE * billed:
        return False, (f"UNDER: ledger ${ledger_total:.4f} / billed ${billed:.4f} = {ratio:.3f}, within the meter's gap records "
                       f"(up to ${gap_max:.4f}): the meter was down; the ledger is short by ${billed - ledger_total:.4f}")
    return False, f"UNDER: ledger ${ledger_total:.4f} / billed ${billed:.4f} = {ratio:.3f}: the ledger misses ${billed - ledger_total:.4f}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since", required=True, help="ISO time or epoch")
    ap.add_argument("--until", required=True)
    ap.add_argument("--table", help="PROJECT.DATASET.gcp_billing_export_v1_XXXXXX")
    ap.add_argument("--project")
    ap.add_argument("--cluster", default="sandboxes")
    ap.add_argument("--ledger-only", action="store_true")
    a = ap.parse_args(argv)
    since, until = epoch(a.since), epoch(a.until)
    led = ledger_side(since, until)
    print(f"ledger {iso(since)} .. {iso(until)}: pods ${led['pods']:.4f} + nodes ${led['nodes']:.4f} + cluster fee "
          f"${led['cluster_fee']:.4f} = ${led['total']:.4f}")
    if led["gap_max_usd"] or led["gaps_unbounded"]:
        print(f"  meter gaps: up to ${led['gap_max_usd']:.4f} more, {led['gaps_unbounded']} of unknown size")
    if led["still_open"]:
        print(f"  {led['still_open']} sandbox(es) opened in the window are not closed in it: widen the window to whole runs")
    if a.ledger_only:
        return 0
    if not (a.table and a.project):
        ap.error("--table and --project are required (or --ledger-only)")
    if until > dt.datetime.now(dt.timezone.utc).timestamp() - 24 * 3600:
        print("  note: the window ended less than 24 h ago; the export may not have all of it yet")
    rows = billing_side(a.table, a.project, a.cluster, since, until)
    billed = sum(r["cost"] for r in rows)
    for r in rows[:15]:
        print(f"  billed {r['service']:<18} {r['sku'][:60]:<60} ${r['cost']:.4f}")
    ok, line = verdict(led["total"], billed, led["gap_max_usd"])
    print(line)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
