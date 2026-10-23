#!/usr/bin/env python3
"""A machine type's on-demand and spot price per hour in a region, from the Cloud Billing Catalog (the prices Google bills by),
for the node meter's `gke_meter.machine_prices` in configs/gke.yaml.

    python scripts/gke/prices.py --project P --region us-central1 --machine c4d-standard-192            # print, with the SKUs used
    python scripts/gke/prices.py --project P --region us-central1 --machine c4d-standard-192 --write    # and update configs/gke.yaml

Machine shape from `gcloud compute machine-types describe`; SKUs from cloudbilling.googleapis.com with `gcloud auth
print-access-token`. A price is vCPUs x the family's core SKU + GiB x its RAM SKU (spot: the "Spot Preemptible" SKUs). When the
catalog has more than one candidate SKU for a part, nothing is guessed: the candidates are printed and the script fails.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path

COMPUTE_SERVICE = "6F81-5844-456A"           # Compute Engine in the Cloud Billing Catalog
EXCLUDE = ("Custom", "Sole Tenancy", "Extended", "Commitment", "Reserved", "DWS", "Calendar", "Flex", "Premium")
CONFIG = Path(__file__).resolve().parents[2] / "configs" / "gke.yaml"


def unit_price(sku: dict) -> float:
    """USD per usage unit: the last (highest-usage) tier, which for compute is the only one."""
    rate = sku["pricingInfo"][0]["pricingExpression"]["tieredRates"][-1]["unitPrice"]
    return float(rate.get("units") or 0) + float(rate.get("nanos") or 0) / 1e9


def candidates(skus: list, family: str, region: str, part: str, spot: bool) -> list:
    """SKUs for one part ("Core" or "Ram") of a machine family in a region, on demand or spot."""
    pat = re.compile(rf"^{'Spot Preemptible ' if spot else ''}{re.escape(family)}\b.*\bInstance {part}\b.*\brunning in\b", re.I)
    out = []
    for s in skus:
        d = s.get("description", "")
        if not pat.search(d) or any(x.lower() in d.lower() for x in EXCLUDE):
            continue
        if not spot and d.lower().startswith(("spot", "preemptible")):
            continue
        if region not in (s.get("serviceRegions") or []):
            continue
        usage = (s.get("category") or {}).get("usageType", "")
        if usage != ("Preemptible" if spot else "OnDemand"):
            continue
        out.append(s)
    return out


def machine_price(skus: list, family: str, region: str, vcpus: float, mem_gib: float) -> dict:
    """{"ondemand": USD/h, "spot": USD/h, "skus": {...}}; raises ValueError naming the candidates when a part is ambiguous or missing."""
    out, used = {}, {}
    for kind, spot in (("ondemand", False), ("spot", True)):
        total = 0.0
        for part, qty in (("Core", vcpus), ("Ram", mem_gib)):
            c = candidates(skus, family, region, part, spot)
            if len(c) != 1:
                names = [s.get("description") for s in c]
                raise ValueError(f"{kind} {family} {part} in {region}: {len(c)} candidate SKUs {names}")
            total += qty * unit_price(c[0])
            used[f"{kind} {part}"] = f"{c[0]['description']} ({c[0].get('skuId')}): {unit_price(c[0]):.6f}/{c[0]['pricingInfo'][0]['pricingExpression'].get('usageUnit')}"
        out[kind] = round(total, 4)
    out["skus"] = used
    return out


def update_config(text: str, machine: str, prices: dict) -> str:
    """configs/gke.yaml with `machine`'s line under gke_meter.machine_prices set (comments elsewhere kept)."""
    line = f"    {machine}: {{spot: {prices['spot']}, ondemand: {prices['ondemand']}}}   # Cloud Billing Catalog (scripts/gke/prices.py)"
    lines = text.splitlines()
    try:
        start = next(i for i, l in enumerate(lines) if l.strip() == "machine_prices:")
    except StopIteration:
        raise ValueError("configs/gke.yaml has no gke_meter.machine_prices section")
    i = start + 1
    while i < len(lines) and lines[i].startswith("    "):
        if lines[i].strip().startswith(f"{machine}:"):
            lines[i] = line
            return "\n".join(lines) + "\n"
        i += 1
    lines.insert(start + 1, line)
    return "\n".join(lines) + "\n"


def _gcloud(*args: str) -> str:
    return subprocess.run(["gcloud", *args], check=True, capture_output=True, text=True).stdout


def fetch_skus(token: str) -> list:
    skus, page = [], ""
    while True:
        q = urllib.parse.urlencode({"currencyCode": "USD", "pageSize": 5000, **({"pageToken": page} if page else {})})
        req = urllib.request.Request(f"https://cloudbilling.googleapis.com/v1/services/{COMPUTE_SERVICE}/skus?{q}",
                                     headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=60) as r:
            body = json.load(r)
        skus += body.get("skus", [])
        page = body.get("nextPageToken", "")
        if not page:
            return skus


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--project", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--machine", default="c4d-standard-192")
    ap.add_argument("--zone", default=None, help="a zone of the region that offers the machine (default <region>-a)")
    ap.add_argument("--write", action="store_true", help=f"update {CONFIG}")
    a = ap.parse_args(argv)
    mt = json.loads(_gcloud("compute", "machine-types", "describe", a.machine, "--zone", a.zone or f"{a.region}-a",
                            "--project", a.project, "--format", "json"))
    vcpus, mem_gib = float(mt["guestCpus"]), float(mt["memoryMb"]) / 1024.0
    family = a.machine.split("-")[0].upper()
    skus = fetch_skus(_gcloud("auth", "print-access-token").strip())
    try:
        p = machine_price(skus, family, a.region, vcpus, mem_gib)
    except ValueError as e:
        print(f"cannot price {a.machine}: {e}", file=sys.stderr)
        return 1
    print(f"{a.machine} in {a.region}: {vcpus:.0f} vCPU, {mem_gib:.0f} GiB")
    for k, v in p["skus"].items():
        print(f"  {k:<14} {v}")
    print(f"  on demand ${p['ondemand']:.4f}/h   spot ${p['spot']:.4f}/h")
    print(f"  pods' rate: export SANDBOX_GKE_VCPU_HOUR={p['spot'] / vcpus:.6f}   (spot price / vCPUs)")
    if a.write:
        CONFIG.write_text(update_config(CONFIG.read_text(), a.machine, p))
        print(f"  wrote {CONFIG}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
