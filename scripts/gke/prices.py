#!/usr/bin/env python3
"""Machine types' shape and on-demand / spot price per hour in a region, from Compute Engine and the Cloud Billing Catalog (the
prices Google bills by), for configs/gke.yaml: `gke_meter.machine_prices` (the node meter) and `gke_meter.machine_shapes` (pool
placement: which pool runs a sandbox of a given size most cheaply).

    python scripts/gke/prices.py --project P --region us-central1 --machine c4d-standard-192 c4d-standard-32 n2d-standard-96
    python scripts/gke/prices.py --project P --region us-central1 --pools --write     # every machine of the pool catalog

Shape from `gcloud compute machine-types describe`; SKUs from cloudbilling.googleapis.com with `gcloud auth print-access-token`.
A price is the family's core SKU x vCPUs + its RAM SKU x memory, each converted through the SKU's own base unit (the catalog
prices RAM per 10^9 bytes: a 744 GiB machine is 798.9 of those). When the catalog has more than one candidate SKU for a part,
nothing is guessed: the candidates are printed and that machine fails.
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

import yaml

COMPUTE_SERVICE = "6F81-5844-456A"           # Compute Engine in the Cloud Billing Catalog
EXCLUDE = ("Custom", "Sole Tenancy", "Extended", "Commitment", "Reserved", "DWS", "Calendar", "Flex", "Premium")
CONFIG = Path(__file__).resolve().parents[2] / "configs" / "gke.yaml"


def unit_price(sku: dict) -> float:
    """USD per usage unit: the last (highest-usage) tier, which for compute is the only one."""
    rate = sku["pricingInfo"][0]["pricingExpression"]["tieredRates"][-1]["unitPrice"]
    return float(rate.get("units") or 0) + float(rate.get("nanos") or 0) / 1e9


def hourly(sku: dict, quantity: float) -> float:
    """USD per hour for `quantity` of the SKU's base resource in use: vCPUs for a core SKU (base unit "s"), bytes for a RAM SKU
    (base unit "By.s"). Converted through baseUnitConversionFactor, so GB and GiB pricing units both come out right."""
    pe = sku["pricingInfo"][0]["pricingExpression"]
    factor = float(pe.get("baseUnitConversionFactor") or 0)
    if factor and pe.get("baseUnit") in ("s", "By.s"):
        return unit_price(sku) * quantity * 3600.0 / factor
    raise ValueError(f"{sku.get('description')}: unexpected pricing unit {pe.get('usageUnit')} / base {pe.get('baseUnit')}")


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


def machine_price(skus: list, family: str, region: str, vcpus: float, mem_bytes: float) -> dict:
    """{"ondemand": USD/h, "spot": USD/h, "skus": {...}}; raises ValueError naming the candidates when a part is ambiguous or missing."""
    out, used = {}, {}
    for kind, spot in (("ondemand", False), ("spot", True)):
        total = 0.0
        for part, qty in (("Core", vcpus), ("Ram", mem_bytes)):
            c = candidates(skus, family, region, part, spot)
            if len(c) != 1:
                names = [s.get("description") for s in c]
                raise ValueError(f"{kind} {family} {part} in {region}: {len(c)} candidate SKUs {names}")
            total += hourly(c[0], qty)
            pe = c[0]["pricingInfo"][0]["pricingExpression"]
            used[f"{kind} {part}"] = f"{c[0]['description']} ({c[0].get('skuId')}): {unit_price(c[0]):.6f}/{pe.get('usageUnit')}"
        out[kind] = round(total, 4)
    out["skus"] = used
    return out


def set_entry(text: str, section: str, key: str, value: str, comment: str = "") -> str:
    """configs/gke.yaml with `key: value` set under `gke_meter.<section>` (created if missing); every other line kept."""
    line = f"    {key}: {value}" + (f"   # {comment}" if comment else "")
    lines = text.splitlines()
    try:
        meter = next(i for i, l in enumerate(lines) if l.rstrip() == "gke_meter:")
    except StopIteration:
        raise ValueError("configs/gke.yaml has no gke_meter section")
    start = next((i for i in range(meter + 1, len(lines)) if lines[i].rstrip() == f"  {section}:"), None)
    if start is None:
        lines.insert(meter + 1, f"  {section}:")
        lines.insert(meter + 2, line)
        return "\n".join(lines) + "\n"
    i = start + 1
    while i < len(lines) and lines[i].startswith("    "):
        if lines[i].strip().startswith(f"{key}:"):
            lines[i] = line
            return "\n".join(lines) + "\n"
        i += 1
    lines.insert(start + 1, line)
    return "\n".join(lines) + "\n"


def update_config(text: str, machine: str, prices: dict, shape: dict = None) -> str:
    """configs/gke.yaml with `machine`'s price (and shape) set (comments elsewhere kept)."""
    text = set_entry(text, "machine_prices", machine, f"{{spot: {prices['spot']}, ondemand: {prices['ondemand']}}}",
                     "Cloud Billing Catalog (scripts/gke/prices.py)")
    if shape:
        text = set_entry(text, "machine_shapes", machine, f"{{vcpus: {shape['vcpus']:g}, mem_gib: {shape['mem_gib']:g}}}")
    return text


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


def catalog_machines(path: Path = CONFIG) -> list:
    """The machine types of the pool catalog (configs/gke.yaml gke.pools)."""
    pools = ((yaml.safe_load(path.read_text()) or {}).get("gke") or {}).get("pools") or []
    return sorted({p["machine"] for p in pools if p.get("machine")})


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--project", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--machine", nargs="*", default=[])
    ap.add_argument("--pools", action="store_true", help="also every machine type of the pool catalog in configs/gke.yaml")
    ap.add_argument("--zone", default=None, help="a zone of the region that offers the machines (default <region>-a)")
    ap.add_argument("--write", action="store_true", help=f"update {CONFIG}")
    a = ap.parse_args(argv)
    machines = sorted(set(a.machine) | set(catalog_machines() if a.pools else []))
    if not machines:
        ap.error("name machines with --machine, or --pools")
    skus = fetch_skus(_gcloud("auth", "print-access-token").strip())
    text, failed = CONFIG.read_text(), 0
    for m in machines:
        try:
            mt = json.loads(_gcloud("compute", "machine-types", "describe", m, "--zone", a.zone or f"{a.region}-a",
                                    "--project", a.project, "--format", "json"))
        except subprocess.CalledProcessError as e:
            print(f"{m}: not offered in {a.zone or a.region + '-a'} ({(e.stderr or '').strip()[-200:]})", file=sys.stderr)
            failed += 1
            continue
        vcpus, mem_mb = float(mt["guestCpus"]), float(mt["memoryMb"])
        try:
            p = machine_price(skus, m.split("-")[0].upper(), a.region, vcpus, mem_mb * 2 ** 20)
        except ValueError as e:
            print(f"cannot price {m}: {e}", file=sys.stderr)
            failed += 1
            continue
        print(f"{m}: {vcpus:.0f} vCPU, {mem_mb / 1024:.0f} GiB   on demand ${p['ondemand']:.4f}/h   spot ${p['spot']:.4f}/h"
              f"   (spot ${p['spot'] / vcpus:.5f}/vCPU-h)")
        for k, v in p["skus"].items():
            print(f"    {k:<14} {v}")
        text = update_config(text, m, p, {"vcpus": vcpus, "mem_gib": mem_mb / 1024})
    if a.write:
        CONFIG.write_text(text)
        print(f"wrote {CONFIG}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
