"""The GKE price lookup and billing reconciliation scripts, offline: SKU selection against the decoys the Cloud Billing Catalog
really has, and the ledger side of the reconciliation against ledgers the backend and meter wrote."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def load(rel: str):
    spec = importlib.util.spec_from_file_location(rel.replace("/", "_").removesuffix(".py"), ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


prices = load("scripts/gke/prices.py")
verify = load("scripts/verify_costs_gke.py")


BASE = {"h": ("s", 3600), "GBy.h": ("By.s", 3.6e12), "GiBy.h": ("By.s", 2 ** 30 * 3600)}


def sku(desc, usage, price, unit="h", regions=("us-central1", "us-east1"), sid=None):
    units, nanos = int(price), round((price - int(price)) * 1e9)
    base, factor = BASE[unit]
    return {"description": desc, "skuId": sid or desc[:8], "serviceRegions": list(regions), "category": {"usageType": usage},
            "pricingInfo": [{"pricingExpression": {"usageUnit": unit, "baseUnit": base, "baseUnitConversionFactor": factor,
                                                   "tieredRates": [
                {"unitPrice": {"units": "0", "nanos": 0}}, {"unitPrice": {"units": str(units), "nanos": nanos}}]}}]}


CATALOG = [
    sku("C4D Instance Core running in Americas", "OnDemand", 0.0400),
    sku("C4D Instance Ram running in Americas", "OnDemand", 0.0050, "GBy.h"),
    sku("Spot Preemptible C4D Instance Core running in Americas", "Preemptible", 0.0120),
    sku("Spot Preemptible C4D Instance Ram running in Americas", "Preemptible", 0.0015, "GBy.h"),
    # decoys
    sku("C4 Instance Core running in Americas", "OnDemand", 0.0480),
    sku("C4D Custom Instance Core running in Americas", "OnDemand", 0.0500),
    sku("C4D Sole Tenancy Instance Core running in Americas", "OnDemand", 0.0440),
    sku("Commitment v1: C4D Cpu in Americas for 1 Year", "Commit1Yr", 0.0250),
    sku("C4D Instance Core running in Frankfurt", "OnDemand", 0.0460, regions=("europe-west3",)),
    sku("Spot Preemptible C4D Instance Core running in Frankfurt", "Preemptible", 0.0140, regions=("europe-west3",)),
]


GiB = 2 ** 30


def test_a_machine_is_priced_from_exactly_its_family_region_and_usage_skus():
    p = prices.machine_price(CATALOG, "C4D", "us-central1", 192, 720 * GiB)
    assert p["ondemand"] == pytest.approx(192 * 0.04 + 720 * GiB / 1e9 * 0.005, rel=1e-4)    # RAM priced per 10^9 bytes
    assert p["spot"] == pytest.approx(192 * 0.012 + 720 * GiB / 1e9 * 0.0015, rel=1e-4)


def test_ram_priced_per_gib_is_converted_through_its_own_base_unit():
    gib_catalog = [s for s in CATALOG if "Ram" not in s["description"]] + [
        sku("C4D Instance Ram running in Americas", "OnDemand", 0.0050, "GiBy.h"),
        sku("Spot Preemptible C4D Instance Ram running in Americas", "Preemptible", 0.0015, "GiBy.h")]
    p = prices.machine_price(gib_catalog, "C4D", "us-central1", 192, 720 * GiB)
    assert p["ondemand"] == pytest.approx(192 * 0.04 + 720 * 0.005, rel=1e-4)
    assert "Spot Preemptible C4D Instance Core" in p["skus"]["spot Core"]


def test_a_missing_or_ambiguous_sku_is_an_error_never_a_guess():
    with pytest.raises(ValueError, match="0 candidate"):
        prices.machine_price(CATALOG, "C4D", "europe-west3", 192, 720 * GiB)          # no RAM SKUs there
    twice = CATALOG + [sku("C4D AMD Instance Core running in Americas", "OnDemand", 0.041)]
    with pytest.raises(ValueError, match="2 candidate"):
        prices.machine_price(twice, "C4D", "us-central1", 192, 720 * GiB)
    with pytest.raises(ValueError):
        prices.machine_price(CATALOG, "N4", "us-central1", 4, 16 * GiB)


SAMPLE = """# a comment that must survive
gke:
  project: p                              # and this one
gke_meter:
  machine_prices:
    c4d-standard-192: {spot: 3.40}        # old
  disk_gb_month: 0.10
"""


def test_writing_a_price_keeps_the_rest_of_the_config():
    out = prices.update_config(SAMPLE, "c4d-standard-192", {"spot": 4.0293, "ondemand": 9.0716})
    assert "c4d-standard-192: {spot: 4.0293, ondemand: 9.0716}" in out and "{spot: 3.40}" not in out
    assert "# a comment that must survive" in out and "# and this one" in out and out.count("\n") == SAMPLE.count("\n")
    out2 = prices.update_config(out, "c3d-standard-180", {"spot": 1.9732, "ondemand": 8.1718}, {"vcpus": 180, "mem_gib": 720})
    import yaml
    meter = yaml.safe_load(out2)["gke_meter"]
    assert meter["machine_prices"]["c3d-standard-180"] == {"spot": 1.9732, "ondemand": 8.1718}
    assert meter["machine_shapes"] == {"c3d-standard-180": {"vcpus": 180, "mem_gib": 720}}
    assert meter["machine_prices"]["c4d-standard-192"]["spot"] == 4.0293 and meter["disk_gb_month"] == 0.10


def test_the_real_config_parses_and_every_enabled_pool_has_a_shape_and_a_price():
    import yaml
    cfg = yaml.safe_load((ROOT / "configs" / "gke.yaml").read_text())
    meter = cfg["gke_meter"]
    for p in cfg["gke"]["pools"]:
        assert p["machine"] in meter["machine_shapes"] and p["machine"] in meter["machine_prices"], p["name"]
        assert meter["machine_prices"][p["machine"]].get("spot" if p.get("spot", True) else "ondemand"), p["name"]


def write_ledger(path: Path, records: list) -> Path:
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    return path


def test_the_ledger_side_counts_gke_records_in_the_window_only(tmp_path):
    led = write_ledger(tmp_path / "a.jsonl", [
        {"t": 100, "ev": "open", "id": "x", "provider": "gke"},
        {"t": 200, "ev": "close", "id": "x", "provider": "gke", "usd": 1.0},
        {"t": 150, "ev": "storage", "provider": "gke", "kind": "node", "usd": 2.0},
        {"t": 160, "ev": "storage", "provider": "gke", "kind": "cluster_fee", "usd": 0.1},
        {"t": 170, "ev": "storage", "provider": "gke", "kind": "meter_gap", "usd": 0.0, "max_usd": 0.5},
        {"t": 171, "ev": "storage", "provider": "gke", "kind": "meter_gap", "usd": 0.0},
        {"t": 180, "ev": "open", "id": "y", "provider": "gke"},
        {"t": 150, "ev": "close", "id": "m", "provider": "modal", "usd": 9.0},
        {"t": 999, "ev": "storage", "provider": "gke", "kind": "node", "usd": 7.0},
    ])
    s = verify.ledger_side(0, 500, [led])
    assert s["total"] == pytest.approx(3.1) and s["gap_max_usd"] == 0.5 and s["gaps_unbounded"] == 1 and s["still_open"] == 1


def test_the_verdict_never_lets_an_under_charge_pass():
    assert verify.verdict(10.0, 10.0)[0] and verify.verdict(9.85, 10.0)[0]
    ok, line = verify.verdict(9.0, 10.0)
    assert not ok and "misses $1.0000" in line
    ok, line = verify.verdict(9.0, 10.0, gap_max=2.0)
    assert not ok and "gap records" in line                       # explained, but still short: still a failure
    ok, line = verify.verdict(13.0, 10.0)
    assert ok and "over-charges" in line


def test_the_ledger_side_matches_what_the_backend_and_meter_wrote(tmp_path, monkeypatch):
    """End to end on the fake cluster, with the pods' rate (SANDBOX_GKE_VCPU_HOUR's default, 3.40/192) different from the node
    price (3.60): a pod open for the hour, a meter pass in the middle and one at the end. Pods + meter = the node's hour + fee."""
    import asyncio
    import time
    from types import SimpleNamespace
    from sandboxes import cost, gke_meter
    from sandboxes.backends import gke_raw
    import gke_fakes as fk
    from test_gke_raw import spec
    monkeypatch.setenv("SANDBOX_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(tmp_path / "ledger.jsonl"))
    monkeypatch.setenv("SANDBOX_GKE_CONFIG", str(tmp_path / "none.yaml"))
    t0 = time.time()
    clock = [t0]
    monkeypatch.setattr(cost, "time", SimpleNamespace(time=lambda: clock[0]))       # the ledger's clock
    api = fk.FakeCore(now=lambda: clock[0])
    api.cluster_created = t0
    api.nodes = [fk.make_node("gke-c-sandbox-spot-1", t0)]
    monkeypatch.setattr(gke_raw, "_core", lambda: api)
    monkeypatch.setattr(gke_raw, "_stream_exec", fk.local_exec)
    cfg = gke_meter.MeterConfig(machine_prices={"c4d-standard-192": {"spot": 3.6}}, disk_gb_month=0, cluster_fee_hour=0.1)
    monkeypatch.setattr(gke_meter.MeterConfig, "load", classmethod(lambda cls, path=None: cfg))
    monkeypatch.setattr(gke_meter, "MIN_INTERVAL_S", 1e9)                              # only the forced passes below
    sb = gke_raw.GKERaw(spec(cpu=1))
    asyncio.run(sb.start())
    api.pods[sb.sandbox_id].spec.node_name = "gke-c-sandbox-spot-1"
    gke_meter.meter_once(api, force=True, now=t0 + 1800)
    clock[0] = t0 + 3600
    asyncio.run(sb.stop())
    gke_meter.meter_once(api, force=True, now=t0 + 3600)
    s = verify.ledger_side(t0 - 10, t0 + 3700, [tmp_path / "ledger.jsonl"])
    assert s["pods"] == pytest.approx(3.40 / 192, rel=1e-3) and s["still_open"] == 0
    assert s["total"] == pytest.approx(3.6 + 0.1, rel=1e-4)
