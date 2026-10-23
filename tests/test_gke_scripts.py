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


def sku(desc, usage, price, unit="h", regions=("us-central1", "us-east1"), sid=None):
    units, nanos = int(price), round((price - int(price)) * 1e9)
    return {"description": desc, "skuId": sid or desc[:8], "serviceRegions": list(regions), "category": {"usageType": usage},
            "pricingInfo": [{"pricingExpression": {"usageUnit": unit, "tieredRates": [
                {"unitPrice": {"units": "0", "nanos": 0}}, {"unitPrice": {"units": str(units), "nanos": nanos}}]}}]}


CATALOG = [
    sku("C4D Instance Core running in Americas", "OnDemand", 0.0400),
    sku("C4D Instance Ram running in Americas", "OnDemand", 0.0050, "GiBy.h"),
    sku("Spot Preemptible C4D Instance Core running in Americas", "Preemptible", 0.0120),
    sku("Spot Preemptible C4D Instance Ram running in Americas", "Preemptible", 0.0015, "GiBy.h"),
    # decoys
    sku("C4 Instance Core running in Americas", "OnDemand", 0.0480),
    sku("C4D Custom Instance Core running in Americas", "OnDemand", 0.0500),
    sku("C4D Sole Tenancy Instance Core running in Americas", "OnDemand", 0.0440),
    sku("Commitment v1: C4D Cpu in Americas for 1 Year", "Commit1Yr", 0.0250),
    sku("C4D Instance Core running in Frankfurt", "OnDemand", 0.0460, regions=("europe-west3",)),
    sku("Spot Preemptible C4D Instance Core running in Frankfurt", "Preemptible", 0.0140, regions=("europe-west3",)),
]


def test_a_machine_is_priced_from_exactly_its_family_region_and_usage_skus():
    p = prices.machine_price(CATALOG, "C4D", "us-central1", 192, 720)
    assert p["ondemand"] == pytest.approx(192 * 0.04 + 720 * 0.005)
    assert p["spot"] == pytest.approx(192 * 0.012 + 720 * 0.0015)
    assert "Spot Preemptible C4D Instance Core" in p["skus"]["spot Core"]


def test_a_missing_or_ambiguous_sku_is_an_error_never_a_guess():
    with pytest.raises(ValueError, match="0 candidate"):
        prices.machine_price(CATALOG, "C4D", "europe-west3", 192, 720)          # no RAM SKUs there
    twice = CATALOG + [sku("C4D AMD Instance Core running in Americas", "OnDemand", 0.041)]
    with pytest.raises(ValueError, match="2 candidate"):
        prices.machine_price(twice, "C4D", "us-central1", 192, 720)
    with pytest.raises(ValueError):
        prices.machine_price(CATALOG, "N4", "us-central1", 4, 16)


def test_writing_a_price_keeps_the_rest_of_the_config():
    text = (ROOT / "configs" / "gke.yaml").read_text()
    out = prices.update_config(text, "c4d-standard-192", {"spot": 2.65, "ondemand": 11.28})
    assert "c4d-standard-192: {spot: 2.65, ondemand: 11.28}" in out and "c4d-standard-192: {spot: 3.40}" not in out
    assert out.count("\n") == text.count("\n") and "# Cluster coordinates for gke_ops" in out
    out2 = prices.update_config(out, "n4-standard-80", {"spot": 1.0, "ondemand": 3.0})
    assert "n4-standard-80: {spot: 1.0, ondemand: 3.0}" in out2 and out2.count("\n") == text.count("\n") + 1
    import yaml
    cfg = yaml.safe_load(out2)["gke_meter"]["machine_prices"]
    assert cfg["c4d-standard-192"] == {"spot": 2.65, "ondemand": 11.28} and cfg["n4-standard-80"]["spot"] == 1.0


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
