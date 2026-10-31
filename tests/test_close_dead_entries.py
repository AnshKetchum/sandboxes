"""A killed process leaves ledger entries open: they must stop counting against the spend cap once older than any sandbox can live."""
import json

from sandboxes import fleet
from sandboxes.cost import CostLedger


def test_close_dead_entries_closes_only_the_old_open_ones(tmp_path):
    path = tmp_path / "costs.jsonl"
    led = CostLedger(path)
    now = 1_000_000.0
    rows = [{"t": now - 5 * 3600, "ev": "open", "id": "old", "provider": "daytona", "run": "", "cpu": 1, "ram_gib": 2, "disk_gib": 8},
            {"t": now - 600, "ev": "open", "id": "young", "provider": "daytona", "run": "", "cpu": 1, "ram_gib": 2, "disk_gib": 8},
            {"t": now - 6 * 3600, "ev": "open", "id": "done", "provider": "daytona", "run": "", "cpu": 1, "ram_gib": 2, "disk_gib": 8},
            {"t": now - 5 * 3600, "ev": "close", "id": "done", "provider": "daytona", "seconds": 60, "usd": 0.01}]
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    assert fleet.close_dead_entries(3 * 3600.0, path=str(path), now=now) == 1
    closes = [json.loads(l) for l in path.read_text().splitlines() if json.loads(l)["ev"] == "close"]
    assert {c["id"] for c in closes} == {"done", "old"}
    assert fleet.close_dead_entries(3 * 3600.0, path=str(path), now=now) == 0         # idempotent
