#!/usr/bin/env python3
"""Compare providers' cost: the list-price $/hour of one sandbox at a few sizes (every provider in pricing.DEFAULT_MODELS),
and, from benchmark_concurrency.py result files, $/hour per unit of measured CPU work at each concurrency level.

    python scripts/compare_rates.py [scripts/benchmark_results/bench-*.json ...]
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sandboxes.pricing import DEFAULT_MODELS     # noqa: E402

SIZES = [(1, 2, 5), (2, 4, 10), (4, 8, 20)]

print("list price, $/hour per sandbox (vCPU / GiB RAM / GB disk)")
print(f"{'provider':9}" + "".join(f"{f'{c}/{m}/{d}':>12}" for c, m, d in SIZES))
for name, model in sorted(DEFAULT_MODELS.items(), key=lambda kv: kv[1].hourly_usd(2, 4, 10)):
    if name != "docker":
        print(f"{name:9}" + "".join(f"{model.hourly_usd(c, m, d):>12.4f}" for c, m, d in SIZES))

if sys.argv[1:]:
    print("\nmeasured: CPU burn p50 (million loop iterations per --hold) and $/hour per 1,000M of it")
for path in sys.argv[1:]:
    data = json.load(open(path))
    for level in data["levels"]:
        node = data["summary"][level["provider"]]["node_usd_per_hour"]
        burn = level.get("burn_p50") or 0
        cost = f"{node / (burn / 1e9):.3f}" if burn else "-"
        print(f"{level['provider']:8} N={level['n']:<5} burn p50 {burn / 1e6:8.1f}M  node ${node:.3f}/h  $/h per 1,000M: {cost}")
