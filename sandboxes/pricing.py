"""Cost models: what each provider actually bills for, in USD.

Every provider charges differently, and not only for running time:

    daytona   vCPU-hour + GiB-RAM-hour + GiB-disk-hour (first 5 GiB of disk free), per second
    fly       vCPU-second (performance or shared presets) + GB-second above the RAM included with the preset
    modal     core-second + GiB-second ($0.00003942 / $0.00000667); VM runtime has a large root disk
    e2b       vCPU-second + GiB-RAM-second; storage, templates, pause are free; the plan fee
              ($150/mo Pro) and concurrency add-ons (600 -> $650/mo, 1100 -> $1150/mo) are fixed
    blaxel    GB-RAM-second only (CPU scales with RAM and is not billed separately), plus storage:
              snapshots $0.20/GB-month, images $0.045/GB-month, volumes $0.12/GB-month
    docker    local, free

The numbers are list prices fetched 2026-10-06 (daytona.io/pricing, e2b.dev/pricing,
blaxel.ai/pricing). Override any field per provider in the config (`pricing:` mapping) when a
contract differs. `CostModel` is pure arithmetic; `sandboxes.cost` does the bookkeeping.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from typing import Optional

HOURS_PER_MONTH = 730.0


@dataclass(frozen=True)
class CostModel:
    provider: str
    vcpu_hour: float = 0.0               # USD per vCPU-hour of a running sandbox
    ram_gib_hour: float = 0.0            # USD per GiB-RAM-hour of a running sandbox
    cpu_included_in_ram: bool = False    # True: vcpu_hour is ignored (CPU rides on the RAM price)
    included_ram_gib_per_vcpu: float = 0.0   # RAM that comes with each vCPU and is not billed separately (Fly performance: 2)
    disk_gib_hour: float = 0.0           # USD per GiB-disk-hour of a running sandbox
    free_disk_gib: float = 0.0           # disk allowance that is not billed
    snapshot_gib_month: float = 0.0      # persistent snapshot storage
    image_gib_month: float = 0.0         # stored template / image storage
    volume_gib_month: float = 0.0        # provisioned volume capacity
    build_minute: float = 0.0            # image build time, if billed
    plan_monthly_usd: float = 0.0        # fixed plan fee (informational, not per-sandbox)
    max_concurrent: Optional[int] = None  # published plan concurrency (informational)
    notes: str = ""

    # -- running sandboxes ----------------------------------------------------------------
    def sandbox_usd(self, cpu: float, ram_gib: float, disk_gib: float, seconds: float) -> float:
        """Cost of one sandbox of this size that ran for `seconds` (billed per second)."""
        hours = max(0.0, seconds) / 3600.0
        cpu_part = 0.0 if self.cpu_included_in_ram else self.vcpu_hour * cpu
        disk_part = self.disk_gib_hour * max(0.0, disk_gib - self.free_disk_gib)
        billable_ram = max(0.0, ram_gib - self.included_ram_gib_per_vcpu * cpu)
        return (cpu_part + self.ram_gib_hour * billable_ram + disk_part) * hours

    def hourly_usd(self, cpu: float, ram_gib: float, disk_gib: float) -> float:
        return self.sandbox_usd(cpu, ram_gib, disk_gib, 3600.0)

    # -- stored artifacts (snapshots, images, volumes) -----------------------------------------
    def storage_usd(self, kind: str, gib: float, seconds: float) -> float:
        """`kind` is "snapshot" | "image" | "volume"; billed per GiB-month, prorated."""
        rate = {"snapshot": self.snapshot_gib_month, "image": self.image_gib_month,
                "volume": self.volume_gib_month}.get(kind)
        if rate is None:
            raise ValueError(f"unknown storage kind {kind!r} (snapshot|image|volume)")
        return rate * gib * (max(0.0, seconds) / 3600.0) / HOURS_PER_MONTH

    def build_usd(self, seconds: float) -> float:
        return self.build_minute * max(0.0, seconds) / 60.0


DEFAULT_MODELS: dict[str, CostModel] = {
    "daytona": CostModel(
        "daytona", vcpu_hour=0.0504, ram_gib_hour=0.0162, disk_gib_hour=0.000108, free_disk_gib=5.0,
        notes="per-second billing; snapshot/stopped-sandbox storage not published (billed 0 here)"),
    "e2b": CostModel(
        "e2b", vcpu_hour=0.000014 * 3600, ram_gib_hour=0.0000045 * 3600, plan_monthly_usd=150.0, max_concurrent=100,
        notes="storage/templates/pause free; Pro $150/mo, 100 concurrent (600: +$650/mo, 1100: +$1150/mo)"),
    "blaxel": CostModel(
        "blaxel", ram_gib_hour=0.0000115 * 3600, cpu_included_in_ram=True,
        snapshot_gib_month=0.20, image_gib_month=0.045, volume_gib_month=0.12,
        notes="CPU scales with RAM and is free; snapshots $0.20, images $0.045, volumes $0.12 per GB-month"),
    "modal": CostModel(
        "modal", vcpu_hour=0.00003942 * 3600, ram_gib_hour=0.00000667 * 3600, plan_monthly_usd=0.0, max_concurrent=100,
        notes="$0.00003942 per core-second (priced per `cpu` requested; Modal calls a core 2 vCPU) + $0.00000667 per GiB-second; "
              "Starter $0 with 100 containers and $30 credits, Team $250/mo with 5,000"),
    "fly": CostModel(
        "fly", vcpu_hour=0.000012732 * 3600, ram_gib_hour=0.000002316 * 3600, included_ram_gib_per_vcpu=2.0,
        notes="performance CPU, Ashburn list price: $0.000012732 per vCPU-second, 2 GB RAM per vCPU included, extra RAM $0.000002316 per GB-second; "
              "shared CPU is ~15x cheaper per vCPU (override: vcpu_hour 0.003047, included_ram_gib_per_vcpu 0.25); volumes $0.15/GB-month"),
    "docker": CostModel("docker", notes="local daemon, free"),
    # A pod's share of its node: the node's price per vCPU-hour, per requested vCPU. Google bills nodes, not pods; gke_meter
    # charges whatever the pods' shares do not cover, so this rate only splits a node's cost between runs. Set it with
    # SANDBOX_GKE_VCPU_HOUR (the node price / its vCPUs), never with a config `pricing:` override: the meter must use the same rate.
    "gke": CostModel(
        "gke", vcpu_hour=float(os.environ.get("SANDBOX_GKE_VCPU_HOUR", 3.40 / 192)),
        notes="default: c4d-standard-192 spot ~$3.40/h over 192 vCPU (us-central1, check the day's spot price); "
              "nodes, disks and the cluster fee are charged by gke_meter"),
}


def cost_model_for(provider: str, overrides: Optional[dict] = None) -> CostModel:
    """The default model of `provider` with any config `pricing:` overrides applied."""
    base = DEFAULT_MODELS.get(provider) or CostModel(provider)
    if not overrides:
        return base
    known = {k: v for k, v in overrides.items() if k in base.__dataclass_fields__ and k != "provider"}
    return replace(base, **known)
