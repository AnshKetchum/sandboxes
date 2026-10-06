"""Cost models: what each provider actually bills for, in USD.

Every provider charges differently, and not only for running time:

    daytona   vCPU-hour + GiB-RAM-hour + GiB-disk-hour (first 5 GiB of disk free), per second
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

from dataclasses import dataclass, field, replace
from typing import Optional

HOURS_PER_MONTH = 730.0


@dataclass(frozen=True)
class CostModel:
    provider: str
    vcpu_hour: float = 0.0               # USD per vCPU-hour of a running sandbox
    ram_gib_hour: float = 0.0            # USD per GiB-RAM-hour of a running sandbox
    cpu_included_in_ram: bool = False    # True: vcpu_hour is ignored (CPU rides on the RAM price)
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
        return (cpu_part + self.ram_gib_hour * ram_gib + disk_part) * hours

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
    "docker": CostModel("docker", notes="local daemon, free"),
}


def cost_model_for(provider: str, overrides: Optional[dict] = None) -> CostModel:
    """The default model of `provider` with any config `pricing:` overrides applied."""
    base = DEFAULT_MODELS.get(provider) or CostModel(provider)
    if not overrides:
        return base
    known = {k: v for k, v in overrides.items() if k in base.__dataclass_fields__ and k != "provider"}
    return replace(base, **known)
