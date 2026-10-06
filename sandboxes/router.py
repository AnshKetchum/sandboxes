"""SandboxRouter: one entry point over every usable provider.

    router = SandboxRouter.from_config("/ansh/sandboxes/configs/multi.yaml")
    sbx = await router.open(image="registry/…:latest", boot_cmd="dind-start.sh", require=("dind",))
    ...
    await router.close(sbx)

Each `open` goes to the provider with the most free capacity (peak_concurrent_sandboxes minus what
is in use, scaled by weight; ties by priority). A provider that answers with a capacity/rate-limit
error is cooled down and the sandbox goes to the next one, and the in-use count at that moment is
remembered as its observed ceiling, so the router stops sending more there until something closes.
Per-provider `spend_cap_usd` and the router-wide `total_spend_cap_usd` are checked against the cost
ledger before every open.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence, Union

from .config import load_provider_entries
from .cost import get_ledger, summary
from .errors import CapacityError, ConfigError, SandboxError
from .models import SandboxProviderConfig
from .pricing import cost_model_for
from .raw import RawSandbox, SandboxSpec
from .registry import PROVIDERS, capabilities, load_class, usable

COOLDOWN_S = 20.0
SPEND_CACHE_S = 30.0


def _cap_divisor() -> float:
    """Data-parallel runs start several processes, each with its own router, against one provider
    account. SANDBOX_CAP_DIVISOR=N makes each process use 1/N of every provider's peak."""
    try:
        return max(1.0, float(os.environ.get("SANDBOX_CAP_DIVISOR", "1")))
    except ValueError:
        return 1.0


class NoProviderAvailable(SandboxError):
    pass


@dataclass
class _Slot:
    cfg: SandboxProviderConfig
    caps: dict
    in_use: int = 0
    opened: int = 0
    cool_until: float = 0.0
    observed_cap: Optional[int] = None
    capacity_errors: int = 0

    @property
    def name(self) -> str:
        return self.cfg.name.lower()

    @property
    def cap(self) -> float:
        c = self.cfg.peak_concurrent_sandboxes
        c = float("inf") if not c else max(1.0, float(c) / _cap_divisor())
        return min(c, float(self.observed_cap)) if self.observed_cap else c


class SandboxRouter:
    def __init__(self, configs: Sequence[SandboxProviderConfig], *, env=None, total_spend_cap_usd: Optional[float] = None,
                 log=print):
        self.slots: list[_Slot] = []
        self.skipped: dict[str, str] = {}
        self.total_spend_cap_usd = total_spend_cap_usd
        self._lock = threading.Lock()
        for cfg in configs:
            ok, why = usable(cfg, env)
            if ok:
                self.slots.append(_Slot(cfg, capabilities(cfg)))
            else:
                self.skipped[cfg.name] = why
        for n, why in self.skipped.items():
            log(f"[sandboxes] provider {n} skipped: {why}")
        if not self.slots:
            raise ConfigError("no usable sandbox provider (" + "; ".join(f"{n}: {w}" for n, w in self.skipped.items()) + ")")

    @classmethod
    def from_config(cls, path: Union[str, Path, None] = None, **kw) -> "SandboxRouter":
        entries = load_provider_entries(path)
        return cls([SandboxProviderConfig.model_validate(e) for e in entries["providers"]],
                   total_spend_cap_usd=kw.pop("total_spend_cap_usd", entries.get("total_spend_cap_usd")), **kw)

    # -- capacity ---------------------------------------------------------------------------------
    def total_capacity(self, require: Sequence[str] = ()) -> Optional[int]:
        """Sum of the peaks of the providers that can serve `require` (None if any is unbounded)."""
        slots = [s for s in self.slots if all(s.caps.get(r) for r in require)]
        if any(s.cap == float("inf") for s in slots):
            return None
        return int(sum(s.cap for s in slots))

    def status(self) -> dict:
        return {s.name: {"in_use": s.in_use, "cap": None if s.cap == float("inf") else int(s.cap),
                         "opened": s.opened, "capacity_errors": s.capacity_errors,
                         "cooling": max(0.0, round(s.cool_until - time.time(), 1))} for s in self.slots}

    def _ledger_summary(self) -> dict:
        """The cost ledger replay, cached for SPEND_CACHE_S (it is a file scan; opens are frequent)."""
        now = time.time()
        if now - getattr(self, "_sum_at", 0.0) > SPEND_CACHE_S:
            self._sum, self._sum_at = summary(), now
        return self._sum

    def _spend_ok(self, slot: _Slot) -> bool:
        cap = slot.cfg.spend_cap_usd
        if not cap:
            return True
        return self._ledger_summary().get("providers", {}).get(slot.name, {}).get("total_usd", 0.0) < cap

    def _pick(self, require: Sequence[str], skip: set) -> Optional[_Slot]:
        now = time.time()
        best, best_score = None, None
        for s in self.slots:
            if s.name in skip or s.cool_until > now or s.in_use >= s.cap:
                continue
            if not all(s.caps.get(r) for r in require):
                continue
            free = (s.cap - s.in_use) if s.cap != float("inf") else 1e9
            score = (min(free, 1e9) * max(s.cfg.weight, 1e-9), -s.cfg.priority)
            if best_score is None or score > best_score:
                best, best_score = s, score
        return best

    # -- open / close -------------------------------------------------------------------------------
    def make_spec(self, slot: _Slot, image: str, **kw) -> SandboxSpec:
        cfg = slot.cfg
        spec = SandboxSpec(image=cfg.image or image,
                           cpu=int(kw.pop("cpu", None) or cfg.cpu or 2),
                           mem_gb=int(kw.pop("mem_gb", None) or ((cfg.memory_mb or 4096) / 1024)),
                           disk_gb=int(kw.pop("disk_gb", None) or cfg.disk_gb or 10),
                           lifetime_s=int((cfg.auto_delete_minutes or 240) * 60), extra=dict(cfg.extra or {}))
        for k, v in kw.items():
            setattr(spec, k, v)
        if kw.get("image"):
            spec.image = kw["image"]
        return spec

    async def open(self, image: str, *, require: Sequence[str] = (), wait_s: float = 600.0, **spec_kw) -> RawSandbox:
        """Start a sandbox on the best provider. Waits up to `wait_s` for capacity before failing."""
        if self.total_spend_cap_usd:
            tot = self._ledger_summary()["total_usd"]
            if tot >= self.total_spend_cap_usd:
                from .cost import SpendCapExceeded
                raise SpendCapExceeded(f"sandbox spend ${tot:.2f} reached the ${self.total_spend_cap_usd:.2f} cap")
        deadline = time.time() + wait_s
        tried: set = set()
        while True:
            with self._lock:
                slot = self._pick(require, tried)
                if slot is not None and not self._spend_ok(slot):
                    tried.add(slot.name); continue
                if slot is not None:
                    slot.in_use += 1
            if slot is None:
                if time.time() > deadline:
                    raise NoProviderAvailable(f"no provider with free capacity for require={list(require)}: {self.status()}")
                tried.clear()
                await asyncio.sleep(2.0)
                continue
            spec = self.make_spec(slot, image, **dict(spec_kw))
            info = PROVIDERS[slot.name]
            if not info.raw:
                with self._lock:
                    slot.in_use -= 1
                raise SandboxError(f"provider {slot.name} has no async backend")
            sbx = load_class(info.raw)(spec, cost_model_for(slot.name, slot.cfg.pricing))
            try:
                await sbx.start()
            except CapacityError:
                with self._lock:
                    slot.in_use -= 1
                    slot.capacity_errors += 1
                    slot.cool_until = time.time() + COOLDOWN_S
                    if slot.in_use > 0:
                        slot.observed_cap = slot.in_use      # we hit the ceiling at this many
                tried.add(slot.name)
                continue
            except BaseException:
                with self._lock:
                    slot.in_use -= 1
                raise
            with self._lock:
                slot.opened += 1
            sbx._router_slot = slot
            return sbx

    async def close(self, sbx: RawSandbox, sync_dirs: tuple = ()) -> float:
        slot = getattr(sbx, "_router_slot", None)
        try:
            return await sbx.stop(sync_dirs=sync_dirs)
        finally:
            if slot is not None:
                with self._lock:
                    slot.in_use = max(0, slot.in_use - 1)
                    sbx._router_slot = None
                    if slot.observed_cap and slot.in_use < slot.observed_cap - 5:
                        slot.observed_cap = None          # headroom came back: probe again
