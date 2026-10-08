"""Cost tracking for every sandbox any provider opens, across processes.

Each provider records an `open` event when a sandbox starts and a `close` event (with the dollar
cost from `pricing.CostModel`) when it ends, into one append-only JSONL ledger. Sandboxes that were
never closed (the process was killed) are counted as running until `max_age_s`, so a crash cannot
hide spend. Storage (snapshots, images, volumes) goes in as `storage` events.

    SANDBOX_COST_LEDGER=/path/costs.jsonl     where to write (default: the active session's ledger, else ~/.cache/sandboxes/costs.jsonl)
    SANDBOX_SESSION_DIR=<dir>                 set by sandboxes.session.open_session(): the active session's own ledger directory
    SANDBOX_RUN_ID=v0-scale-200               tag events so one run can be reported on its own

Each client (euphoria, greenhouse, pr_task_synthesis ...) opens a SESSION (sandboxes/session.py) and gets its own ledger,
~/.cache/sandboxes/sessions/<client>/<session id>/costs.jsonl, so its spend can be read on its own without filtering a shared file.
The spend caps still hold machine-wide: `summary_all()` sums the legacy ledger and every session ledger.

    python -m sandboxes.cost                  # totals per provider, now
    python -m sandboxes.cost --run v0-scale-200 --watch 30
    python -m sandboxes.cost --cap 120        # exit status 2 once the total passes $120

Ledger format (JSON lines, the one uniform shape every provider is normalized into; times are epoch seconds):

    {"ev":"open",    "t","id","provider","run","sid","cpu","ram_gib","disk_gib","labels"}   a sandbox started
    {"ev":"close",   "t","id","provider","run","seconds","usd"[, "note"]}                    it ended; usd from the provider's CostModel
    {"ev":"storage", "t","provider","run","kind","gib","seconds","usd","label"}              image / snapshot / volume storage, or a one-off charge

The ledger is the single place to look at sandbox spend. It does not know about LLM spend; the
caller adds that (`summary()["total_usd"]` is sandbox dollars only).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Optional

from .errors import SandboxError
from .pricing import CostModel, cost_model_for

LEDGER_ENV = "SANDBOX_COST_LEDGER"
RUN_ENV = "SANDBOX_RUN_ID"
SESSION_ENV = "SANDBOX_SESSION_DIR"
DEFAULT_LEDGER = Path.home() / ".cache" / "sandboxes" / "costs.jsonl"


def cache_dir() -> Path:
    return Path(os.environ.get("SANDBOX_CACHE_DIR", Path.home() / ".cache" / "sandboxes"))


def sessions_dir() -> Path:
    return cache_dir() / "sessions"


def default_ledger_path() -> Path:
    """Where this process writes: SANDBOX_COST_LEDGER, else the active session's ledger, else the legacy shared ledger."""
    if os.environ.get(LEDGER_ENV):
        return Path(os.environ[LEDGER_ENV])
    if os.environ.get(SESSION_ENV):
        return Path(os.environ[SESSION_ENV]) / "costs.jsonl"
    return Path(os.environ["SANDBOX_CACHE_DIR"]) / "costs.jsonl" if os.environ.get("SANDBOX_CACHE_DIR") else DEFAULT_LEDGER


def all_ledger_paths() -> list[Path]:
    """The legacy shared ledger plus every session ledger (what the machine-wide spend cap adds up). An explicit SANDBOX_COST_LEDGER is a
    deliberate redirect (tests, a one-off experiment): then that one file is the whole ledger, as before sessions existed."""
    if os.environ.get(LEDGER_ENV):
        return [Path(os.environ[LEDGER_ENV])]
    legacy = Path(os.environ["SANDBOX_CACHE_DIR"]) / "costs.jsonl" if os.environ.get("SANDBOX_CACHE_DIR") else DEFAULT_LEDGER
    out = [legacy] if legacy.exists() else []
    sd = sessions_dir()
    if sd.is_dir():
        out += sorted(sd.glob("*/*/costs.jsonl"))
    return out
# an `open` with no `close` older than this is treated as dead (killed process), billed up to here
from .lifetime import MAX_LIFETIME_MIN

STALE_AFTER_S = float(os.environ.get("SANDBOX_STALE_AFTER_S", MAX_LIFETIME_MIN * 60))      # past the lifetime ceiling a sandbox is dead


class SpendCapExceeded(SandboxError):
    """The ledger total passed a configured spend cap; no new sandbox should be opened."""


class CostLedger:
    def __init__(self, path: Optional[os.PathLike] = None, run: Optional[str] = None):
        self.path = Path(path) if path else default_ledger_path()
        self.run = run if run is not None else os.environ.get(RUN_ENV, "")
        self._lock = threading.Lock()
        self._open: dict[str, dict] = {}
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _append(self, rec: dict) -> None:
        line = json.dumps(rec, separators=(",", ":")) + "\n"
        with self._lock:
            # O_APPEND writes below PIPE_BUF are atomic, so several processes can share a file
            fd = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
            try:
                os.write(fd, line.encode())
            finally:
                os.close(fd)

    # -- sandboxes ------------------------------------------------------------------------------
    def open(self, provider: str, *, cpu: float, ram_gib: float, disk_gib: float,
             labels: Optional[dict] = None, sandbox_id: str = "") -> str:
        """Record a sandbox start; returns the handle to pass to `close`."""
        h = uuid.uuid4().hex[:12]
        rec = {"t": time.time(), "ev": "open", "id": h, "provider": provider, "run": self.run,
               "sid": sandbox_id, "cpu": cpu, "ram_gib": ram_gib, "disk_gib": disk_gib, "labels": labels or {}}
        self._open[h] = rec
        self._append(rec)
        return h

    def close(self, handle: str, model: CostModel) -> float:
        """Record a sandbox end; returns the dollar cost of that sandbox. Idempotent."""
        rec = self._open.pop(handle, None)
        if rec is None:
            return 0.0
        seconds = time.time() - rec["t"]
        usd = model.sandbox_usd(rec["cpu"], rec["ram_gib"], rec["disk_gib"], seconds)
        self._append({"t": time.time(), "ev": "close", "id": handle, "provider": rec["provider"], "run": self.run,
                      "seconds": round(seconds, 2), "usd": round(usd, 6)})
        return usd

    # -- storage and one-off charges ---------------------------------------------------------------
    def storage(self, provider: str, kind: str, gib: float, seconds: float, *, label: str = "",
                model: Optional[CostModel] = None) -> float:
        """Record `gib` GiB of snapshot/image/volume storage held for `seconds`."""
        usd = (model or cost_model_for(provider)).storage_usd(kind, gib, seconds)
        self._append({"t": time.time(), "ev": "storage", "provider": provider, "run": self.run,
                      "kind": kind, "gib": gib, "seconds": round(seconds, 1), "usd": round(usd, 6), "label": label})
        return usd

    def charge(self, provider: str, usd: float, *, kind: str, label: str = "") -> None:
        """A one-off charge (an image build, a plan fee share)."""
        self._append({"t": time.time(), "ev": "storage", "provider": provider, "run": self.run,
                      "kind": kind, "gib": 0, "seconds": 0, "usd": round(usd, 6), "label": label})


_ledger: Optional[CostLedger] = None
_ledger_lock = threading.Lock()


def get_ledger() -> CostLedger:
    """The process-wide ledger (path from SANDBOX_COST_LEDGER)."""
    global _ledger
    if os.environ.get("SANDBOX_SESSION_CLIENT") and not os.environ.get(SESSION_ENV) and not os.environ.get(LEDGER_ENV):
        from .session import ensure_env_session            # a launcher script opted this process in with env vars alone
        ensure_env_session()
    with _ledger_lock:
        want = default_ledger_path()
        if _ledger is None or want != _ledger.path:
            _ledger = CostLedger()
        return _ledger


# -- reading ---------------------------------------------------------------------------------------------
def summary(path: Optional[os.PathLike] = None, *, run: Optional[str] = None, models: Optional[dict] = None,
            now: Optional[float] = None) -> dict:
    """Replay the ledger. Returns per-provider and overall totals in dollars:

        {"providers": {name: {"opened", "running", "closed", "sandbox_hours", "closed_usd",
                              "running_usd", "storage_usd", "total_usd", "burn_usd_per_hour"}},
         "total_usd", "running_usd", "burn_usd_per_hour", "as_of"}
    """
    path = Path(path) if path else default_ledger_path()
    now = now or time.time()
    models = models or {}
    opens: dict[str, dict] = {}
    closes: dict[str, dict] = {}
    storage = defaultdict(float)
    if path.exists():
        with open(path) as f:
            for line in f:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if run is not None and r.get("run", "") != run:
                    continue
                if r["ev"] == "open":
                    opens[r["id"]] = r
                elif r["ev"] == "close":
                    closes[r["id"]] = r
                elif r["ev"] == "storage":
                    storage[r["provider"]] += r.get("usd", 0.0)
    out: dict[str, dict] = {}

    def row(p):
        return out.setdefault(p, {"opened": 0, "running": 0, "closed": 0, "sandbox_hours": 0.0, "closed_usd": 0.0,
                                  "running_usd": 0.0, "stale": 0, "storage_usd": 0.0, "burn_usd_per_hour": 0.0})
    for h, o in opens.items():
        p = o["provider"]; a = row(p); a["opened"] += 1
        c = closes.get(h)
        if c:
            a["closed"] += 1; a["closed_usd"] += c["usd"]; a["sandbox_hours"] += c["seconds"] / 3600.0
        else:
            m = models.get(p) or cost_model_for(p)
            age = now - o["t"]; billed = min(age, STALE_AFTER_S)
            a["running_usd"] += m.sandbox_usd(o["cpu"], o["ram_gib"], o["disk_gib"], billed)
            a["sandbox_hours"] += billed / 3600.0
            if age > STALE_AFTER_S:
                a["stale"] += 1
            else:
                a["running"] += 1
                a["burn_usd_per_hour"] += m.hourly_usd(o["cpu"], o["ram_gib"], o["disk_gib"])
    for p, usd in storage.items():
        row(p)["storage_usd"] += usd
    for a in out.values():
        a["total_usd"] = a["closed_usd"] + a["running_usd"] + a["storage_usd"]
        for k in ("closed_usd", "running_usd", "storage_usd", "total_usd", "sandbox_hours", "burn_usd_per_hour"):
            a[k] = round(a[k], 4)
    return {"providers": out, "as_of": now,
            "total_usd": round(sum(a["total_usd"] for a in out.values()), 4),
            "running_usd": round(sum(a["running_usd"] for a in out.values()), 4),
            "burn_usd_per_hour": round(sum(a["burn_usd_per_hour"] for a in out.values()), 4)}


def summary_all(*, run: Optional[str] = None, models: Optional[dict] = None, now: Optional[float] = None) -> dict:
    """`summary()` over the legacy ledger and every session ledger, added together. This is what a machine-wide spend cap compares against."""
    now = now or time.time()
    parts = [summary(p, run=run, models=models, now=now) for p in all_ledger_paths()]
    return merge_summaries(parts, now)


def merge_summaries(parts: list, now: Optional[float] = None) -> dict:
    out: dict[str, dict] = {}
    keys = ("opened", "running", "closed", "stale", "sandbox_hours", "closed_usd", "running_usd", "storage_usd", "total_usd", "burn_usd_per_hour")
    for s in parts:
        for p, a in s["providers"].items():
            o = out.setdefault(p, {k: 0 for k in keys})
            for k in keys:
                o[k] += a.get(k, 0)
    for a in out.values():
        for k in ("closed_usd", "running_usd", "storage_usd", "total_usd", "sandbox_hours", "burn_usd_per_hour"):
            a[k] = round(a[k], 4)
    return {"providers": out, "as_of": now or time.time(),
            "total_usd": round(sum(a["total_usd"] for a in out.values()), 4),
            "running_usd": round(sum(a["running_usd"] for a in out.values()), 4),
            "burn_usd_per_hour": round(sum(a["burn_usd_per_hour"] for a in out.values()), 4)}


def check_cap(cap_usd: Optional[float], path: Optional[os.PathLike] = None, *, run: Optional[str] = None) -> float:
    """Raise SpendCapExceeded when the ledger total is >= cap_usd; returns the total otherwise."""
    total = summary(path, run=run)["total_usd"]
    if cap_usd and total >= cap_usd:
        raise SpendCapExceeded(f"sandbox spend ${total:.2f} reached the ${cap_usd:.2f} cap")
    return total


def format_summary(s: dict) -> str:
    lines = [f"{'provider':<9}{'opened':>7}{'running':>8}{'sbx-hours':>10}{'closed $':>10}{'running $':>10}"
             f"{'storage $':>10}{'total $':>9}{'burn $/h':>9}"]
    for p, a in sorted(s["providers"].items()):
        lines.append(f"{p:<9}{a['opened']:>7}{a['running']:>8}{a['sandbox_hours']:>10.1f}{a['closed_usd']:>10.2f}"
                     f"{a['running_usd']:>10.2f}{a['storage_usd']:>10.2f}{a['total_usd']:>9.2f}{a['burn_usd_per_hour']:>9.2f}")
    lines.append(f"{'TOTAL':<9}{'':>7}{'':>8}{'':>10}{'':>10}{s['running_usd']:>10.2f}{'':>10}{s['total_usd']:>9.2f}{s['burn_usd_per_hour']:>9.2f}")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="sandbox spend from the cost ledger")
    ap.add_argument("--ledger", default=None); ap.add_argument("--run", default=None)
    ap.add_argument("--watch", type=float, default=0, help="refresh every N seconds")
    ap.add_argument("--cap", type=float, default=0, help="exit 2 when the total reaches this many dollars")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    while True:
        s = summary(a.ledger, run=a.run)
        print(json.dumps(s) if a.json else format_summary(s), flush=True)
        if a.cap and s["total_usd"] >= a.cap:
            return 2
        if not a.watch:
            return 0
        time.sleep(a.watch)
        print()


if __name__ == "__main__":
    sys.exit(main())
