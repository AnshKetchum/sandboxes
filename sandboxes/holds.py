"""Holds: pause or stop one job at the sandbox layer, whatever runs it and on whichever provider.

A hold is a set of labels. While it stands, no new sandbox whose labels include all of them opens on this machine: the check is
in RawSandbox.start, beside the spend caps, so it applies to every provider and every caller (greenhouse, euphoria, ...).
Sandboxes already open keep running; `sandboxes stop` with the same labels ends them. Callers label their sandboxes (greenhouse:
greenhouse.run and greenhouse.task; every sandbox: sandboxes.owner and sandboxes.run), so one unit of a run can be held while
the run's other units go on:

    sandboxes hold    --label greenhouse.run=r-1 --label greenhouse.task=t-0070 --reason "tail unit"   # no new sandboxes
    sandboxes stop    --label greenhouse.run=r-1 --label greenhouse.task=t-0070                        # end the open ones
    sandboxes release --label greenhouse.run=r-1 --label greenhouse.task=t-0070                        # it may open again
    sandboxes holds                                                                                   # what is held

A held start raises SandboxHeld (a SandboxError, not a CapacityError: the router does not try another provider for it).
Holds live in <cache dir>/holds.json, per machine like the spend caps.
"""

from __future__ import annotations

import fcntl
import json
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from .errors import SandboxError

HOLDS_FILE = "holds.json"


class SandboxHeld(SandboxError):
    """A sandbox was not opened because a hold covers its labels (`sandboxes holds` lists them, `sandboxes release` lifts one)."""


def _path() -> Path:
    from .cost import cache_dir
    return cache_dir() / HOLDS_FILE


@contextmanager
def _locked():
    p = _path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p.with_name(p.name + ".lock"), "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield p


def parse_labels(pairs) -> dict:
    """["k=v", ...] -> {"k": "v"}; a pair without "=" is an error."""
    out = {}
    for kv in pairs or []:
        k, sep, v = str(kv).partition("=")
        if not sep or not k:
            raise ValueError(f"a label is key=value, got {kv!r}")
        out[k.strip()] = v.strip()
    return out


def matches(labels: dict, want: dict) -> bool:
    """True when `labels` carries every key=value of `want` (values compared as strings)."""
    labels = labels or {}
    return all(str(labels.get(k)) == str(v) for k, v in (want or {}).items())


def load() -> list:
    """The holds standing now: [{"labels": {...}, "reason": str, "t": epoch}]. A missing or unreadable file means none."""
    try:
        holds = json.loads(_path().read_text())
    except (OSError, ValueError):
        return []
    return [h for h in holds if isinstance(h, dict) and isinstance(h.get("labels"), dict) and h["labels"]] \
        if isinstance(holds, list) else []


def hold(labels: dict, reason: str = "") -> dict:
    """Hold every future sandbox whose labels include `labels`. Holding the same labels again updates the reason."""
    if not labels:
        raise ValueError("a hold needs at least one label (holding everything is `sandboxes guard`'s job)")
    with _locked() as p:
        holds = [h for h in load() if h["labels"] != labels]
        entry = {"labels": dict(labels), "reason": reason, "t": time.time()}
        holds.append(entry)
        p.write_text(json.dumps(holds, indent=1))
    return entry


def release(labels: dict) -> int:
    """Lift the hold with exactly these labels. Returns how many were lifted (0 or 1)."""
    with _locked() as p:
        holds = load()
        keep = [h for h in holds if h["labels"] != labels]
        p.write_text(json.dumps(keep, indent=1))
    return len(holds) - len(keep)


def held_by(labels: dict) -> Optional[dict]:
    """The first hold covering a sandbox with these labels, or None."""
    return next((h for h in load() if matches(labels, h["labels"])), None)


def check(labels: dict) -> None:
    """Raise SandboxHeld when a hold covers a sandbox with these labels (RawSandbox.start calls this before creating one)."""
    h = held_by(labels)
    if h is not None:
        what = ", ".join(f"{k}={v}" for k, v in h["labels"].items())
        raise SandboxHeld(f"sandbox not opened: held ({what})" + (f": {h['reason']}" if h.get("reason") else "")
                          + " — `sandboxes release " + " ".join(f"--label {k}={v}" for k, v in h["labels"].items()) + "` lifts it")
