"""Sandbox sessions: each client of the library (euphoria, greenhouse, pr_task_synthesis ...) opens a session and gets a dedicated cost ledger.

    from sandboxes.session import open_session

    with open_session("euphoria", name="v3-b07", tags={"config": "pr-task-synthesis-v3-b07.yaml"}) as s:
        ...                                   # every sandbox this process (and the child processes it spawns) opens is recorded in s's ledger
        print(s.cost()["total_usd"])          # sandbox dollars of this session alone

    list_sessions(client="euphoria")          # [Session, ...] newest first
    get_session("v3-b07-20261008T154701-ab12").cost()

A session is a directory, ~/.cache/sandboxes/sessions/<client>/<session id>/, holding

    costs.jsonl    the ledger (same format as the legacy shared ledger, see cost.py), written only by this session
    session.json   {"client", "id", "name", "started", "ended", "pid", "host", "tags"}

`open_session` exports SANDBOX_SESSION_DIR (and SANDBOX_RUN_ID = name) into the environment, so spawned worker processes inherit the session and
write to the same ledger. Spend caps are unchanged: they add up the legacy ledger and every session ledger (cost.summary_all).

Also from the shell, for a process you do not control:  SANDBOX_SESSION_CLIENT=euphoria SANDBOX_SESSION_NAME=v3-b07 python ...  (see `ensure_env_session`).

    python -m sandboxes sessions                      # every session with its cost
    python -m sandboxes sessions --client euphoria
    python -m sandboxes sessions cost <session id> [--json]
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import secrets
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .cost import RUN_ENV, SESSION_ENV, CostLedger, merge_summaries, sessions_dir, summary

CLIENT_ENV = "SANDBOX_SESSION_CLIENT"
NAME_ENV = "SANDBOX_SESSION_NAME"
_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _slug(s: str) -> str:
    return _SAFE.sub("-", s).strip("-") or "x"


@dataclass
class Session:
    client: str
    id: str
    name: str = ""
    dir: Path = field(default_factory=Path)
    started: float = 0.0
    ended: Optional[float] = None
    tags: dict = field(default_factory=dict)

    # -- paths ----------------------------------------------------------------------------------
    @property
    def ledger_path(self) -> Path:
        return self.dir / "costs.jsonl"

    @property
    def meta_path(self) -> Path:
        return self.dir / "session.json"

    # -- reading --------------------------------------------------------------------------------
    def cost(self, *, now: Optional[float] = None) -> dict:
        """`cost.summary()` of this session's ledger alone: per-provider opened/running/closed, sandbox_hours, closed_usd, running_usd, total_usd."""
        return summary(self.ledger_path, now=now)

    def total_usd(self) -> float:
        return self.cost()["total_usd"]

    def hours(self) -> float:
        return sum(a["sandbox_hours"] for a in self.cost()["providers"].values())

    # -- lifecycle ------------------------------------------------------------------------------
    def close(self) -> None:
        """Mark the session ended. Does not touch running sandboxes (use fleet / the janitor for that)."""
        self.ended = time.time()
        self._write_meta()

    def __enter__(self) -> "Session":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
        if os.environ.get(SESSION_ENV) == str(self.dir):
            os.environ.pop(SESSION_ENV, None)
            if os.environ.get(RUN_ENV) == (self.name or self.id):
                os.environ.pop(RUN_ENV, None)

    def _write_meta(self) -> None:
        tmp = self.meta_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"client": self.client, "id": self.id, "name": self.name, "started": self.started, "ended": self.ended,
                                   "pid": os.getpid(), "host": socket.gethostname(), "tags": self.tags}, indent=1))
        os.replace(tmp, self.meta_path)

    def to_dict(self) -> dict:
        return {"client": self.client, "id": self.id, "name": self.name, "dir": str(self.dir), "started": self.started, "ended": self.ended, "tags": self.tags}


def open_session(client: str, name: str = "", *, tags: Optional[dict] = None, activate: bool = True) -> Session:
    """Create a session directory for `client` and (by default) make it the active session of this process and its children."""
    client_s = _slug(client)
    stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    sid = f"{_slug(name) + '-' if name else ''}{stamp}-{secrets.token_hex(2)}"
    d = sessions_dir() / client_s / sid
    d.mkdir(parents=True, exist_ok=True)
    s = Session(client=client_s, id=sid, name=name, dir=d, started=time.time(), tags=dict(tags or {}))
    s._write_meta()
    s.ledger_path.touch()
    if activate:
        os.environ[SESSION_ENV] = str(d)
        os.environ[RUN_ENV] = name or sid            # the existing per-run spend cap / report tag keeps working
    return s


@contextlib.contextmanager
def use_session(ref):
    """Make `ref` the active session for the duration of the block, then restore whatever was active before. `ref` is a Session, a session id/name,
    or a session directory. This is how a client threads its session down into library code (e.g. euphoria's `session=` parameters): the
    environment variables it sets are inherited by the worker processes started inside the block."""
    if isinstance(ref, Session):
        s = ref
    elif os.path.isdir(str(ref)):
        s = _load(Path(str(ref)))
    else:
        s = get_session(str(ref))
    prev = (os.environ.get(SESSION_ENV), os.environ.get(RUN_ENV))
    os.environ[SESSION_ENV] = str(s.dir)
    os.environ[RUN_ENV] = s.name or s.id
    try:
        yield s
    finally:
        for k, v in zip((SESSION_ENV, RUN_ENV), prev):
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def current_session() -> Optional[Session]:
    d = os.environ.get(SESSION_ENV)
    return _load(Path(d)) if d else None


def ensure_env_session() -> Optional[Session]:
    """If SANDBOX_SESSION_CLIENT is set and no session is active, open one (so a launcher script can opt a process in without code changes)."""
    if os.environ.get(SESSION_ENV) or not os.environ.get(CLIENT_ENV):
        return current_session()
    return open_session(os.environ[CLIENT_ENV], os.environ.get(NAME_ENV, ""))


def _load(d: Path) -> Optional[Session]:
    try:
        m = json.loads((d / "session.json").read_text())
    except (OSError, ValueError):
        return Session(client=d.parent.name, id=d.name, dir=d, started=d.stat().st_mtime) if d.is_dir() else None
    return Session(client=m.get("client", d.parent.name), id=m.get("id", d.name), name=m.get("name", ""), dir=d, started=m.get("started", 0.0),
                   ended=m.get("ended"), tags=m.get("tags") or {})


def list_sessions(client: Optional[str] = None) -> list[Session]:
    """Every session on this machine, newest first."""
    root = sessions_dir()
    if not root.is_dir():
        return []
    out = []
    for cdir in sorted(root.iterdir()):
        if not cdir.is_dir() or (client and cdir.name != _slug(client)):
            continue
        for d in cdir.iterdir():
            s = _load(d) if d.is_dir() else None
            if s:
                out.append(s)
    return sorted(out, key=lambda s: s.started, reverse=True)


def get_session(ref: str, client: Optional[str] = None) -> Session:
    """By exact session id, else by name (the newest session with that name), else by a unique id prefix."""
    cands = list_sessions(client)
    for s in cands:
        if s.id == ref:
            return s
    by_name = [s for s in cands if s.name == ref]
    if by_name:
        return by_name[0]
    pre = [s for s in cands if s.id.startswith(ref)]
    if len(pre) == 1:
        return pre[0]
    raise KeyError(f"no unique session matches {ref!r}")


def sessions_cost(sessions: list[Session]) -> dict:
    """Added-together `cost()` of several sessions (e.g. every session of one euphoria run)."""
    now = time.time()
    return merge_summaries([s.cost(now=now) for s in sessions], now)


def format_sessions(rows: list[Session]) -> str:
    lines = [f"{'client':<18}{'session':<46}{'started (UTC)':<18}{'hours':>8}{'total $':>10}  providers"]
    for s in rows:
        c = s.cost()
        lines.append(f"{s.client:<18}{s.id:<46}{time.strftime('%m-%d %H:%M', time.gmtime(s.started)):<18}"
                     f"{sum(a['sandbox_hours'] for a in c['providers'].values()):>8.1f}{c['total_usd']:>10.2f}  "
                     + ", ".join(f"{p} ${a['total_usd']:.0f}" for p, a in sorted(c["providers"].items())))
    return "\n".join(lines)


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="sandboxes sessions", description="sandbox sessions and their costs")
    ap.add_argument("what", nargs="*", help="empty = list; 'cost REF' = one session's per-provider cost")
    ap.add_argument("--client"); ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    if a.what[:1] == ["cost"]:
        if len(a.what) < 2:
            ap.error("cost needs a session id or name")
        s = get_session(a.what[1], a.client); c = s.cost()
        if a.json:
            print(json.dumps({"session": s.to_dict(), "cost": c}))
        else:
            from .cost import format_summary
            print(f"{s.client}/{s.id}  started {time.strftime('%F %T', time.gmtime(s.started))}\n" + format_summary(c))
        return 0
    rows = list_sessions(a.client)
    if a.json:
        print(json.dumps([{**s.to_dict(), "cost": s.cost()} for s in rows]))
    else:
        print(format_sessions(rows) if rows else "no sessions yet")
    return 0
