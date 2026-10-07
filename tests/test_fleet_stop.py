"""fleet.stop returns what it actually deleted, reports each sandbox as it goes, and only closes the ledger for the deleted ones."""

import asyncio

from sandboxes import fleet
from sandboxes.raw import LiveSandbox


def _sbs():
    return [LiveSandbox("daytona", "ok-1", None, "started", {"euphoria.managed": "true"}),
            LiveSandbox("daytona", "bad-1", None, "started", {"euphoria.managed": "true"}),
            LiveSandbox("fly", "ok-2", None, "started", {"euphoria.managed": "true"})]


def _patch(monkeypatch, found):
    gone, killed = [], []

    async def fake_ls(*a, **k):
        return list(found)

    class FakeRaw:
        @classmethod
        async def kill(cls, sid):
            if sid.startswith("bad"):
                raise RuntimeError("state change in progress")
            killed.append(sid)

    async def fake_reconcile(path=None, live_now=None, now=None, gone=()):
        gone_ids.update(gone)
        return len(gone)

    gone_ids = set()
    monkeypatch.setattr(fleet, "ls", fake_ls)
    monkeypatch.setattr(fleet, "raw_class", lambda provider: FakeRaw)
    monkeypatch.setattr(fleet, "areconcile", fake_reconcile)
    return killed, gone_ids


def test_returns_only_what_was_deleted_and_reconciles_only_those(monkeypatch):
    killed, gone_ids = _patch(monkeypatch, _sbs())
    seen = []
    out = asyncio.run(fleet.stop(owner="euphoria", on_result=lambda sb, err: seen.append((sb.id, err is None))))
    assert [sb.id for sb in out] == ["ok-1", "ok-2"]
    assert gone_ids == {"ok-1", "ok-2"}                                    # a failed kill is NOT closed in the cost ledger
    assert seen == [("ok-1", True), ("bad-1", False), ("ok-2", True)]      # every one is reported, in order, with its outcome


def test_dry_run_reports_and_returns_everything_and_deletes_nothing(monkeypatch):
    killed, gone_ids = _patch(monkeypatch, _sbs())
    seen = []
    out = asyncio.run(fleet.stop(owner="euphoria", dry_run=True, on_result=lambda sb, err: seen.append(sb.id)))
    assert [sb.id for sb in out] == ["ok-1", "bad-1", "ok-2"] and seen == ["ok-1", "bad-1", "ok-2"]
    assert killed == [] and gone_ids == set()
