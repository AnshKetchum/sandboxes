"""Fly's exec API rejects a request over ~16 KB with HTTP 200 + exit 0 + "Unhandled rejection: ... PayloadTooLarge" as stdout:
an upload must stay under it, and such a reply must be an error, not a success."""
import asyncio
import base64
import gzip

from sandboxes.backends import fly_raw
from sandboxes.raw import SandboxSpec

LIMIT = 16_000


def test_upload_chunks_fit_the_exec_payload_limit():
    assert len(base64.b64encode(b"x" * fly_raw.UP_CHUNK)) + 200 < LIMIT


def test_a_rejected_exec_is_an_error(monkeypatch):
    class Resp:
        status_code = 200
        def json(self):
            return {"exit_code": 0, "stdout": "Unhandled rejection: Rejection([PayloadTooLarge])", "stderr": ""}

    class Client:
        async def post(self, *a, **k):
            return Resp()

    monkeypatch.setattr(fly_raw, "_client", lambda: Client())
    sbx = fly_raw.FlyRaw(SandboxSpec(image="x"))
    sbx._app, sbx.sandbox_id = "a", "m"
    r = asyncio.run(sbx._exec("true", None, None, 10))
    assert r.rc != 0 and "rejected" in r.stdout


def test_a_volume_is_mounted_at_create_and_deleted_with_the_machine(monkeypatch):
    calls = []

    class Resp:
        def __init__(self, code=200, body=None):
            self.status_code, self._b, self.text = code, body or {}, ""

        def json(self):
            return self._b

    class Client:
        async def post(self, url, json=None, **k):
            calls.append(("POST", url, json))
            if url.endswith("/volumes"):
                return Resp(201, {"id": "vol_1"})
            return Resp(200, {"id": "m1"})

        async def get(self, url, **k):
            return Resp(200)

        async def delete(self, url, **k):
            calls.append(("DELETE", url, None))
            return Resp(200)

    async def app():
        return "a"

    monkeypatch.setattr(fly_raw, "_client", lambda: Client())
    monkeypatch.setattr(fly_raw, "ensure_app", app)
    sbx = fly_raw.FlyRaw(SandboxSpec(image="x", extra={"volume_gb": 40}))
    asyncio.run(sbx._create())
    machine = [c for c in calls if c[1].endswith("/machines")][0][2]
    assert machine["config"]["mounts"] == [{"volume": "vol_1", "path": "/var/lib/docker"}]
    assert [c for c in calls if c[1].endswith("/volumes")][0][2]["size_gb"] == 40
    asyncio.run(sbx._delete())
    assert ("DELETE", "/apps/a/volumes/vol_1", None) in calls and ("DELETE", "/apps/a/machines/m1", None) in calls


def test_context_tag_names_the_exact_build_context(tmp_path):
    (tmp_path / "Dockerfile").write_text("FROM debian:12\n")
    (tmp_path / "setup.sh").write_text("echo 1\n")
    a = fly_raw.context_tag(str(tmp_path / "Dockerfile"), str(tmp_path))
    assert a == fly_raw.context_tag(str(tmp_path / "Dockerfile"), str(tmp_path)) and a.startswith("b-")
    (tmp_path / "setup.sh").write_text("echo 2\n")
    assert fly_raw.context_tag(str(tmp_path / "Dockerfile"), str(tmp_path)) != a


def test_a_big_upload_is_staged_and_downloaded_not_chunked(monkeypatch):
    monkeypatch.setattr(fly_raw, "_stage_sync", lambda data: "https://bucket.example/obj?sig=1")
    cmds = []

    async def fake_exec(self, command, **k):
        cmds.append(command)
        from sandboxes.raw import ExecResult
        return ExecResult(0, "")

    monkeypatch.setattr(fly_raw.FlyRaw, "exec", fake_exec)
    sbx = fly_raw.FlyRaw(SandboxSpec(image="x"))
    asyncio.run(sbx._upload(b"x" * 2_000_000, "/tmp/big.tgz"))
    assert len(cmds) == 1 and "curl" in cmds[0] and "https://bucket.example/obj?sig=1" in cmds[0]


def test_staging_is_off_without_the_env_var(monkeypatch):
    monkeypatch.delenv(fly_raw.STAGE_ENV, raising=False)
    assert fly_raw._stage_sync(b"x" * 2_000_000) is None


def test_a_short_detached_command_takes_one_exec_call_and_a_long_one_keeps_waiting(monkeypatch):
    from sandboxes.raw import ExecResult
    calls = []
    replies = iter([ExecResult(0, "::RUNNING::\n"), ExecResult(0, "::RC::3\nhello\n")])

    async def fake_exec(self, command, cwd, env, timeout):
        calls.append(command)
        return next(replies)

    monkeypatch.setattr(fly_raw.FlyRaw, "_exec", fake_exec)
    sbx = fly_raw.FlyRaw(SandboxSpec(image="x"))
    r = asyncio.run(sbx._exec_detached("echo hello; exit 3", None, {"A": "1"}, 600))
    assert (r.rc, r.stdout) == (3, "hello\n")
    assert len(calls) == 2 and "base64 -d" in calls[0] and "base64 -d" not in calls[1]       # the script is sent once; later calls only wait

    calls.clear()
    replies2 = iter([ExecResult(0, "::RC::0\nok\n")])
    monkeypatch.setattr(fly_raw.FlyRaw, "_exec", lambda self, c, cwd, env, t: _async(next(replies2)))
    assert asyncio.run(sbx._exec_detached("true", None, None, 600)).stdout == "ok\n"


async def _async(v):
    return v


def test_an_api_timeout_on_the_waiting_call_keeps_waiting_and_a_lost_script_is_an_error(monkeypatch):
    from sandboxes.raw import ExecResult
    replies = iter([ExecResult(-1, "Execution timed out after 50 seconds."), ExecResult(0, "::RUNNING::\n"), ExecResult(0, "::RC::0\nfine\n")])
    monkeypatch.setattr(fly_raw.FlyRaw, "_exec", lambda self, c, cwd, env, t: _async(next(replies)))
    sbx = fly_raw.FlyRaw(SandboxSpec(image="x"))
    assert asyncio.run(sbx._exec_detached("sleep 80", None, None, 600)).stdout == "fine\n"
    monkeypatch.setattr(fly_raw.FlyRaw, "_exec", lambda self, c, cwd, env, t: _async(ExecResult(0, "::LOST::\n")))
    r = asyncio.run(sbx._exec_detached("true", None, None, 600))
    assert r.rc == -1 and "could not run" in r.stdout
