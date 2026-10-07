"""Auto-deletion and cost tracking for harbor environments: every provider's environment is built with a FINITE lifetime and the
labels cleanup tooling needs, and a sandbox is neither leaked on a failed start nor counted twice in the cost ledger."""

import asyncio
import json
from pathlib import Path

import pytest

pytest.importorskip("harbor")

from harbor.models.task.config import EnvironmentConfig          # noqa: E402
from harbor.models.trial.paths import TrialPaths                   # noqa: E402

from sandboxes import harbor_adapter                               # noqa: E402
from sandboxes.harbor_adapter import harbor_env_class, harbor_providers, lifetime_kwargs   # noqa: E402
from sandboxes.labels import standard_labels                       # noqa: E402
from sandboxes.lifetime import clamp_lifetime_min                  # noqa: E402

SIZE = {"auto_delete_minutes": 180, "auto_stop_minutes": 60, "extra": {"region": "iad"}}
LABELS = {"harness": "test"}
EXPECTED_S = 180 * 60


def _build(provider, tmp_path, monkeypatch, **extra_kw):
    for k in ("DAYTONA_API_KEY", "E2B_API_KEY", "BL_API_KEY", "BL_WORKSPACE", "FLY_API_KEY"):
        monkeypatch.setenv(k, "test")
    env_dir = tmp_path / "environment"
    env_dir.mkdir(exist_ok=True)
    trial = TrialPaths(trial_dir=tmp_path / "trial")
    trial.mkdir()
    kw = dict(environment_dir=env_dir, environment_name="t", session_id="t__1", trial_paths=trial,
              task_env_config=EnvironmentConfig(docker_image="python:3.12-slim"), **extra_kw)
    kw.update(lifetime_kwargs(provider, SIZE, labels=LABELS))
    if provider == "blaxel":
        from sandboxes.compat import ensure_blaxel_importable
        ensure_blaxel_importable()
    return harbor_env_class(provider)(**kw)


@pytest.mark.parametrize("provider", harbor_providers())
def test_every_harbor_provider_gets_a_finite_lifetime(provider, tmp_path, monkeypatch):
    kw = lifetime_kwargs(provider, SIZE, labels=LABELS)
    if provider == "daytona":
        assert kw["auto_delete_interval_mins"] == 180 and kw["auto_stop_interval_mins"] == 60
    elif provider == "blaxel":
        assert kw["ttl"] == "180m"
    else:
        assert kw["lifetime_s"] == EXPECTED_S and kw["labels"] == LABELS
    # and the environment object actually takes it
    try:
        env = _build(provider, tmp_path, monkeypatch)
    except Exception as e:       # the provider's SDK is not installed in this environment
        pytest.skip(f"{provider}: cannot construct here ({type(e).__name__}: {e})")
    if provider == "daytona":
        assert env._auto_delete_interval == 180 and env._auto_stop_interval == 60
    elif provider == "blaxel":
        assert env._ttl == "180m" and env._labels == LABELS
    else:
        assert env._lifetime_s == EXPECTED_S


def _minutes(provider, kw):
    if provider == "daytona":
        return kw["auto_delete_interval_mins"]
    if provider == "blaxel":
        return int(kw["ttl"].rstrip("m"))
    return kw["lifetime_s"] // 60


def test_unset_lifetime_is_never_unlimited():
    for provider in harbor_providers():
        minutes = _minutes(provider, lifetime_kwargs(provider, {}, labels={}))
        assert minutes == clamp_lifetime_min(None) and 0 < minutes < 24 * 60, provider


def test_e2b_sandbox_is_created_with_the_lifetime_and_standard_labels(tmp_path, monkeypatch):
    pytest.importorskip("e2b")
    from sandboxes import harbor_e2b
    created = {}

    class FakeSandbox:
        @staticmethod
        async def create(**kw):
            created.update(kw)
            return object()

    monkeypatch.setattr(harbor_e2b._harbor_e2b, "AsyncSandbox", FakeSandbox)
    env = _build("e2b", tmp_path, monkeypatch)
    asyncio.run(env._create_sandbox())
    assert created["timeout"] == EXPECTED_S                      # harbor's own value is 86_400
    meta = created["metadata"]
    assert meta["harness"] == "test" and meta["environment_name"] == "t" and meta["session_id"] == "t__1"
    assert meta.items() >= standard_labels().items()             # sandboxes.managed (+ owner/run when set): what the janitor matches


class _FakeEnv:
    LEDGER_NATIVE = False

    def __init__(self, fail_start=False, native=False):
        self.fail_start, self.LEDGER_NATIVE, self.stops, self.session_id = fail_start, native, [], "t__1"

    async def start(self, force_build=False):
        if self.fail_start:
            raise RuntimeError("boom")

    async def stop(self, delete):
        self.stops.append(delete)


def _ledger_records(path: Path):
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()] if path.exists() else []


def test_a_failed_start_deletes_whatever_it_created(monkeypatch, tmp_path):
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(tmp_path / "costs.jsonl"))
    env = _FakeEnv(fail_start=True)
    harbor_adapter.track_env(env, "daytona", {"cpu": 1}, {}, None)
    with pytest.raises(RuntimeError):
        asyncio.run(env.start())
    assert env.stops == [True]


@pytest.mark.parametrize("native, expected_open_close", [(False, 2), (True, 0)])
def test_ledger_entry_is_recorded_once(monkeypatch, tmp_path, native, expected_open_close):
    ledger = tmp_path / "costs.jsonl"
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(ledger))
    env = _FakeEnv(native=native)
    harbor_adapter.track_env(env, "daytona", {"cpu": 1, "memory_mb": 2048, "disk_gb": 8}, {}, None)

    async def run():
        await env.start()
        await env.stop(True)
    asyncio.run(run())
    assert len(_ledger_records(ledger)) == expected_open_close      # a RawSandbox-backed env records its own entry; _track must not add a second


def test_blaxel_sandbox_carries_the_standard_labels(tmp_path, monkeypatch):
    env = _build("blaxel", tmp_path, monkeypatch)
    env._sandbox_name = "t-sbx"
    labels = env._create_sandbox_config("img:latest")["labels"]
    assert labels["created-by"] == "harbor" and labels["harness"] == "test"       # harbor's own labels are kept
    assert labels.items() >= standard_labels().items()                            # sandboxes.managed (+ owner/run when set)


def test_blaxel_image_is_deleted_after_the_sandbox_and_in_use_is_not_an_error(tmp_path, monkeypatch):
    env = _build("blaxel", tmp_path, monkeypatch)
    calls = []

    class Resp:
        def __init__(self, code, text=""):
            self.status_code, self.text = code, text

    class Client:
        def __init__(self, resp):
            self.resp = resp

        async def delete(self, url):
            calls.append(url)
            return self.resp

    from sandboxes import harbor_blaxel
    for resp in (Resp(200), Resp(400, '{"error":"image in use"}'), Resp(404)):
        calls.clear()
        fake = type("F", (), {"get_async_httpx_client": staticmethod(lambda r=resp: Client(r))})
        monkeypatch.setattr(harbor_blaxel._harbor_blaxel, "blaxel_client", fake)
        env._sandbox = None
        asyncio.run(env._delete_image())
        assert calls == [f"/images/sandbox/{env._require_image_name()}"]


def test_blaxel_image_delete_is_delayed_so_a_sibling_starting_now_keeps_its_image(tmp_path, monkeypatch):
    from sandboxes import harbor_blaxel
    env = _build("blaxel", tmp_path, monkeypatch)
    calls = []

    async def fake_delete():
        calls.append(1)

    async def run():
        env._sandbox = object()
        env._delete_image = fake_delete
        orig = harbor_blaxel.BlaxelEnvironment._stop_direct

        async def fake_super_stop(self, delete):
            self._sandbox = None
        monkeypatch.setattr(harbor_blaxel.BlaxelEnvironment, "_stop_direct", fake_super_stop)
        monkeypatch.setattr(harbor_blaxel, "IMAGE_DELETE_DELAY_S", 0.05)
        await env._stop_direct(True)
        assert calls == []                      # not deleted at stop time
        await asyncio.sleep(0.2)
        assert calls == [1]                     # deleted once the delay has passed
        monkeypatch.setattr(harbor_blaxel.BlaxelEnvironment, "_stop_direct", orig)
    asyncio.run(run())


def test_blaxel_stuck_build_times_out_and_cleans_up(tmp_path, monkeypatch):
    from sandboxes import harbor_blaxel
    env = _build("blaxel", tmp_path, monkeypatch)
    deleted = []

    class Resp:
        def __init__(self, code, body=None, headers=None):
            self.status_code, self._b, self.headers, self.text = code, body or {}, headers or {}, ""

        def json(self):
            return self._b

    class Client:
        async def post(self, *a, **k):
            return Resp(200, headers={"x-blaxel-upload-url": "http://x"})

        async def get(self, url, *a, **k):
            return Resp(200, {"metadata": {"status": "BUILDING"}})

        async def delete(self, url):
            deleted.append(url)
            return Resp(200)

    fake = type("F", (), {"get_async_httpx_client": staticmethod(lambda: Client())})
    monkeypatch.setattr(harbor_blaxel._harbor_blaxel, "blaxel_client", fake)
    monkeypatch.setattr(harbor_blaxel, "BUILD_TIMEOUT_S", 0.2)
    monkeypatch.setattr(harbor_blaxel, "BUILD_QUEUE_TIMEOUT_S", 5)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(harbor_blaxel.asyncio, "sleep", lambda s: real_sleep(0))

    async def noop_upload(self, url, content):
        return None
    monkeypatch.setattr(type(env)._build_image_from_dockerfile.__globals__["ImageInstance"], "_upload_zip", noop_upload)
    env._image_built = lambda: real_sleep(0, result=False)
    (env.environment_dir / "Dockerfile").write_text("FROM debian:12\n")
    import pytest
    with pytest.raises(TimeoutError):
        asyncio.run(env._build_image(wait_for_concurrent_build=False))
    assert deleted and deleted[0].startswith("/images/sandbox/harbor-img-")


def test_blaxel_parallel_tries_with_one_session_id_get_distinct_sandbox_names(tmp_path, monkeypatch):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    a = _build("blaxel", tmp_path / "a", monkeypatch)
    b = _build("blaxel", tmp_path / "b", monkeypatch)
    assert a.session_id == b.session_id and a._sandbox_name != b._sandbox_name and len(a._sandbox_name) <= 40
    assert a._require_image_name() == b._require_image_name()          # the task's image is still shared


def test_blaxel_create_is_throttled_and_retried_on_a_gateway_504():
    from sandboxes import harbor_blaxel
    calls = {"n": 0, "live": 0, "peak": 0}

    class E(Exception):
        status_code = 504

    class Real:
        async def create(self, *a, **k):
            calls["n"] += 1
            mine = calls["n"]
            calls["live"] += 1
            calls["peak"] = max(calls["peak"], calls["live"])
            await asyncio.sleep(0.01)
            calls["live"] -= 1
            if mine <= 2:
                raise E("Unexpected HTTP status 504")
            return "sandbox"

    harbor_blaxel._create_sems.clear()
    proxy = harbor_blaxel._ThrottledSandboxInstance(Real())

    async def run():
        orig = harbor_blaxel.asyncio.sleep
        harbor_blaxel.asyncio.sleep = lambda s: orig(0)
        try:
            return await asyncio.gather(*(proxy.create() for _ in range(30)))
        finally:
            harbor_blaxel.asyncio.sleep = orig
    res = asyncio.run(run())
    assert res == ["sandbox"] * 30, res
    assert calls["peak"] <= harbor_blaxel.CREATE_CONCURRENCY, calls
    assert calls["n"] == 32, calls


def test_blaxel_create_retry_adopts_a_sandbox_the_504d_create_already_made():
    from sandboxes import harbor_blaxel
    made = {}

    class E(Exception):
        status_code = 504

    class Real:
        async def create(self, config, **k):
            if config["name"] in made:
                raise RuntimeError(f"Sandbox {config['name']} already exists")
            made[config["name"]] = "sb"
            raise E("Unexpected HTTP status 504")          # it was created, but the reply was a 504

        async def get(self, name):
            if name not in made:
                raise KeyError(name)
            return made[name]

    harbor_blaxel._create_sems.clear()
    proxy = harbor_blaxel._ThrottledSandboxInstance(Real())

    async def run():
        orig = harbor_blaxel.asyncio.sleep
        harbor_blaxel.asyncio.sleep = lambda s: orig(0)
        try:
            return await proxy.create({"name": "harbor-x-1"}, safe=True)
        finally:
            harbor_blaxel.asyncio.sleep = orig
    assert asyncio.run(run()) == "sb"


def test_blaxel_failed_deployment_is_detected_early_renamed_and_retryable(tmp_path, monkeypatch):
    from sandboxes import harbor_blaxel
    from sandboxes.harbor_adapter import is_retryable_start_error
    env = _build("blaxel", tmp_path, monkeypatch)
    old = env._sandbox_name

    class FS:
        async def ls(self, p):
            raise RuntimeError("not ready")
    env._sandbox = type("S", (), {"fs": FS()})()

    async def status():
        return "FAILED"
    env._sandbox_status = status
    import pytest
    with pytest.raises(RuntimeError) as ei:
        asyncio.run(env._wait_until_ready())
    assert "deployment failed" in str(ei.value) and is_retryable_start_error(ei.value)
    assert env._sandbox_name != old and len(env._sandbox_name) <= 40


def test_blaxel_build_waits_for_storage_headroom_and_fails_clearly_when_it_cannot_be_freed(tmp_path, monkeypatch):
    from sandboxes import harbor_blaxel, blaxel_images
    env = _build("blaxel", tmp_path, monkeypatch)
    used = {"gib": 190}

    class Resp:
        status_code = 200

        def json(self):
            return [{"metadata": {"resourceType": "sandbox"}, "spec": {"size": int(used["gib"] * 2**30)}}]

    class Client:
        async def get(self, url, *a, **k):
            return Resp()

    swept = []

    async def fake_purge(older):
        swept.append(older)
        used["gib"] = 60
    monkeypatch.setattr(blaxel_images, "purge_images", fake_purge)
    asyncio.run(env._wait_for_image_headroom(Client()))
    assert swept == [2.0]                                  # over the headroom line: sweep first, then proceed

    async def no_purge(older):
        swept.append(older)
    used["gib"] = 190
    monkeypatch.setattr(blaxel_images, "purge_images", no_purge)
    monkeypatch.setattr(harbor_blaxel, "HEADROOM_WAIT_S", 0)
    import pytest
    with pytest.raises(RuntimeError, match="storage is full"):
        asyncio.run(env._wait_for_image_headroom(Client()))


def test_blaxel_queued_build_is_not_a_timeout_only_actual_building_time_counts(tmp_path, monkeypatch):
    from sandboxes import harbor_blaxel
    env = _build("blaxel", tmp_path, monkeypatch)
    deleted, polls = [], {"n": 0}

    class Resp:
        def __init__(self, code, body=None, headers=None):
            self.status_code, self._b, self.headers, self.text = code, body or {}, headers or {}, ""

        def json(self):
            return self._b

    class Client:
        async def post(self, *a, **k):
            return Resp(200, headers={"x-blaxel-upload-url": "http://x"})

        async def get(self, url, *a, **k):
            polls["n"] += 1
            return Resp(200, {"metadata": {"status": "UPLOADING" if polls["n"] < 40 else "BUILT"}})      # queued far longer than BUILD_TIMEOUT_S

        async def delete(self, url):
            deleted.append(url)
            return Resp(200)

    fake = type("F", (), {"get_async_httpx_client": staticmethod(lambda: Client())})
    monkeypatch.setattr(harbor_blaxel._harbor_blaxel, "blaxel_client", fake)
    monkeypatch.setattr(harbor_blaxel, "BUILD_TIMEOUT_S", 0.05)
    monkeypatch.setattr(harbor_blaxel, "BUILD_QUEUE_TIMEOUT_S", 30)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(harbor_blaxel.asyncio, "sleep", lambda s: real_sleep(0.005))

    async def noop_upload(self, url, content):
        return None
    monkeypatch.setattr(type(env)._build_image_from_dockerfile.__globals__["ImageInstance"], "_upload_zip", noop_upload)
    env._image_built = lambda: real_sleep(0, result=True)
    async def headroom(c):
        return None
    env._wait_for_image_headroom = headroom
    (env.environment_dir / "Dockerfile").write_text("FROM debian:12\n")
    asyncio.run(env._build_image(wait_for_concurrent_build=False))        # returns: queued time did not count against the build timeout
    assert not deleted


def test_blaxel_sandbox_gets_a_disk_backed_scratch_volume_and_the_script_moves_the_working_dirs_onto_it(tmp_path, monkeypatch):
    from sandboxes import harbor_blaxel
    monkeypatch.setattr(harbor_blaxel, "SCRATCH_MB", 16 * 1024)
    env = _build("blaxel", tmp_path, monkeypatch)
    vols = env._create_sandbox_config("img:latest")["volumes"]
    assert len(vols) == 1 and vols[0].type == "ephemeral" and vols[0].size_mb == harbor_blaxel.SCRATCH_MB and vols[0].mount_path == "/scratch"
    script = harbor_blaxel.scratch_script()
    for d in ("/workspace", "/tmp", "/root", "/opt", "/logs"):
        assert f"mount --bind /scratch/{d.strip('/')} {d}" in script
    assert "chmod --reference=/tmp" in script and script.startswith("set -e")           # /tmp keeps its sticky 1777 mode


def test_blaxel_429_is_retried_by_create_and_by_the_api_helper(monkeypatch):
    from sandboxes import harbor_blaxel
    assert harbor_blaxel._transient(RuntimeError("HTTP 429 Too Many Requests"))

    class RateLimitError(Exception):
        pass
    assert harbor_blaxel._transient(RateLimitError("HTTP 429 Too Many Requests"))
    seen = {"n": 0}

    class Resp:
        def __init__(self, code):
            self.status_code = code

    class Client:
        async def get(self, url):
            seen["n"] += 1
            return Resp(429 if seen["n"] < 3 else 200)

    real_sleep = asyncio.sleep
    monkeypatch.setattr(harbor_blaxel.asyncio, "sleep", lambda s: real_sleep(0))
    r = asyncio.run(harbor_blaxel._api(Client(), "get", "/x"))
    assert r.status_code == 200 and seen["n"] == 3


def test_blaxel_scratch_volume_is_off_by_default(tmp_path, monkeypatch):
    env = _build("blaxel", tmp_path, monkeypatch)
    assert "volumes" not in env._create_sandbox_config("img:latest")
