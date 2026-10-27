"""Building harbor Dockerfiles for GKE sandboxes, against a fake gcloud (Cloud Build, Cloud Storage, Artifact Registry) and a
fake registry: one build per context however many sandboxes ask at once, every build minute and every stored GiB-hour in the
ledger, and nothing left behind: no source upload, and no image once it is unused or the run is torn down."""

from __future__ import annotations

import datetime as dt
import json
import threading
import time
from pathlib import Path

import pytest
import yaml

from sandboxes import gke_build
from sandboxes.errors import ImageBuildError
from sandboxes.gke_build import BuildConfig, purge, purge_staging

REG = "us-central1-docker.pkg.dev/p/envs"
GiB = 2 ** 30


def iso(t: float) -> str:
    return dt.datetime.fromtimestamp(t, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class FakeCloud:
    """Cloud Build + the staging bucket + Artifact Registry, as gcloud and the registry API show them."""

    def __init__(self):
        self.calls, self.builds, self.objects, self.images, self.files = [], {}, {}, {}, {}
        self.unreadable = set()
        self.fail_build = False
        self.never_finish = False
        self.build_minutes = 3.2
        self.layer_bytes = 3 * GiB // 2
        self.lock = threading.Lock()

    def gcloud(self, *args, timeout=0):
        with self.lock:
            self.calls.append(list(args))
        a = list(args)
        if a[:2] == ["auth", "print-access-token"]:
            return "tok\n"
        if a[:2] == ["builds", "submit"]:
            conf = yaml.safe_load(Path(a[a.index("--config") + 1]).read_text())
            bid = f"b{len(self.builds) + 1}"
            obj = f"source/{bid}.tgz"
            self.objects[obj] = time.time()
            self.builds[bid] = {"id": bid, "status": "QUEUED", "conf": conf, "polls": 0,
                                "source": {"storageSource": {"bucket": "stage", "object": obj}}}
            return json.dumps({k: v for k, v in self.builds[bid].items() if k != "conf"})
        if a[:2] == ["builds", "describe"]:
            b = self.builds[a[2]]
            b["polls"] += 1
            if b["polls"] >= 2 and not self.never_finish:
                t1 = time.time()
                b.update(status="FAILURE" if self.fail_build else "SUCCESS", startTime=iso(t1 - self.build_minutes * 60),
                         finishTime=iso(t1))
                if not self.fail_build:
                    self.push(b["conf"]["images"][0], t1)
            else:
                b["status"] = "WORKING"
            return json.dumps({k: v for k, v in b.items() if k != "conf"})
        if a[:2] == ["builds", "log"]:
            return "Step 4/7 : RUN pip install nothere\nERROR: No matching distribution found for nothere\n"
        if a[:2] == ["builds", "cancel"]:
            self.builds[a[2]]["status"] = "CANCELLED"
            return ""
        if a[:2] == ["storage", "rm"]:
            for url in a[2:]:
                self.objects.pop(url.split("gs://stage/", 1)[1], None)
            return ""
        if a[:2] == ["storage", "ls"]:
            if not self.objects:
                raise RuntimeError("One or more URLs matched no objects.")
            return json.dumps([{"url": f"gs://stage/{o}", "metadata": {"timeCreated": iso(t)}} for o, t in self.objects.items()])
        if a[:4] == ["artifacts", "docker", "images", "list"]:
            return json.dumps([{"package": f"{REG}/ctx", "tags": [ref.rsplit(":", 1)[1]], "createTime": iso(i["created"]),
                                "metadata": {"imageSizeBytes": str(i["bytes"])}} for ref, i in self.images.items()])
        if a[:3] == ["artifacts", "files", "list"]:
            return json.dumps([{"name": f"projects/p/locations/us-central1/repositories/envs/files/{d}", "sizeBytes": str(f["bytes"]),
                                "createTime": iso(f["created"])} for d, f in self.files.items()])
        if a[:3] == ["artifacts", "files", "delete"]:
            if a[3] not in self.files:
                raise RuntimeError("NOT_FOUND: file")
            del self.files[a[3]]
            return ""
        if a[:4] == ["artifacts", "docker", "images", "delete"]:
            if a[4] not in self.images:
                raise RuntimeError("NOT_FOUND: Requested entity was not found.")
            del self.images[a[4]]
            return ""
        raise AssertionError(f"unexpected gcloud {a}")

    def push(self, ref, t, layer_bytes=None):
        """An image and its blobs: a config, a base layer every image shares, and a layer of its own."""
        n = ref.rsplit(":", 1)[1]
        size = layer_bytes or self.layer_bytes
        blobs = {f"sha256:cfg-{n}": 1000, "sha256:base": size // 2, f"sha256:own-{n}": size - size // 2 - 1000}
        for d, b in blobs.items():
            self.files.setdefault(d, {"bytes": b, "created": t})
        self.images[ref] = {"bytes": size, "created": t, "blobs": list(blobs)}

    def registry(self, url, method="GET"):
        tag = url.rsplit("/manifests/", 1)[1]
        ref = next((r for r in self.images if r.endswith(":" + tag)), None)
        if ref is None:
            return 404, b""
        if ref in self.unreadable:
            return 500, b""
        blobs = self.images[ref].get("blobs") or []
        sizes = {d: self.files.get(d, {}).get("bytes", 0) for d in blobs}
        body = json.dumps({"config": {"digest": blobs[0], "size": 1000} if blobs else {"size": 1000},
                           "layers": [{"digest": d, "size": sizes[d]} for d in blobs[1:]] or
                                     [{"size": self.images[ref]["bytes"] - 1000}]}).encode()
        return 200, (body if method == "GET" else b"")


@pytest.fixture
def cloud(tmp_path, monkeypatch):
    monkeypatch.setenv("SANDBOX_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(tmp_path / "ledger.jsonl"))
    fake = FakeCloud()
    monkeypatch.setattr(gke_build, "gcloud", fake.gcloud)
    monkeypatch.setattr(gke_build, "registry_request", fake.registry)
    monkeypatch.setattr(gke_build.time, "sleep", lambda s: None)
    monkeypatch.setattr(gke_build, "BUILDER", gke_build.Builder())
    fake.ledger = tmp_path / "ledger.jsonl"
    return fake


CFG = BuildConfig(project="p", registry=REG, bucket="stage", region="us-central1", usd_per_minute=0.0156, image_ttl_h=6)


def task(tmp_path, name="t1", body="FROM python:3.12-slim\nRUN pip install requests\n") -> Path:
    env = tmp_path / name / "environment"
    env.mkdir(parents=True)
    (env / "Dockerfile").write_text(body)
    (env / "app.py").write_text(f"print('{name}')\n")                       # each task its own context, its own image
    return env


def charges(cloud, kind):
    if not cloud.ledger.exists():
        return []
    return [json.loads(l) for l in cloud.ledger.read_text().splitlines() if json.loads(l).get("kind") == kind]


def test_identical_contexts_share_one_image(cloud, tmp_path):
    a, b = task(tmp_path, "same"), tmp_path / "copy" / "environment"
    b.mkdir(parents=True)
    for f in a.iterdir():
        (b / f.name).write_bytes(f.read_bytes())
    assert gke_build.BUILDER.ensure(CFG, a) == gke_build.BUILDER.ensure(CFG, b)
    assert sum(c[:2] == ["builds", "submit"] for c in cloud.calls) == 1


def test_a_context_is_built_once_charged_and_its_upload_deleted(cloud, tmp_path):
    env = task(tmp_path)
    ref = gke_build.BUILDER.ensure(CFG, env, run="r1")
    assert ref == f"{REG}/ctx:{gke_build.context_digest(env)}" and ref in cloud.images
    assert gke_build.BUILDER.ensure(CFG, env, run="r1") == ref
    assert sum(c[:2] == ["builds", "submit"] for c in cloud.calls) == 1
    assert [c["usd"] for c in charges(cloud, "image_build")] == [pytest.approx(4 * 0.0156)]       # 3.2 min billed as 4
    assert cloud.objects == {}                                                                  # the source upload is gone
    info = gke_build.known_images()[ref]
    assert info["gib"] == pytest.approx(1.5) and info["runs"] == {"r1"} and info["deleted"] is None


def test_the_build_uses_buildkit_cloud_logging_only_and_the_tasks_own_dockerfile(cloud, tmp_path):
    env = task(tmp_path)
    (env / "docker").mkdir()
    (env / "docker" / "Dockerfile.gpu").write_text("FROM ubuntu\n")
    gke_build.BUILDER.ensure(CFG, env, "docker/Dockerfile.gpu")
    conf = cloud.builds["b1"]["conf"]
    step = conf["steps"][0]
    assert "DOCKER_BUILDKIT=1" in step["env"] and step["args"][step["args"].index("-f") + 1] == "docker/Dockerfile.gpu"
    assert not any(a.startswith("--network") for a in step["args"])          # BuildKit rejects --network=cloudbuild (seen live)
    assert conf["options"]["logging"] == "CLOUD_LOGGING_ONLY" and conf["images"] == [step["args"][step["args"].index("-t") + 1]]


def test_many_sandboxes_of_one_task_at_once_trigger_one_build(cloud, tmp_path):
    env = task(tmp_path)
    refs, errs = [], []

    def go():
        try:
            refs.append(gke_build.BUILDER.ensure(CFG, env, run="r"))
        except Exception as e:  # noqa: BLE001
            errs.append(e)
    ts = [threading.Thread(target=go) for _ in range(16)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errs and len(set(refs)) == 1 and len(refs) == 16
    assert sum(c[:2] == ["builds", "submit"] for c in cloud.calls) == 1


def test_an_image_already_in_the_registry_is_not_rebuilt_or_charged(cloud, tmp_path):
    env = task(tmp_path)
    cloud.push(f"{REG}/ctx:{gke_build.context_digest(env)}", time.time(), GiB)
    gke_build.BUILDER.ensure(CFG, env)
    assert not any(c[:2] == ["builds", "submit"] for c in cloud.calls) and charges(cloud, "image_build") == []


def test_a_failed_build_raises_with_its_log_charges_its_minutes_cleans_up_and_is_retried_next_time(cloud, tmp_path):
    env = task(tmp_path)
    cloud.fail_build = True
    with pytest.raises(ImageBuildError, match="No matching distribution"):
        gke_build.BUILDER.ensure(CFG, env)
    assert cloud.objects == {} and len(charges(cloud, "image_build")) == 1
    assert "(failed)" in charges(cloud, "image_build")[0]["label"]
    cloud.fail_build = False
    gke_build.BUILDER.ensure(CFG, env)                                          # not remembered as built: built now
    assert sum(c[:2] == ["builds", "submit"] for c in cloud.calls) == 2


def test_a_build_that_never_finishes_is_cancelled(cloud, tmp_path, monkeypatch):
    env = task(tmp_path)
    cloud.never_finish = True
    clock = [time.time()]
    monkeypatch.setattr(gke_build.time, "time", lambda: clock[0])
    monkeypatch.setattr(gke_build.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + 600))
    with pytest.raises(ImageBuildError, match="cancelled"):
        gke_build.BUILDER.ensure(BuildConfig(**{**CFG.__dict__, "timeout_s": 600}), env)
    assert cloud.builds["b1"]["status"] == "CANCELLED"


def test_without_a_registry_or_bucket_the_error_says_what_to_configure(cloud, tmp_path):
    with pytest.raises(ImageBuildError, match="gke.registry"):
        gke_build.BUILDER.ensure(BuildConfig(project="p"), task(tmp_path))


def test_a_dockerfile_outside_its_context_is_refused(cloud, tmp_path):
    env = task(tmp_path)
    (tmp_path / "elsewhere").mkdir()
    (tmp_path / "elsewhere" / "Dockerfile").write_text("FROM x\n")
    with pytest.raises(ImageBuildError, match="outside its context"):
        gke_build.BUILDER.ensure(CFG, env, str(tmp_path / "elsewhere" / "Dockerfile"))


# -- nothing left behind -----------------------------------------------------------------------------------------------------
def test_ttl_purge_keeps_what_runs_or_was_used_recently(cloud, tmp_path, monkeypatch):
    envs = {n: task(tmp_path, n) for n in ("idle", "recent", "running")}
    refs = {n: gke_build.BUILDER.ensure(CFG, e) for n, e in envs.items()}
    t0 = time.time()
    now = t0 + 7 * 3600
    monkeypatch.setattr(gke_build.time, "time", lambda: now - 3600)           # "recent" used an hour before the purge
    gke_build.record("used", refs["recent"], run="r")
    monkeypatch.setattr(gke_build.time, "time", lambda: now)
    rep = purge(CFG, in_use={refs["running"]}, now=now)
    assert rep["deleted"] == [refs["idle"]] and set(cloud.images) == {refs["recent"], refs["running"]}
    storage = charges(cloud, "image_storage")
    assert len(storage) == 1 and storage[0]["usd"] == pytest.approx(1.5 * 0.10 * 7 / 730, rel=0.01)   # 1.5 GiB for ~7 h


def test_down_deletes_every_image_and_a_second_purge_charges_nothing_more(cloud, tmp_path):
    for n in ("a", "b", "c"):
        gke_build.BUILDER.ensure(CFG, task(tmp_path, n))
    rep = purge(CFG, all_images=True)
    assert len(rep["deleted"]) == 3 and cloud.images == {}
    n_storage = len(charges(cloud, "image_storage"))
    again = purge(CFG, all_images=True)
    assert (again["deleted"], again["failed"], again["gone"], again["files"]["deleted"]) == ([], [], [], 0)
    assert len(charges(cloud, "image_storage")) == n_storage == 3


def test_an_image_the_cleanup_policy_deleted_is_charged_up_to_when_it_was_found_gone_once(cloud, tmp_path):
    ref = gke_build.BUILDER.ensure(CFG, task(tmp_path))
    cloud.images.clear()                                                    # the repository's cleanup policy ran
    later = time.time() + 2 * 86400
    rep = purge(CFG, now=later)
    assert rep["gone"] == [ref]
    assert charges(cloud, "image_storage")[0]["usd"] == pytest.approx(1.5 * 0.10 * 48 / 730, rel=0.01)
    assert purge(CFG, now=later + 60)["gone"] == []


def test_an_image_we_did_not_record_is_still_deleted_by_down(cloud):
    cloud.push(f"{REG}/ctx:feedfacecafebeef", time.time() - 86400, 2 * GiB)
    rep = purge(CFG, all_images=True)
    assert rep["deleted"] == [f"{REG}/ctx:feedfacecafebeef"] and cloud.images == {}
    assert charges(cloud, "image_storage")[0]["usd"] == pytest.approx(2 * 0.10 * 24 / 730, rel=0.01)


def test_leftover_build_uploads_are_deleted(cloud):
    cloud.objects = {"source/old.tgz": time.time() - 7200, "source/new.tgz": time.time() - 60}
    assert purge_staging(CFG, older_than_s=3600) == 1 and set(cloud.objects) == {"source/new.tgz"}
    assert purge_staging(CFG, older_than_s=0) == 1 and cloud.objects == {}
    assert purge_staging(CFG, older_than_s=0) == 0


# -- layers outlive images (found live) ------------------------------------------------------------------------------------------
def test_down_leaves_no_blob_behind(cloud, tmp_path):
    for n in ("a", "b"):
        gke_build.BUILDER.ensure(CFG, task(tmp_path, n))
    assert len(cloud.files) == 5                                            # 2 configs, 2 own layers, 1 shared base
    rep = purge(CFG, all_images=True)
    assert cloud.images == {} and cloud.files == {} and rep["files"]["deleted"] == 5


def test_a_partial_purge_keeps_the_blobs_remaining_images_use(cloud, tmp_path, monkeypatch):
    old_t = time.time() - 8 * 3600
    keep, drop = f"{REG}/ctx:keepkeepkeepkeep", f"{REG}/ctx:dropdropdropdrop"
    cloud.push(keep, old_t); cloud.push(drop, old_t)
    gke_build.record("used", keep, run="r")                                 # used just now: kept
    rep = purge(CFG, now=time.time())
    assert rep["deleted"] == [drop] and set(cloud.images) == {keep}
    assert set(cloud.files) == {"sha256:cfg-keepkeepkeepkeep", "sha256:own-keepkeepkeepkeep", "sha256:base"}


def test_a_blob_from_a_push_in_progress_is_not_deleted(cloud):
    cloud.files["sha256:uploading"] = {"bytes": 10 ** 8, "created": time.time() - 60}        # no tag points at it yet
    cloud.files["sha256:abandoned"] = {"bytes": 10 ** 8, "created": time.time() - 7200}
    rep = gke_build.purge_files(CFG, ())
    assert set(cloud.files) == {"sha256:uploading"} and rep["deleted"] == 1


def test_an_unreadable_manifest_deletes_no_blob(cloud):
    keep = f"{REG}/ctx:keepkeepkeepkeep"
    cloud.push(keep, time.time() - 7200)
    cloud.files["sha256:orphan"] = {"bytes": 1, "created": time.time() - 7200}
    cloud.unreadable.add(keep)
    with pytest.raises(ImageBuildError, match="not deleting blobs blind"):
        gke_build.purge_files(CFG, [keep])
    assert "sha256:orphan" in cloud.files and "sha256:base" in cloud.files
