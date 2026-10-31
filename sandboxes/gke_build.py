"""Building a harbor task's Dockerfile into the image its GKE sandbox runs, and leaving nothing billable behind.

GKE runs registry images. A sandbox opened from a Dockerfile and its context (a harbor task's environment/) is built by Cloud
Build into Artifact Registry as `<registry>/ctx:<digest>`, the digest being the 16-hex content hash euphoria and greenhouse tag
their images with: built once, then every sandbox of that context runs the same image. Each build also pushes the tag
`ctx:cache-<hash of the Dockerfile>` with BuildKit's inline cache and builds from the one before it, so a context whose
Dockerfile is unchanged (an evolving task's package edits) rebuilds only from the layer its change touches. The rest is about
cost:

  - build minutes are charged to the ledger when the build ends (kind "image_build")
  - the source upload is deleted when its build ends; the staging bucket's lifecycle rule deletes any leftover after a day
  - every image pushed is recorded (~/.cache/sandboxes/gke_images.jsonl) and deleted: by `gke_ops down` / teardown, by the
    watchdog once no pod runs it and it is unused for `image_ttl_h`, and by the repository's cleanup policy (server side,
    `cleanup_days`) whatever happens to this machine
  - an image's storage is charged for its whole life when it is deleted, or when the watchdog finds it gone
  - `gke_ops residue` lists whatever in the project still costs money, ours or not

Configuration: configs/gke.yaml `gke.registry` and `gke.build` (bucket, machine_type, usd_per_minute, timeout_s,
max_concurrent, image_ttl_h, cleanup_days, service_account).
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

from .buildctx import context_digest, resolve_dockerfile
from .cost import cache_dir
from .errors import ImageBuildError

IMAGES_FILE = "gke_images.jsonl"
IMAGE_NAME = "ctx"
GIB = 2 ** 30


@dataclass
class BuildConfig:
    project: str = ""
    registry: str = ""                    # <region>-docker.pkg.dev/<project>/<repo>
    bucket: str = ""                      # Cloud Build source staging (lifecycle: delete after 1 day)
    region: str = ""
    machine_type: str = "E2_HIGHCPU_8"
    usd_per_minute: float = 0.016         # Cloud Build, e2-highcpu-8 (configs/gke.yaml says where it comes from)
    storage_gib_month: float = 0.10       # Artifact Registry
    timeout_s: int = 1800
    max_concurrent: int = 20              # under Cloud Build's default 30 concurrent builds per region
    image_ttl_h: float = 6.0
    cleanup_days: int = 2
    service_account: str = ""
    layer_cache: bool = True              # build from the last image of the same Dockerfile (ctx:cache-<hash>)

    @classmethod
    def load(cls, path: Optional[os.PathLike] = None) -> "BuildConfig":
        from .gke_meter import DEFAULT_CONFIG
        p = Path(path or os.environ.get("SANDBOX_GKE_CONFIG") or DEFAULT_CONFIG)
        raw = (yaml.safe_load(p.read_text()) or {}) if p.exists() else {}
        g = raw.get("gke") or {}
        c = cls(**{k: v for k, v in (g.get("build") or {}).items() if k in cls.__dataclass_fields__})
        c.project = os.environ.get("GKE_PROJECT") or g.get("project") or c.project
        c.registry = os.environ.get("GKE_REGISTRY") or g.get("registry") or c.registry
        if not c.region and c.registry:
            c.region = c.registry.split("-docker.pkg.dev")[0]
        return c

    def check(self) -> None:
        if not (self.project and self.registry and self.bucket and self.region):
            raise ImageBuildError("gke: building a Dockerfile needs gke.project, gke.registry and gke.build.bucket in "
                                  "configs/gke.yaml (scripts/gke/setup.sh creates them)")


# -- seams (tests replace these) ---------------------------------------------------------------------------------------------
def gcloud(*args: str, timeout: float = 600) -> str:
    p = subprocess.run(["gcloud", *args], capture_output=True, text=True, timeout=timeout)
    if p.returncode != 0:
        raise RuntimeError(f"gcloud {' '.join(args[:3])} failed ({p.returncode}): {p.stderr.strip()[-800:]}")
    return p.stdout


_token = {"value": "", "t": 0.0}


MANIFEST_TYPES = ", ".join(["application/vnd.oci.image.index.v1+json", "application/vnd.oci.image.manifest.v1+json",
                            "application/vnd.docker.distribution.manifest.list.v2+json",
                            "application/vnd.docker.distribution.manifest.v2+json"])


def registry_request(url: str, method: str = "GET") -> tuple:
    """(HTTP status, body) of a registry request, with this machine's gcloud credentials. gcloud hands out its cached
    token, which may be minutes from expiry, so a 401 gets a fresh token and one retry (live: 401s for ~25 min of every
    30-min cache window once the login's first token aged out, failing every sandbox start in that window)."""
    for attempt in range(2):
        if attempt or time.time() - _token["t"] > 1800:
            _token.update(value=gcloud("auth", "print-access-token").strip(), t=time.time())
        req = urllib.request.Request(url, method=method,
                                     headers={"Authorization": f"Bearer {_token['value']}", "Accept": MANIFEST_TYPES})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, (r.read() if method == "GET" else b"")
        except urllib.error.HTTPError as e:
            if e.code != 401 or attempt:
                return e.code, b""
    return 401, b""


# -- the record of what we pushed --------------------------------------------------------------------------------------------
def _images_path() -> Path:
    return cache_dir() / IMAGES_FILE


def record(ev: str, ref: str, **kw) -> None:
    p = _images_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p.with_name(p.name + ".lock"), "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with open(p, "a") as f:
            f.write(json.dumps({"t": time.time(), "ev": ev, "ref": ref, **kw}) + "\n")


def known_images() -> dict:
    """ref -> {"built": t, "gib": size, "last_used": t, "runs": set, "deleted": t or None} from the record."""
    out: dict = {}
    try:
        lines = _images_path().read_text().splitlines()
    except OSError:
        return out
    for line in lines:
        try:
            r = json.loads(line)
        except ValueError:
            continue
        ref = r.get("ref")
        if not ref:
            continue
        e = out.setdefault(ref, {"built": None, "gib": 0.0, "last_used": None, "runs": set(), "deleted": None})
        if r["ev"] == "built":
            e.update(built=r["t"], gib=float(r.get("gib") or 0.0), deleted=None)
        elif r["ev"] == "used":
            e["last_used"] = max(e["last_used"] or 0.0, r["t"])
            e["runs"].add(r.get("run", ""))
        elif r["ev"] == "deleted":
            e["deleted"] = r["t"]
    return out


# -- build -------------------------------------------------------------------------------------------------------------------
def image_ref(cfg: BuildConfig, context_dir, dockerfile=None) -> str:
    return f"{cfg.registry}/{IMAGE_NAME}:{context_digest(context_dir, dockerfile)}"


def manifest_url(ref: str) -> str:
    host, path = ref.split("/", 1)
    repo, tag = path.rsplit(":", 1)
    return f"https://{host}/v2/{repo}/manifests/{tag}"


def image_exists(ref: str) -> bool:
    status, _ = registry_request(manifest_url(ref), "HEAD")
    if status == 200:
        return True
    if status == 404:
        return False
    raise ImageBuildError(f"gke: registry answered {status} for {ref}")


def cache_ref(cfg: BuildConfig, dockerfile: Path) -> str:
    """The tag every build from this Dockerfile's text pushes and builds from: the layers before its context changes."""
    return f"{cfg.registry}/{IMAGE_NAME}:cache-{hashlib.sha256(dockerfile.read_bytes()).hexdigest()[:16]}"


def build_config_yaml(cfg: BuildConfig, ref: str, dockerfile_rel: str, cache: str = "") -> str:
    # BUILDKIT_SYNTAX: the builder's own Dockerfile frontend predates RUN heredocs (`cat > f <<'EOF'`), which task
    # Dockerfiles use and Modal/Daytona/docker build; the current frontend parses them (live: "unknown instruction").
    args = ["build", "--build-arg", "BUILDKIT_SYNTAX=docker/dockerfile:1", "-t", ref, "-f", dockerfile_rel]
    if cache:       # a missing cache image is only a cache miss: BuildKit builds from scratch
        args += ["--build-arg", "BUILDKIT_INLINE_CACHE=1", "--cache-from", cache, "-t", cache]
    conf = {"steps": [{"name": "gcr.io/cloud-builders/docker", "env": ["DOCKER_BUILDKIT=1"],
                       "args": args + ["."]}],     # BuildKit refuses --network=cloudbuild
            "images": [ref] + ([cache] if cache else []), "timeout": f"{int(cfg.timeout_s)}s",
            "options": {"machineType": cfg.machine_type, "logging": "CLOUD_LOGGING_ONLY"}}
    if cfg.service_account:
        conf["serviceAccount"] = f"projects/{cfg.project}/serviceAccounts/{cfg.service_account}"
    return yaml.safe_dump(conf, sort_keys=False)


def _epoch(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    import datetime as dt
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def run_build(cfg: BuildConfig, context_dir, dockerfile, ref: str, poll_s: float = 5.0) -> dict:
    """Submit, wait, delete the source upload. Returns {"minutes", "gib", "id"}; raises ImageBuildError with the log's tail."""
    ctx = Path(context_dir).resolve()
    df = resolve_dockerfile(ctx, dockerfile).resolve()
    try:
        df_rel = df.relative_to(ctx).as_posix()
    except ValueError:
        raise ImageBuildError(f"gke: the Dockerfile {df} is outside its context {ctx}")
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        f.write(build_config_yaml(cfg, ref, df_rel, cache_ref(cfg, df) if cfg.layer_cache else ""))
        conf_path = f.name
    try:
        out = gcloud("builds", "submit", str(ctx), "--config", conf_path, "--async", "--format=json", "--project", cfg.project,
                     "--region", cfg.region, "--gcs-source-staging-dir", f"gs://{cfg.bucket}/source", timeout=600)
    finally:
        os.unlink(conf_path)
    b = json.loads(out)
    bid = b["id"]
    deadline = time.time() + cfg.timeout_s + 600
    while b.get("status") in ("QUEUED", "PENDING", "WORKING", None):
        if time.time() > deadline:
            gcloud("builds", "cancel", bid, "--project", cfg.project, "--region", cfg.region)
            raise ImageBuildError(f"gke: build {bid} of {ctx} did not finish in time (cancelled)")
        time.sleep(poll_s)
        b = json.loads(gcloud("builds", "describe", bid, "--format=json", "--project", cfg.project, "--region", cfg.region))
    src = ((b.get("source") or {}).get("storageSource") or {})
    if src.get("bucket") and src.get("object"):
        try:
            gcloud("storage", "rm", f"gs://{src['bucket']}/{src['object']}")
        except RuntimeError as e:          # the bucket's lifecycle rule gets it within a day
            print(f"[sandboxes] gke_build: could not delete the build source ({e}); the lifecycle rule will", file=sys.stderr)
    start, end = _epoch(b.get("startTime")), _epoch(b.get("finishTime"))
    minutes = math.ceil(max(0.0, (end or time.time()) - (start or end or time.time())) / 60.0) if start else 0
    if b.get("status") != "SUCCESS":
        tail = ""
        try:
            tail = gcloud("builds", "log", bid, "--project", cfg.project, "--region", cfg.region)[-1500:]
        except RuntimeError:
            pass
        raise _build_error(ctx, b, tail, minutes)
    return {"id": bid, "minutes": minutes}


def _build_error(ctx, b: dict, tail: str, minutes: int) -> ImageBuildError:
    e = ImageBuildError(f"gke: build of {ctx} {b.get('status')}: {b.get('statusDetail', '')}\n{tail}")
    e.minutes = minutes
    return e


def image_gib(ref: str) -> float:
    """What the pushed image stores: its config and layers (compressed), from its manifest; an index sums its manifests."""
    def size(url: str, depth: int = 0) -> int:
        status, body = registry_request(url)
        if status != 200 or not body:
            return 0
        m = json.loads(body)
        if m.get("manifests") and depth == 0:
            base = url.rsplit("/", 1)[0]
            return sum(size(f"{base}/{d['digest']}", 1) for d in m["manifests"])
        return int((m.get("config") or {}).get("size") or 0) + sum(int(l.get("size") or 0) for l in m.get("layers") or [])
    try:
        return size(manifest_url(ref)) / GIB
    except (ValueError, KeyError, OSError):
        return 0.0


class Builder:
    """Builds once per context across this process (threads wait for the build in flight) and this machine (a lock per
    digest); at most `max_concurrent` builds at a time."""

    def __init__(self):
        self.lock = threading.Lock()
        self.inflight: dict = {}
        self.known: set = set()
        self.slots: Optional[threading.Semaphore] = None

    def ensure(self, cfg: BuildConfig, context_dir, dockerfile=None, *, run: str = "", ledger=None) -> str:
        cfg.check()
        ref = image_ref(cfg, context_dir, dockerfile)
        with self.lock:
            if self.slots is None:
                self.slots = threading.Semaphore(max(1, int(cfg.max_concurrent)))
            ev = self.inflight.get(ref)
            mine = ev is None and ref not in self.known
            if mine:
                ev = self.inflight[ref] = threading.Event()
        if not mine:
            if ev is not None:
                ev.wait(cfg.timeout_s + 900)
                if ref not in self.known:
                    raise ImageBuildError(f"gke: the build of {ref} in this process failed")
            record("used", ref, run=run)
            return ref
        try:
            self._build_once(cfg, context_dir, dockerfile, ref, run, ledger)
            with self.lock:
                self.known.add(ref)
        finally:
            with self.lock:
                self.inflight.pop(ref, None)
            ev.set()
        record("used", ref, run=run)
        return ref

    def _build_once(self, cfg, context_dir, dockerfile, ref, run, ledger) -> None:
        digest = ref.rsplit(":", 1)[1]
        cache_dir().mkdir(parents=True, exist_ok=True)
        with open(cache_dir() / f"gke_build_{digest}.lock", "a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)                # another process building it: wait, then find it built
            if image_exists(ref):
                return
            with self.slots:
                try:
                    res = run_build(cfg, context_dir, dockerfile, ref)
                except ImageBuildError as e:
                    _charge_build(ledger, cfg, getattr(e, "minutes", 0), ref, run, failed=True)
                    raise
            _charge_build(ledger, cfg, res["minutes"], ref, run)
            record("built", ref, gib=image_gib(ref), run=run, build=res["id"])


def _charge_build(ledger, cfg: BuildConfig, minutes: float, ref: str, run: str, failed: bool = False) -> None:
    from .cost import get_ledger
    led = ledger or get_ledger()
    led.charge("gke", minutes * cfg.usd_per_minute, kind="image_build", label=ref + (" (failed)" if failed else ""))


BUILDER = Builder()


# -- removal -----------------------------------------------------------------------------------------------------------------
def list_images(cfg: BuildConfig) -> list:
    """[{"ref", "gib", "created"}] for every tagged image of ours in the repository."""
    out = []
    for d in json.loads(gcloud("artifacts", "docker", "images", "list", f"{cfg.registry}/{IMAGE_NAME}", "--include-tags",
                               "--format=json") or "[]"):
        tags = d.get("tags") or []
        for tag in (tags.split(",") if isinstance(tags, str) else tags):
            if tag:
                out.append({"ref": f"{cfg.registry}/{IMAGE_NAME}:{tag}",
                            "gib": float((d.get("metadata") or {}).get("imageSizeBytes") or 0) / GIB,
                            "created": _epoch(d.get("createTime"))})
    return out


def charge_storage(ref: str, info: dict, gone_at: float, cfg: BuildConfig, ledger=None) -> float:
    """An image's storage for its whole life (push to deletion), once."""
    from .cost import get_ledger
    if not info.get("built"):
        return 0.0
    seconds = max(0.0, gone_at - float(info["built"]))
    usd = info["gib"] * cfg.storage_gib_month * seconds / (730 * 3600)
    (ledger or get_ledger()).charge("gke", usd, kind="image_storage", label=ref)
    return usd


def delete_image(ref: str, cfg: BuildConfig, ledger=None, info: Optional[dict] = None) -> bool:
    """Delete one image (all its tags) and charge its storage. True when it is gone (404 counts)."""
    try:
        gcloud("artifacts", "docker", "images", "delete", ref, "--delete-tags", "--quiet", timeout=300)
    except RuntimeError as e:
        if "NOT_FOUND" not in str(e) and "not found" not in str(e).lower():
            print(f"[sandboxes] gke_build: could not delete {ref}: {e}", file=sys.stderr, flush=True)
            return False
    info = info if info is not None else known_images().get(ref, {})
    charge_storage(ref, info, time.time(), cfg, ledger)
    record("deleted", ref)
    return True


def repo_coordinates(cfg: BuildConfig) -> list:
    """["--repository", repo, "--location", region, "--project", project] for the registry's repository."""
    host, project, repo = cfg.registry.split("/")[:3]
    return ["--repository", repo, "--location", host.split("-docker.pkg.dev")[0], "--project", project]


def list_files(cfg: BuildConfig) -> list:
    """The repository's blobs (layers, configs): [{"name": "sha256:...", "bytes", "created"}]. They outlive the images that
    used them: deleting an image leaves its layers billed (found live), so they are deleted here too."""
    out = []
    for f in json.loads(gcloud("artifacts", "files", "list", *repo_coordinates(cfg), "--format=json", timeout=600) or "[]"):
        out.append({"name": f["name"].rsplit("/", 1)[-1], "bytes": int(f.get("sizeBytes") or 0), "created": _epoch(f.get("createTime"))})
    return out


def referenced_blobs(refs) -> set:
    """Every blob the images `refs` need: config and layers, through an index's manifests."""
    out: set = set()

    def walk(url: str, depth: int = 0):
        status, body = registry_request(url)
        if status != 200 or not body:
            raise ImageBuildError(f"gke: cannot read the manifest {url} ({status}): not deleting blobs blind")
        m = json.loads(body)
        if m.get("manifests") and depth == 0:
            base = url.rsplit("/", 1)[0]
            for d in m["manifests"]:
                out.add(d["digest"])
                walk(f"{base}/{d['digest']}", 1)
            return
        if (m.get("config") or {}).get("digest"):
            out.add(m["config"]["digest"])
        out.update(l["digest"] for l in m.get("layers") or [] if l.get("digest"))
    for ref in refs:
        walk(manifest_url(ref))
    return out


def purge_files(cfg: BuildConfig, keep_refs=(), *, older_than_s: float = 3600.0, now: Optional[float] = None) -> dict:
    """Delete the repository's blobs no kept image needs, older than `older_than_s` (a push in progress uploads its blobs before
    its tag exists; 0 when nothing can be pushing, as in `down`). Returns {"deleted", "bytes", "failed"}."""
    from concurrent.futures import ThreadPoolExecutor
    now = now or time.time()
    keep = referenced_blobs(keep_refs) if keep_refs else set()
    doomed = [f for f in list_files(cfg) if f["name"] not in keep and now - (f["created"] or now) >= older_than_s]
    rep = {"deleted": 0, "bytes": 0, "failed": []}

    def rm(f):
        try:
            gcloud("artifacts", "files", "delete", f["name"], *repo_coordinates(cfg), "--quiet", timeout=300)
            return f, None
        except RuntimeError as e:
            gone = "NOT_FOUND" in str(e) or "not found" in str(e).lower()     # already deleted: fine
            return f, None if gone else str(e)[:200]
    with ThreadPoolExecutor(8) as pool:
        for f, err in pool.map(rm, doomed):
            if err:
                rep["failed"].append(f["name"])
            else:
                rep["deleted"] += 1
                rep["bytes"] += f["bytes"]
    return rep


def purge(cfg: BuildConfig, *, in_use=frozenset(), all_images: bool = False, now: Optional[float] = None, ledger=None) -> dict:
    """Delete our images: all of them (`all_images`), else those no pod runs and unused for image_ttl_h. Images recorded but
    found gone (the repository's cleanup policy) are charged up to now and marked deleted. Returns {"deleted", "failed", "gone"}."""
    now = now or time.time()
    rep = {"deleted": [], "failed": [], "gone": []}
    present = {i["ref"]: i for i in list_images(cfg)}
    known = known_images()
    for ref, info in known.items():
        if info["deleted"] is None and ref not in present:
            charge_storage(ref, info, now, cfg, ledger)
            record("deleted", ref, note="found gone")
            rep["gone"].append(ref)
    for ref, img in present.items():
        if ref in in_use:
            continue
        info = known.get(ref) or {"built": img.get("created"), "gib": img["gib"], "last_used": None}
        if not info.get("gib"):
            info["gib"] = img["gib"]
        last = max(float(info.get("last_used") or 0.0), float(info.get("built") or img.get("created") or 0.0))
        if all_images or now - last >= cfg.image_ttl_h * 3600:
            (rep["deleted"] if delete_image(ref, cfg, ledger, info) else rep["failed"]).append(ref)
    kept = [r for r in present if r not in rep["deleted"]]
    rep["files"] = purge_files(cfg, kept, older_than_s=0.0 if all_images and not kept else 3600.0, now=now)
    return rep


def purge_staging(cfg: BuildConfig, older_than_s: float = 3600.0, now: Optional[float] = None) -> int:
    """Delete build source uploads older than `older_than_s` (0: all) from the staging bucket. Returns how many."""
    now = now or time.time()
    try:
        listing = json.loads(gcloud("storage", "ls", "--json", f"gs://{cfg.bucket}/source/**") or "[]")
    except RuntimeError as e:
        if "matched no objects" in str(e) or "One or more URLs matched no objects" in str(e):
            return 0
        raise
    old = [o["url"] for o in listing if now - (_epoch((o.get("metadata") or {}).get("timeCreated")) or 0.0) >= older_than_s]
    for i in range(0, len(old), 100):
        gcloud("storage", "rm", *old[i:i + 100])
    return len(old)


def cancel_ongoing(cfg: BuildConfig) -> list:
    """Cancel every build still queued or running in the project's build region: one that finished after `down` purged the
    registry would push an image that bills until the cleanup policy removes it. Returns the cancelled build ids."""
    if not (cfg.project and cfg.region):
        return []
    ids = [b["id"] for b in json.loads(gcloud("builds", "list", "--ongoing", "--format=json", "--project", cfg.project,
                                              "--region", cfg.region) or "[]") if b.get("id")]
    for bid in ids:
        try:
            gcloud("builds", "cancel", bid, "--project", cfg.project, "--region", cfg.region)
        except RuntimeError as e:
            print(f"[sandboxes] gke_build: could not cancel build {bid}: {str(e)[:200]}", file=sys.stderr, flush=True)
    return ids
