"""No test reaches a real cloud: configs/gke.yaml names a real project and this machine may hold real gcloud credentials and a
kubeconfig. Every test starts with the GKE config pointing nowhere and with gcloud and the Kubernetes client raising; a test
that needs them replaces them with fakes (monkeypatch), which overrides this."""

from __future__ import annotations

import os

import pytest


class RealCloudCall(AssertionError):
    pass


def _refuse(what):
    def call(*a, **kw):
        raise RealCloudCall(f"a test reached the real {what}: replace it with a fake")
    return call


@pytest.fixture(autouse=True)
def no_real_cloud(tmp_path_factory, monkeypatch):
    base = tmp_path_factory.getbasetemp()
    monkeypatch.setenv("SANDBOX_GKE_CONFIG", str(base / "no-gke-config.yaml"))
    for k, d in (("SANDBOX_RATELIMIT_DIR", "ratelimit"), ("SANDBOX_HEALTH_DIR", "health")):     # never the machine's own state
        if not os.environ.get(k, "").startswith(str(base)):
            monkeypatch.setenv(k, str(base / d))
    for k in ("GKE_PROJECT", "GKE_LOCATION", "GKE_CLUSTER", "GKE_REGISTRY"):
        monkeypatch.delenv(k, raising=False)
    # no process-wide pod watch thread: it would outlive the test's fakes and reach the real cluster (test_gke_watch turns it on)
    monkeypatch.setenv("GKE_POD_WATCH", "0")
    from sandboxes import gke_build, gke_ops
    from sandboxes.backends import gke_raw
    monkeypatch.setattr(gke_ops, "gcloud", _refuse("gcloud (gke_ops)"))
    monkeypatch.setattr(gke_build, "gcloud", _refuse("gcloud (gke_build)"))
    monkeypatch.setattr(gke_build, "registry_request", _refuse("Artifact Registry"))
    monkeypatch.setattr(gke_raw, "_core", _refuse("Kubernetes API"))
