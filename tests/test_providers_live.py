"""Live provider checks (skipped unless that provider's API key is set). One sandbox per provider:
boot docker-in-docker, build + run a container, round-trip a directory, and verify the cost ledger.

    pytest tests/test_providers_live.py -k e2b -s
"""
import asyncio
import os
import tempfile
from pathlib import Path

import pytest

from sandboxes import CostLedger, SandboxRouter
from sandboxes.cost import summary
from sandboxes.models import SandboxProviderConfig

IMAGE = os.environ.get("SANDBOX_TEST_IMAGE", "registry-b.deepgrove.ai/openenv/swe-env-builder-dind:latest")
CASES = {
    "daytona": ["DAYTONA_API_KEY"], "e2b": ["E2B_API_KEY"], "blaxel": ["BL_API_KEY", "BL_WORKSPACE"],
}


@pytest.mark.parametrize("name", list(CASES))
def test_open_exec_files_cost(name, monkeypatch, tmp_path):
    if not all(os.environ.get(k) for k in CASES[name]):
        pytest.skip(f"{name}: {CASES[name]} not set")
    ledger = tmp_path / "ledger.jsonl"
    monkeypatch.setenv("SANDBOX_COST_LEDGER", str(ledger))
    extra = {"docker_loop_gb": 1} if name == "blaxel" else {}
    router = SandboxRouter([SandboxProviderConfig(name=name, cpu=2, memory_mb=4096, disk_gb=10, extra=extra)])

    async def go():
        sbx = await router.open(IMAGE, boot_cmd="dind-start.sh")
        try:
            r = await sbx.exec("docker info --format '{{.Driver}}'", timeout=60)
            assert r.rc == 0 and "overlay2" in r.stdout, r
            r = await sbx.exec("printf 'FROM busybox\\nRUN echo built > /x\\n' > /tmp/D && docker build -q -t t -f /tmp/D /tmp >/dev/null && docker run --rm t cat /x", timeout=180)
            assert r.rc == 0 and "built" in r.stdout, r
            src = tmp_path / "src"; (src / "sub").mkdir(parents=True)
            (src / "a.txt").write_text("hello"); (src / "sub" / "b.txt").write_text("world")
            await sbx.put_file(src, "/work/src")
            r = await sbx.exec("cat /work/src/a.txt /work/src/sub/b.txt && echo -n '!' > /work/src/new.txt", timeout=30)
            assert "helloworld" in r.stdout, r
            back = tmp_path / "back"
            await sbx.get_file("/work/src", back)
            assert (back / "new.txt").read_text() == "!" and (back / "sub" / "b.txt").read_text() == "world"
            r = await sbx.exec("sleep 70; echo done", timeout=120)       # longer than any single-call cap
            assert r.rc == 0 and "done" in r.stdout, r
        finally:
            usd = await router.close(sbx)
        return usd

    usd = asyncio.run(go())
    s = summary(ledger)["providers"][name]
    assert s["opened"] == 1 and s["closed"] == 1 and s["running"] == 0
    assert 0 < usd < 0.5 and abs(s["closed_usd"] - usd) < 1e-4
