"""harbor's E2B environment, with the lifetime and labels sandboxes needs.

harbor's own `E2BEnvironment` creates every sandbox with a hardcoded `timeout=86_400` (24 h) and only `environment_name` /
`session_id` as metadata, so a crashed run leaves sandboxes billing for a day and sandboxes' janitor / `stop --owner` cannot
recognise them. This subclass changes exactly that call: the sandbox ends at `lifetime_s` (E2B kills it at that age, hard) and
carries the standard managed/owner/run labels. Everything else (templates, exec, files, network policy) is harbor's.

The lifetime is never unlimited: unset falls back to the sandboxes default (lifetime.clamp_lifetime_min).
"""

from __future__ import annotations

import asyncio
import re
import shlex
import shutil
import tempfile
from pathlib import Path
from typing import Optional

from harbor.environments import e2b as _harbor_e2b
from harbor.environments.e2b import E2BEnvironment
from harbor.models.task.config import NetworkMode
from e2b.template_async import main as _template_main
from e2b.template_async.build_api import get_build_status as _get_build_status
from tenacity import retry, stop_after_attempt, wait_exponential

from .gates import build_slot, log_build_failure, recent_failure, remember_failure, single_flight
from .labels import standard_labels
from .lifetime import clamp_lifetime_min


def split_env_instructions(dockerfile: str) -> str:
    """Rewrite `ENV A=1 B=2 C=3` (also across `\\` continuations) as one `ENV K=V` line per variable.

    E2B's Dockerfile-to-template converter mangles an ENV line that sets several variables: it rebuilds it as `ENV A 1 B 2 C 3`, which
    Docker reads as ONE variable, and values come out with stray whitespace (pip then dies on `PIP_NO_CACHE_DIR='1 '`). 657 of the 1000
    pr-task-synthesis-v0 Dockerfiles have such a line. One variable per ENV line is converted correctly.
    """
    text = re.sub(r"\\\n[ \t]*", " ", dockerfile)                   # join continuation lines
    out = []
    for line in text.split("\n"):
        m = re.match(r"^(\s*)ENV\s+(.*)$", line)
        if m:
            try:
                toks = shlex.split(m.group(2), comments=False)
            except ValueError:
                toks = []
            if len(toks) > 1 and all("=" in t and not t.startswith("=") for t in toks):
                out.extend(f"{m.group(1)}ENV {t.split('=', 1)[0]}={shlex.quote(t.split('=', 1)[1])}" for t in toks)
                continue
        out.append(line)
    return "\n".join(out)


class LifetimeE2BEnvironment(E2BEnvironment):
    def __init__(self, *args, lifetime_s: Optional[int] = None, labels: Optional[dict] = None, **kwargs):
        self._lifetime_s = int(lifetime_s) if lifetime_s else clamp_lifetime_min(None) * 60
        self._labels = dict(labels or {})
        super().__init__(*args, **kwargs)

    async def _does_template_exist(self) -> bool:
        """harbor asks only whether the alias exists, but E2B registers the alias AND its 'default' tag when a build STARTS: a failed build
        leaves both, and every later start then skips the build and dies with "404: tag 'default' does not exist". Built means the
        build behind the default tag finished ('ready')."""
        if not await super()._does_template_exist():
            return False
        try:
            tags = await _harbor_e2b.AsyncTemplate.get_tags(self._template_name)
            build = next((t.build_id for t in tags if getattr(t, "tag", None) == "default"), None)
            if not build:
                return False
            config = _template_main.ConnectionConfig(**_harbor_e2b.AsyncTemplate._resolve_api_params())
            status = await _get_build_status(_template_main.get_api_client(config), self._template_name, build, 0)
        except Exception:  # noqa: BLE001
            return False
        return str(status.status) == "ready"

    async def _build_with_split_env(self):
        """harbor's build, from a copy of the task's environment directory whose Dockerfile has one variable per ENV line."""
        dockerfile = Path(self.environment_dir) / "Dockerfile"
        if not dockerfile.exists() or self.task_env_config.docker_image:
            return await super()._create_template()
        tmp = Path(tempfile.mkdtemp(prefix="e2b-ctx-"))
        original = self.environment_dir
        try:
            shutil.copytree(original, tmp / "environment")
            (tmp / "environment" / "Dockerfile").write_text(split_env_instructions(dockerfile.read_text()))
            self.environment_dir = tmp / "environment"
            await super()._create_template()
        finally:
            self.environment_dir = original
            shutil.rmtree(tmp, ignore_errors=True)

    async def _create_template(self):
        """harbor builds a template whenever none exists, and E2B allows only ~20 builds at once, so 100 sandboxes all building at the
        start of a run fail and burn each task's tries. Here at most `max_concurrent_builds` builds run host-wide, and the parallel tries of
        one task share one build: the first builds, the others wait on it and find the template there (or its failure: a build that just
        failed is not repeated by each sibling)."""
        from .registry import PROVIDERS
        limit = int(PROVIDERS["e2b"].capabilities.get("max_concurrent_builds") or 15)
        async with single_flight("e2b", self._template_name):
            failed = recent_failure("e2b", self._template_name)
            if failed:
                raise RuntimeError(f"e2b template {self._template_name} failed to build moments ago: {failed}")
            if await self._does_template_exist():
                return
            timeout = float(PROVIDERS["e2b"].capabilities.get("build_timeout_s") or 900)
            try:
                async with build_slot("e2b", limit):
                    try:
                        await asyncio.wait_for(self._build_with_split_env(), timeout)
                    except asyncio.TimeoutError:
                        raise RuntimeError(f"e2b template build exceeded {timeout:.0f}s and was cancelled") from None
            except Exception as e:  # noqa: BLE001
                msg = f"{type(e).__name__}: {str(e)[:300]}"
                remember_failure("e2b", self._template_name, msg)
                log_build_failure("e2b", self._template_name, msg)
                raise

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
    async def _create_sandbox(self):
        metadata = {"environment_name": self.environment_name, "session_id": self.session_id,
                    **standard_labels(extra=self._labels)}
        self._sandbox = await _harbor_e2b.AsyncSandbox.create(
            template=self._template_name,
            metadata=metadata,
            envs=self._startup_env(),
            timeout=self._lifetime_s,
            allow_internet_access=(self.network_policy.network_mode != NetworkMode.NO_NETWORK),
            network=self._sandbox_create_network_options(),
        )
