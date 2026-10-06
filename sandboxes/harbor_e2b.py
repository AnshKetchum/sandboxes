"""harbor's E2B environment, with the lifetime and labels sandboxes needs.

harbor's own `E2BEnvironment` creates every sandbox with a hardcoded `timeout=86_400` (24 h) and only `environment_name` /
`session_id` as metadata, so a crashed run leaves sandboxes billing for a day and sandboxes' janitor / `stop --owner` cannot
recognise them. This subclass changes exactly that call: the sandbox ends at `lifetime_s` (E2B kills it at that age, hard) and
carries the standard managed/owner/run labels. Everything else (templates, exec, files, network policy) is harbor's.

The lifetime is never unlimited: unset falls back to the sandboxes default (lifetime.clamp_lifetime_min).
"""

from __future__ import annotations

from typing import Optional

from harbor.environments import e2b as _harbor_e2b
from harbor.environments.e2b import E2BEnvironment
from harbor.models.task.config import NetworkMode
from tenacity import retry, stop_after_attempt, wait_exponential

from .labels import standard_labels
from .lifetime import clamp_lifetime_min


class LifetimeE2BEnvironment(E2BEnvironment):
    def __init__(self, *args, lifetime_s: Optional[int] = None, labels: Optional[dict] = None, **kwargs):
        self._lifetime_s = int(lifetime_s) if lifetime_s else clamp_lifetime_min(None) * 60
        self._labels = dict(labels or {})
        super().__init__(*args, **kwargs)

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
