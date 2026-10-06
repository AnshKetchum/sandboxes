"""Every sandbox gets a finite lifetime; the janitor selects by age."""
import asyncio
import datetime as dt

from sandboxes import janitor
from sandboxes.lifetime import DEFAULT_LIFETIME_MIN, MAX_LIFETIME_MIN, clamp_lifetime_min
from sandboxes.models import SandboxProviderConfig
from sandboxes.raw import SandboxSpec


def test_clamp_never_unlimited():
    assert clamp_lifetime_min(None) == DEFAULT_LIFETIME_MIN
    assert clamp_lifetime_min(0) == DEFAULT_LIFETIME_MIN
    assert clamp_lifetime_min(-5) == DEFAULT_LIFETIME_MIN
    assert clamp_lifetime_min("abc") == DEFAULT_LIFETIME_MIN
    assert clamp_lifetime_min(90) == 90
    assert clamp_lifetime_min(10 ** 6) == MAX_LIFETIME_MIN


def test_config_and_spec_defaults_are_finite():
    c = SandboxProviderConfig(name="daytona")
    assert c.auto_delete_minutes == DEFAULT_LIFETIME_MIN and c.auto_stop_minutes > 0
    assert SandboxProviderConfig(name="e2b", auto_delete_minutes=0, auto_stop_minutes=0).auto_delete_minutes == DEFAULT_LIFETIME_MIN
    assert SandboxProviderConfig(name="e2b", auto_delete_minutes=10 ** 6).auto_delete_minutes == MAX_LIFETIME_MIN
    assert SandboxSpec(image="x", lifetime_s=0).lifetime_s == DEFAULT_LIFETIME_MIN * 60
    assert SandboxSpec(image="x", lifetime_s=10 ** 8).lifetime_s == MAX_LIFETIME_MIN * 60


def test_janitor_age_helper():
    now = dt.datetime.now(dt.timezone.utc)
    assert abs(janitor._age_h((now - dt.timedelta(hours=7)).isoformat()) - 7) < 0.01
    assert janitor._age_h("garbage") is None and janitor._age_h(None) is None
