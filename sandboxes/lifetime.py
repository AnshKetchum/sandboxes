"""Default and maximum sandbox lifetime: the guard against sandboxes that outlive their job and bill for ever.

Every sandbox the library creates gets a FINITE lifetime, whatever the config says:

    SANDBOX_DEFAULT_LIFETIME_MIN   used when the config gives none, or 0 / a negative number (default 240 = 4 h)
    SANDBOX_MAX_LIFETIME_MIN       hard ceiling on any requested lifetime (default 480 = 8 h)

What each provider does with it: E2B (`timeout`) and Blaxel (`ttl`) end the sandbox at that age, hard. Daytona only
auto-stops after that many minutes of INACTIVITY, so a busy or hung one is not covered; `sandboxes.janitor` is the
backstop for it (and for harbor's own E2B environment, which fixes its lifetime at 24 h).
"""

import os


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


DEFAULT_LIFETIME_MIN = _env_int("SANDBOX_DEFAULT_LIFETIME_MIN", 240)
MAX_LIFETIME_MIN = _env_int("SANDBOX_MAX_LIFETIME_MIN", 480)
DEFAULT_IDLE_STOP_MIN = 120


def clamp_lifetime_min(minutes) -> int:
    """A finite lifetime in minutes: the default when unset/<=0, never above the ceiling."""
    try:
        m = int(minutes or 0)
    except (TypeError, ValueError):
        m = 0
    if m <= 0:
        m = DEFAULT_LIFETIME_MIN
    return max(1, min(m, MAX_LIFETIME_MIN))
