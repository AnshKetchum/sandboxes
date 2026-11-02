"""What an LLM call costs: OpenRouter's live per-provider prices, or static rates from the config.

OpenRouter lists every provider endpoint serving a model, with its prices (USD per token, as strings) and the higher
rates above a prompt size, at GET <base_url>/models/<model>/endpoints. The prices of the models in use are refreshed in
the background every `refresh_s`, so a call never waits for them (except the first call of a model) and is priced with
rates at most that old. One provider can serve a model at several prices (OpenAI's standard and flex endpoints), and the
response names only the provider, so the endpoint is inferred from the billed cost: the one whose rates give it.

Each call records the price it was charged at (`Price.to_record`): the endpoint, the rates, the tier, when they were
fetched and their age; `version` changes whenever a price does, and a session keeps each version it used (prices.jsonl).
"""

from __future__ import annotations

import hashlib
import json
import sys
import threading
import time
from dataclasses import asdict, dataclass, field, replace
from typing import Callable, Optional

from .config import PricingConfig

PER_M = 1_000_000.0
FETCH_TIMEOUT_S = 20.0


@dataclass(frozen=True)
class Rates:
    """USD per million tokens. None: billed like uncached prompt tokens (cached, cache write) or like completion tokens
    (reasoning)."""

    prompt_per_m: float = 0.0
    cached_per_m: Optional[float] = None
    cache_write_per_m: Optional[float] = None
    completion_per_m: float = 0.0
    reasoning_per_m: Optional[float] = None

    def usd(self, tokens: dict) -> float:
        prompt, cached = tokens.get("prompt") or 0, tokens.get("cached") or 0
        write, completion = tokens.get("cache_write") or 0, tokens.get("completion") or 0
        reasoning = tokens.get("reasoning") or 0
        uncached = max(0, prompt - cached - write)
        cached_rate = self.prompt_per_m if self.cached_per_m is None else self.cached_per_m
        write_rate = self.prompt_per_m if self.cache_write_per_m is None else self.cache_write_per_m
        if self.reasoning_per_m is None:
            out = completion * self.completion_per_m
        else:
            out = max(0, completion - reasoning) * self.completion_per_m + reasoning * self.reasoning_per_m
        return (uncached * self.prompt_per_m + cached * cached_rate + write * write_rate + out) / PER_M


@dataclass(frozen=True)
class Price:
    source: str                     # openrouter | static
    model: str
    upstream: str                   # the provider serving it ("OpenAI"); a static price: the config provider's name
    endpoint_tag: str = ""          # OpenRouter's endpoint tag ("openai/flex"); "" for a static price
    base: Rates = field(default_factory=Rates)
    tiers: tuple = ()               # ((min_prompt_tokens, Rates), ...), ascending: the rates from that prompt size on
    request_usd: float = 0.0
    discount: float = 0.0
    fetched_at: float = 0.0
    version: str = ""

    def get_rates(self, prompt_tokens: int) -> tuple[Rates, str]:
        rates, tier = self.base, "base"
        for min_tokens, tier_rates in self.tiers:
            if prompt_tokens >= min_tokens:
                rates, tier = tier_rates, f">={min_tokens}"
        return rates, tier

    def usd(self, tokens: dict) -> float:
        rates, _ = self.get_rates(tokens.get("prompt") or 0)
        return (rates.usd(tokens) + self.request_usd) * (1.0 - self.discount)

    def to_record(self, tokens: dict, *, now: float, max_age_s: float, match: str = "") -> dict:
        """The price block a call records: the rates it was charged at, the tier, the endpoint and how fresh they were."""
        rates, tier = self.get_rates(tokens.get("prompt") or 0)
        age = max(0.0, now - self.fetched_at) if self.fetched_at else None
        return {"source": self.source, "model": self.model, "upstream": self.upstream, "endpoint_tag": self.endpoint_tag,
                "endpoint_match": match, **asdict(rates), "request_usd": self.request_usd, "discount": self.discount,
                "tier": tier, "fetched_at": self.fetched_at or None, "age_s": None if age is None else round(age, 1),
                "stale": bool(age is not None and age > max_age_s), "version": self.version}

    def to_history(self) -> dict:
        """A prices.jsonl row: everything about this price, tiers included."""
        return {"version": self.version, "source": self.source, "model": self.model, "upstream": self.upstream,
                "endpoint_tag": self.endpoint_tag, "base": asdict(self.base),
                "tiers": [[m, asdict(r)] for m, r in self.tiers], "request_usd": self.request_usd,
                "discount": self.discount, "fetched_at": self.fetched_at}


def _get_version(price: Price) -> str:
    body = json.dumps([price.source, price.model, price.upstream, price.endpoint_tag, asdict(price.base),
                       [[m, asdict(r)] for m, r in price.tiers], price.request_usd, price.discount], sort_keys=True)
    return hashlib.sha256(body.encode()).hexdigest()[:12]


def _per_m(value) -> Optional[float]:
    try:
        return float(value) * PER_M
    except (TypeError, ValueError):
        return None


def _parse_rates(p: dict) -> Rates:
    reasoning = _per_m(p.get("internal_reasoning"))
    return Rates(prompt_per_m=_per_m(p.get("prompt")) or 0.0, cached_per_m=_per_m(p.get("input_cache_read")),
                 cache_write_per_m=_per_m(p.get("input_cache_write")), completion_per_m=_per_m(p.get("completion")) or 0.0,
                 reasoning_per_m=reasoning if reasoning else None)


def parse_endpoints(model: str, data: dict, fetched_at: float) -> list[Price]:
    """OpenRouter's /models/<model>/endpoints answer as one Price per endpoint."""
    out = []
    for e in (data.get("data") or data).get("endpoints") or []:
        p = e.get("pricing") or {}
        base = _parse_rates(p)
        tiers = []
        for o in p.get("overrides") or []:
            if o.get("min_prompt_tokens") is not None:
                tiers.append((int(o["min_prompt_tokens"]), _parse_rates({**p, **o})))
        price = Price(source="openrouter", model=model, upstream=e.get("provider_name") or "", endpoint_tag=e.get("tag") or "",
                      base=base, tiers=tuple(sorted(tiers, key=lambda t: t[0])), request_usd=float(p.get("request") or 0.0),
                      discount=float(p.get("discount") or 0.0), fetched_at=fetched_at)
        out.append(replace(price, version=_get_version(price)))
    return out


def get_static_price(model: str, provider_name: str, cfg: PricingConfig) -> Optional[Price]:
    """A config's static rates as a Price (None when the config prices nothing)."""
    if cfg.source != "static":
        return None
    rates = Rates(prompt_per_m=cfg.prompt_per_m or 0.0, cached_per_m=cfg.cached_per_m, cache_write_per_m=cfg.cache_write_per_m,
                  completion_per_m=cfg.completion_per_m or 0.0, reasoning_per_m=cfg.reasoning_per_m)
    price = Price(source="static", model=model, upstream=provider_name, base=rates, request_usd=cfg.request_usd)
    return replace(price, version=_get_version(price))


class OpenRouterPrices:
    """The current per-endpoint prices of the models in use, refreshed every `refresh_s` by a daemon thread. `fetch`
    (model -> the endpoints JSON) is the HTTP seam; a failed refresh keeps the last prices, whose age then grows."""

    def __init__(self, base_url: str, *, refresh_s: float = 60.0, fetch: Optional[Callable[[str], dict]] = None):
        self.base_url = base_url.rstrip("/")
        self.refresh_s = refresh_s
        self.fetch = fetch or self._fetch
        self.endpoints: dict[str, list[Price]] = {}
        self.lock = threading.Lock()
        self.thread: Optional[threading.Thread] = None
        self.errors = 0

    def _fetch(self, model: str) -> dict:
        import httpx
        r = httpx.get(f"{self.base_url}/models/{model}/endpoints", timeout=FETCH_TIMEOUT_S)
        r.raise_for_status()
        return r.json()

    def refresh(self, model: str) -> bool:
        """Fetch `model`'s prices now; False (and the old prices kept) when that fails."""
        try:
            prices = parse_endpoints(model, self.fetch(model), time.time())
        except Exception as e:  # noqa: BLE001 — pricing never fails a call: the old prices stay, and age
            self.errors += 1
            if self.errors <= 3:
                print(f"[sandboxes.llm] price refresh for {model} failed: {type(e).__name__}: {str(e)[:200]}",
                      file=sys.stderr, flush=True)
            return False
        if prices:
            with self.lock:
                self.endpoints[model] = prices
        return bool(prices)

    def get(self, model: str) -> list[Price]:
        """`model`'s endpoint prices: fetched now the first time, from the background refresh afterwards."""
        with self.lock:
            known = model in self.endpoints
        if not known:
            if not self.refresh(model):
                with self.lock:
                    self.endpoints.setdefault(model, [])
        self._start()
        with self.lock:
            return list(self.endpoints.get(model, []))

    def _start(self) -> None:
        with self.lock:
            if self.thread is not None or self.refresh_s <= 0:
                return
            self.thread = threading.Thread(target=self._loop, name="sandboxes-llm-prices", daemon=True)
            self.thread.start()

    def _loop(self) -> None:
        while True:
            time.sleep(self.refresh_s)
            with self.lock:
                models = list(self.endpoints)
            for model in models:
                self.refresh(model)

    def choose(self, model: str, upstream: str, tokens: dict, billed_usd: Optional[float]) -> tuple[Optional[Price], str]:
        """The endpoint that served a call, and how sure: `only` (one endpoint of that provider), `exact` (its rates give
        the billed cost), `ambiguous` (several do), `closest` (none does; the nearest), `unbilled` (no billed cost: the
        provider's first endpoint), `none` (no prices)."""
        prices = self.get(model)
        candidates = [p for p in prices if upstream and p.upstream == upstream] or prices
        if not candidates:
            return None, "none"
        if len(candidates) == 1:
            return candidates[0], "only"
        if billed_usd is None:
            return candidates[0], "unbilled"
        diffs = [(abs(p.usd(tokens) - billed_usd), p) for p in candidates]
        tolerance = max(1e-7, 0.01 * billed_usd)
        close = [p for d, p in diffs if d <= tolerance]
        if len(close) > 1:
            return close[0], "ambiguous"
        if close:
            return close[0], "exact"
        return min(diffs, key=lambda pair: pair[0])[1], "closest"


_REGISTRY: dict[str, OpenRouterPrices] = {}
_REGISTRY_LOCK = threading.Lock()


def get_openrouter_prices(base_url: str, refresh_s: float = 60.0) -> OpenRouterPrices:
    """One price cache (and refresh thread) per OpenRouter base URL in this process."""
    with _REGISTRY_LOCK:
        prices = _REGISTRY.get(base_url)
        if prices is None:
            prices = _REGISTRY[base_url] = OpenRouterPrices(base_url, refresh_s=refresh_s)
        return prices
