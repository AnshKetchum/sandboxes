"""LLM provider configs: the `llm_providers:` list, shaped like `sandbox_providers:`.

    llm_providers:
      - name: openrouter                 # label recorded on every call
        provider_type: openrouter        # openrouter | openai_compatible
        base_url: https://openrouter.ai/api/v1
        api_key_env: [OPENROUTER_API_KEYS, OPENROUTER_API_KEY]   # a pool (comma or space separated), used round-robin
        keys_file: /ansh/data_env/openrouter_keys.txt              # one key per line, when no env var is set
        models: ["*"]                    # model ids it serves (globs); the most specific matching glob wins
        routing: {order: [OpenAI], allow_fallbacks: false}         # openrouter only: sent as `provider`
        pricing: {source: openrouter, refresh_s: 60}
      - name: local-sglang
        provider_type: openai_compatible # vLLM, SGLang or any server speaking the OpenAI chat API
        base_url: http://gpu-box:30000/v1
        models: ["local/*"]
        model_map: {local/qwen3-coder: Qwen/Qwen3-Coder-30B-A3B-Instruct}
        pricing: {source: static, prompt_per_m: 0.02, completion_per_m: 0.08}
        capabilities: {reasoning_effort: false}
        extra_body: {chat_template_kwargs: {enable_thinking: true}}
    recording:
      content: true                      # store messages, answers, reasoning and tool calls (false: counts only)
      sessions_dir: null                 # default <cache>/llm_sessions

A config is a path, a dict, or `{base: path, overrides: {...}}`; overrides deep-merge into the base, a key naming a provider
(`overrides: {openrouter: {routing: {order: [Azure]}}}`) into that provider. SANDBOX_LLM_CONFIG_PATH names the default file.
"""

from __future__ import annotations

import fnmatch
import os
from pathlib import Path
from typing import Optional, Union

import yaml
from pydantic import BaseModel, Field, field_validator

from ..errors import ConfigError

LLM_CONFIG_PATH_ENV = "SANDBOX_LLM_CONFIG_PATH"
PROVIDER_TYPES = ("openrouter", "openai_compatible")
PRICING_SOURCES = ("openrouter", "static", "none")
DEFAULT_CAPABILITIES = {
    "openrouter": {"tools": True, "json_schema": True, "reasoning_effort": True, "provider_routing": True},
    "openai_compatible": {"tools": True, "json_schema": True, "reasoning_effort": False, "provider_routing": False},
}


class RoutingConfig(BaseModel):
    order: list[str] = Field(default_factory=list)   # upstream providers to try first (OpenRouter `provider.order`)
    allow_fallbacks: bool = True                       # whether OpenRouter may route past `order`


class PricingConfig(BaseModel):
    source: str = "openrouter"            # openrouter: OpenRouter's live per-provider prices; static: the rates below; none
    refresh_s: float = 60.0               # openrouter: refresh the prices of the models in use this often, in the background
    max_age_s: float = 3600.0             # openrouter: a call priced with prices older than this is marked stale
    verify_generations: float = 0.05      # openrouter: share of calls whose generation record is fetched afterwards
    prompt_per_m: Optional[float] = None  # static: USD per million uncached prompt tokens
    cached_per_m: Optional[float] = None  # static: per million cached prompt tokens (default: prompt_per_m)
    cache_write_per_m: Optional[float] = None
    completion_per_m: Optional[float] = None
    reasoning_per_m: Optional[float] = None   # when reasoning tokens are billed apart from completion tokens
    request_usd: float = 0.0              # per request

    @field_validator("source")
    @classmethod
    def _check_source(cls, v: str) -> str:
        if v not in PRICING_SOURCES:
            raise ValueError(f"pricing.source {v!r}: one of {PRICING_SOURCES}")
        return v


class LLMProviderConfig(BaseModel):
    name: str
    provider_type: str = "openrouter"
    base_url: str = "https://openrouter.ai/api/v1"
    api_key_env: list[str] = Field(default_factory=list)   # default: OPENROUTER_API_KEYS, OPENROUTER_API_KEY for openrouter
    keys_file: Optional[str] = None
    env_file: Optional[str] = None        # KEY=VALUE lines loaded into the environment (never overriding what is set)
    models: list[str] = Field(default_factory=lambda: ["*"])
    model_map: dict[str, str] = Field(default_factory=dict)
    routing: RoutingConfig = Field(default_factory=RoutingConfig)
    pricing: PricingConfig = Field(default_factory=PricingConfig)
    capabilities: dict[str, bool] = Field(default_factory=dict)
    extra_body: dict = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    timeout_s: float = 300.0              # a whole request, its response read included
    retries: int = 8                      # attempts on rate limits, 5xx, timeouts and transport errors
    enabled: bool = True

    @field_validator("provider_type")
    @classmethod
    def _check_type(cls, v: str) -> str:
        if v not in PROVIDER_TYPES:
            raise ValueError(f"provider_type {v!r}: one of {PROVIDER_TYPES}")
        return v

    def get_capability(self, name: str) -> bool:
        return bool(self.capabilities.get(name, DEFAULT_CAPABILITIES[self.provider_type].get(name, False)))

    def get_key_envs(self) -> list[str]:
        if self.api_key_env:
            return list(self.api_key_env)
        return ["OPENROUTER_API_KEYS", "OPENROUTER_API_KEY"] if self.provider_type == "openrouter" else []

    def serves(self, model: str) -> bool:
        return any(fnmatch.fnmatchcase(model, pattern) for pattern in self.models)

    def to_public(self) -> dict:
        """What a session records about this provider: everything but where its keys come from."""
        return self.model_dump(exclude={"api_key_env", "keys_file", "env_file", "headers"})


class RecordingConfig(BaseModel):
    content: bool = True
    sessions_dir: Optional[str] = None


class LLMConfig(BaseModel):
    llm_providers: list[LLMProviderConfig] = Field(default_factory=list)
    recording: RecordingConfig = Field(default_factory=RecordingConfig)

    def get_provider(self, model: str) -> LLMProviderConfig:
        """The enabled provider whose matching `models` glob is the most specific (the most characters that are not
        wildcards), the earliest listed on a tie: `local/*` wins over a catch-all `*` wherever the two are listed."""
        best, best_score = None, -1
        for p in self.llm_providers:
            if not p.enabled:
                continue
            score = max((len(pattern.replace("*", "").replace("?", "")) for pattern in p.models
                         if fnmatch.fnmatchcase(model, pattern)), default=-1)
            if score > best_score:
                best, best_score = p, score
        if best is None:
            raise ConfigError(f"no enabled llm provider serves model {model!r}: "
                              + ", ".join(f"{p.name} {p.models}" for p in self.llm_providers))
        return best


def _deep_merge(base: dict, over: dict) -> dict:
    out = dict(base)
    for k, v in (over or {}).items():
        out[k] = _deep_merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def _read(path: Union[str, Path]) -> dict:
    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"llm config {p} not found")
    return yaml.safe_load(p.read_text()) or {}


def _apply_overrides(raw: dict, overrides: dict) -> dict:
    """`overrides` merged into a plain config: a key naming a provider into that provider, `llm_providers` appended as
    further providers, any other key deep-merged at the top."""
    over = dict(overrides or {})
    providers = [_deep_merge(p, over.pop(p.get("name"), None) or {}) for p in raw.get("llm_providers") or []]
    providers += list(over.pop("llm_providers", None) or [])
    merged = _deep_merge({k: v for k, v in raw.items() if k != "llm_providers"}, over)
    merged["llm_providers"] = providers
    return merged


def _resolve(raw: dict, base_dir: Optional[Path] = None) -> dict:
    """A `{base, overrides}` mapping into the plain config it stands for (bases may nest; a relative base is read from
    the directory of the file naming it)."""
    if "base" not in raw:
        return raw
    base_path = Path(raw["base"])
    if not base_path.is_absolute() and base_dir is not None:
        base_path = base_dir / base_path
    return _apply_overrides(_resolve(_read(base_path), base_path.parent), raw.get("overrides") or {})


def load_llm_config(source: Union[str, Path, dict, None] = None, overrides: Optional[dict] = None) -> LLMConfig:
    """An LLMConfig from a path, a dict (plain or `{base, overrides}`) or SANDBOX_LLM_CONFIG_PATH, with `overrides`
    applied on top as a `{base, overrides}` mapping applies its own."""
    base_dir = None
    if source is None:
        source = os.environ.get(LLM_CONFIG_PATH_ENV)
        if not source:
            raise ConfigError(f"no llm config: pass a path or set {LLM_CONFIG_PATH_ENV}")
    if isinstance(source, (str, Path)):
        base_dir = Path(source).parent
        raw = _read(source)
    else:
        raw = dict(source)
    raw = _apply_overrides(_resolve(raw, base_dir), overrides or {})
    try:
        return LLMConfig.model_validate(raw)
    except ValueError as e:
        raise ConfigError(f"llm config: {e}") from e
