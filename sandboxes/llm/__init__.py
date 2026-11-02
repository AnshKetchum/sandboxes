"""sandboxes.llm: one recorded LLM client over OpenRouter and OpenAI-compatible servers (vLLM, SGLang), with sessions.

    from sandboxes.llm import LLMClient, load_llm_config, open_llm_session

    cfg = load_llm_config("configs/llm/openrouter.yaml")
    with open_llm_session("greenhouse", name="r-20261102-NA1"):
        r = LLMClient(cfg).chat(messages, model="openai/gpt-6-luna", tags={"stage": "evolve"},
                                trajectory={"id": "unit-1.3-review-2", "kind": "fidelity"})

Every call is recorded in the active session: tokens, characters, cost and the price it was charged at, the provider that
served it, and its content (messages stored once, by hash), so a call can be rebuilt as a trajectory. docs/llm.md has the
config, the record fields and the CLI (`sandboxes llm ...`). Needs httpx (`pip install sandboxes[llm]`).
"""

from .client import ChatResult, LLMClient, LLMError, LLMRequestError
from .config import LLMConfig, LLMProviderConfig, load_llm_config
from .pricing import OpenRouterPrices, Price, Rates, get_openrouter_prices
from .session import (LLMSession, current_llm_session, get_llm_session, list_llm_sessions, load_llm_session,
                      open_llm_session, use_llm_session)

__all__ = ["ChatResult", "LLMClient", "LLMError", "LLMRequestError", "LLMConfig", "LLMProviderConfig", "load_llm_config",
           "OpenRouterPrices", "Price", "Rates", "get_openrouter_prices", "LLMSession", "current_llm_session",
           "get_llm_session", "list_llm_sessions", "load_llm_session", "open_llm_session", "use_llm_session"]
