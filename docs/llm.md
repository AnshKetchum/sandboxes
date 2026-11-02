# sandboxes.llm: recorded LLM calls

One client for every LLM call, over OpenRouter or any OpenAI-compatible server (vLLM, SGLang), that records each call in a
**session**: tokens, characters, cost and the exact price it was charged at, the provider that served it, and its content,
so that any call can be rebuilt as a trajectory (and exported as ATIF-v1.7 for training). Needs `httpx`
(`pip install sandboxes[llm]`).

```python
from sandboxes.llm import LLMClient, load_llm_config, open_llm_session

cfg = load_llm_config("configs/llm/openrouter.yaml")
with open_llm_session("greenhouse", name="r-20261102-NA1", root=cfg.recording.sessions_dir):
    client = LLMClient(cfg)
    r = client.chat(messages, model="openai/gpt-6-luna", tools=tools, reasoning_effort="medium",
                    tags={"stage": "evolve", "task_id": "t-0034"},
                    trajectory={"id": "t-0034-u1x3-review2", "parent": "t-0034-u1x3-turn1", "kind": "evolve.fidelity"})
    r.message, r.usage, r.usd, r.record
    client.embed(["text"], model="openai/text-embedding-3-small")
```

## Config: `llm_providers`

Shaped like `sandbox_providers`: a path, a dict, or `{base: path, overrides: {...}}` (a key naming a provider overrides that
provider). `configs/llm/openrouter.yaml` is the shared base.

```yaml
llm_providers:
  - name: openrouter                 # recorded on every call
    provider_type: openrouter        # openrouter | openai_compatible
    base_url: https://openrouter.ai/api/v1
    api_key_env: [OPENROUTER_API_KEYS, OPENROUTER_API_KEY]   # a pool, round-robin
    keys_file: /ansh/data_env/openrouter_keys.txt
    models: ["*"]                    # model ids it serves (globs); the first matching provider serves a call
    routing: {order: [OpenAI], allow_fallbacks: false}
    pricing: {source: openrouter, refresh_s: 60, max_age_s: 3600, verify_generations: 0.05}
    timeout_s: 300
    retries: 8
  - name: local-sglang
    provider_type: openai_compatible
    base_url: http://gpu-box:30000/v1
    api_key_env: [LOCAL_LLM_API_KEY]   # optional: "EMPTY" when unset
    models: ["local/*"]
    model_map: {local/qwen3-coder: Qwen/Qwen3-Coder-30B-A3B-Instruct}
    pricing: {source: static, prompt_per_m: 0.02, cached_per_m: 0.0, completion_per_m: 0.08}
    capabilities: {reasoning_effort: false}
    extra_body: {chat_template_kwargs: {enable_thinking: true}}
recording:
  content: true                      # false: counts only (no messages, answers, reasoning or tool schemas)
  sessions_dir: null                 # where open_llm_session puts sessions when given this as root
```

| | `openrouter` | `openai_compatible` |
|---|---|---|
| request | `provider` routing, `reasoning: {effort}` | `reasoning_effort` only with the `reasoning_effort` capability; `extra_body`; a param the server cannot take is not sent (one warning, and `params.dropped` on the call) |
| reasoning read from | `reasoning`, `reasoning_details` | `reasoning_content` |
| who served it | `upstream` = the answer's `provider` | the config provider |
| cost | `usage.cost` (billed); `list_usd` from OpenRouter's prices | the static rates, or none |

## A session

`<root or ~/.cache/sandboxes>/llm_sessions/<client>/<session id>/`; sessions outside the cache are listed in
`~/.cache/sandboxes/llm_sessions/index.jsonl`. `open_llm_session` exports `SANDBOX_LLM_SESSION_DIR`, so worker processes
record into the same session (each process links its calls to what was recorded when it first wrote).

| file | one row per |
|---|---|
| `session.json` | the session: client, id, name, started, ended, pid, host, tags, and `config` (the providers, without key sources) |
| `calls.jsonl` | call (a retried call is one row with `attempts` > 1; a call that failed for good is a row with `status: error`) |
| `messages.jsonl` | distinct message, by the hash of what a prompt carries (role, content, tool calls, tool call id, name) |
| `tools.jsonl` | distinct set of tool schemas |
| `prices.jsonl` | price version a call was charged at (a price change is a new version) |
| `generations.jsonl` | sampled call's OpenRouter generation record (exact provider, native token counts, total cost) |

### `calls.jsonl` fields

| group | fields |
|---|---|
| identity | `v`, `call_id`, `kind` (chat, embedding, request), `pid`, `t_start`, `t_end`, `latency_s` |
| context | `tags` (the caller's: stage, task_id ...), `trajectory` {`id`, `parent`, `kind`, `step`} |
| routing | `provider` {`name`, `type`, `host`}, `upstream`, `model_requested`, `model_sent` (after `model_map`), `model_served`, `generation_id`, `key_index` |
| request | `params` {`max_tokens`, `temperature`, `reasoning_effort`, `tool_choice`, `response_format`, `routing`, `extra_body_keys`, `dropped`}, `tools` (hash), `n_tools` |
| outcome | `status`, `error` {`kind`, `http_status`, `message`}, `attempts`, `retry_reasons`, `finish_reason`, `native_finish_reason`, `tool_calls` (names) |
| tokens | `tokens` {`prompt`, `cached`, `cache_write`, `uncached`, `completion`, `reasoning`, `total`} (null where not reported), `tokens_reported` |
| characters | `chars` {`prompt`, `prompt_new`, `by_role`, `new_by_source` (role or `tool:<name>`), `content`, `reasoning`, `tool_args`}, `n_messages`, `n_new_messages` |
| cost | `usd`, `usd_source` (api, list, static, none), `list_usd`, `upstream_usd`, `usage_extra` |
| price | `price` {`source`, `model`, `upstream`, `endpoint_tag`, `endpoint_match`, `prompt_per_m`, `cached_per_m`, `cache_write_per_m`, `completion_per_m`, `reasoning_per_m`, `request_usd`, `discount`, `tier`, `fetched_at`, `age_s`, `stale`, `version`} |
| path | `path` {`prompt` (message hashes), `response`, `prefix`, `chain`, `parent_call`, `shared_len`, `new_from`, `branch_of`, `rewritten_at`} |

**Prices.** OpenRouter's per-provider prices (GET `/models/<model>/endpoints`) for the models in use are refreshed every
`refresh_s` in the background; a call never waits except the first of a model, and records the rates it was charged at and
their age (`stale` past `max_age_s`). One provider can serve a model at several prices (OpenAI standard and flex), and the
answer names only the provider, so the endpoint is the one whose rates give the billed cost (`endpoint_match`: exact,
ambiguous, closest, only, unbilled).

**Reasoning.** Kept on the answer's message row when the model returned some (`reasoning`, plus OpenRouter's raw
`reasoning_details`); absent otherwise. A loop that resends the answer without it still matches its row, since a message's
hash leaves reasoning out.

## Trajectories

A caller names trajectories (`trajectory={"id", "parent", "kind"}`); the session also links calls by content. A prompt is
a list of message hashes, and its chain hash (node i = H(node i-1, hash i)) names that exact path: a call's `parent_call`
is the earlier call whose prompt + answer is the longest prefix of its prompt. So each step links to the one before it, a
retry of the same request shows as `branch_of` the trajectory's latest call, a resumed session continues the original, and
a history the loop rewrote shows `rewritten_at` (the first message that differs). `prompt_new` and `new_by_source` count
only what a call added to its parent's context: where prompts grow.

**ATIF-v1.7** (`sandboxes llm trajectory REF ID --format atif`, `sandboxes.llm.atif.to_atif`): the trajectory's last
successful call's messages as steps; an assistant message carries its reasoning (when the model gave some), tool calls, the
tool results after it as its observation, and the metrics of the call that produced it. An assistant message no call of the
trajectory produced is `is_copied_context` (left out of training data). Token ids and logprobs are not recorded yet: a
self-hosted server can return them, which RL would need.

## CLI

```
sandboxes llm sessions [--client greenhouse]
sandboxes llm cost REF [--by model,stage]
sandboxes llm calls REF [--stage S] [--trajectory T]
sandboxes llm trajectories REF [--kind K]
sandboxes llm trajectory REF ID [--format openai|sharegpt|atif] [--out F] [--tree]
sandboxes llm growth REF [--kind K] [--stage S]
sandboxes llm pricing MODEL
```
