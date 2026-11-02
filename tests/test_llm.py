"""sandboxes.llm: configs, prices, the recorded client over OpenRouter and OpenAI-compatible servers, trajectories and the
CLI. Offline: an httpx.MockTransport answers every request, and prices come from a canned endpoints listing."""
import json
import threading
import time

import httpx
import pytest

from sandboxes.llm import LLMClient, LLMRequestError, OpenRouterPrices, load_llm_config, open_llm_session
from sandboxes.llm import cli as llm_cli
from sandboxes.llm import pricing, session as llm_session
from sandboxes.llm.atif import to_atif
from sandboxes.llm.pricing import parse_endpoints

BASE = "https://openrouter.ai/api/v1"
# OpenRouter's listing for a model served by OpenAI at two prices (standard and flex) and by Azure (as of 2026-11-02).
ENDPOINTS = {"data": {"id": "openai/gpt-6-luna", "endpoints": [
    {"provider_name": "OpenAI", "tag": "openai/flex", "pricing": {
        "prompt": "0.00000005", "completion": "0.00000025", "input_cache_read": "0.000000005",
        "input_cache_write": "0.0000000625", "discount": 0,
        "overrides": [{"min_prompt_tokens": 272000, "prompt": "0.0000001", "completion": "0.000000375"}]}},
    {"provider_name": "OpenAI", "tag": "openai", "pricing": {
        "prompt": "0.0000001", "completion": "0.0000005", "input_cache_read": "0.00000001",
        "input_cache_write": "0.000000125", "discount": 0}},
    {"provider_name": "Azure", "tag": "azure", "pricing": {"prompt": "0.0000001", "completion": "0.0000005",
                                                           "input_cache_read": "0.00000001"}}]}}
CONFIG = {"llm_providers": [
    {"name": "openrouter", "provider_type": "openrouter", "base_url": BASE, "api_key_env": ["TEST_OR_KEYS"],
     "models": ["openai/*"], "routing": {"order": ["OpenAI"], "allow_fallbacks": False},
     "pricing": {"source": "openrouter", "refresh_s": 0, "verify_generations": 0.0}, "retries": 3},
    {"name": "local", "provider_type": "openai_compatible", "base_url": "http://gpu-box:30000/v1", "models": ["local/*"],
     "model_map": {"local/qwen": "Qwen/Qwen3-Coder"}, "extra_body": {"chat_template_kwargs": {"enable_thinking": True}},
     "pricing": {"source": "static", "prompt_per_m": 0.02, "completion_per_m": 0.08}, "retries": 2}]}


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("SANDBOX_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("TEST_OR_KEYS", "k1,k2")
    monkeypatch.delenv(llm_session.LLM_SESSION_ENV, raising=False)
    monkeypatch.setattr(llm_session, "_CURRENT", {})
    monkeypatch.setattr(pricing, "_REGISTRY", {})
    return tmp_path


class Server:
    """Answers chat requests from a queue of (status, body) and records every request it got."""

    def __init__(self, replies):
        self.replies = list(replies)
        self.requests = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        status, body = self.replies.pop(0)
        return httpx.Response(status, json=body)

    def payload(self, i=-1) -> dict:
        return json.loads(self.requests[i].content)


def answer(content="ok", *, prompt=1000, cached=800, completion=50, reasoning=20, cost=None, provider="OpenAI",
           tool_calls=None, reasoning_text="thinking", field="reasoning"):
    usage = {"prompt_tokens": prompt, "completion_tokens": completion,
             "prompt_tokens_details": {"cached_tokens": cached}, "completion_tokens_details": {"reasoning_tokens": reasoning}}
    if cost is not None:
        usage["cost"] = cost
    message = {"role": "assistant", "content": content, field: reasoning_text}
    if tool_calls:
        message["tool_calls"] = tool_calls
    return {"id": "gen-1", "provider": provider, "model": "openai/gpt-6-luna-20260922", "usage": usage,
            "choices": [{"message": message, "finish_reason": "tool_calls" if tool_calls else "stop"}]}


def get_client(server, session=None, **kw):
    prices = OpenRouterPrices(BASE, refresh_s=0, fetch=lambda model: ENDPOINTS)
    return LLMClient(load_llm_config(CONFIG), session=session, transport=httpx.MockTransport(server), prices=prices,
                     sleep=lambda s: None, log_retries=False, **kw)


def test_a_config_resolves_bases_overrides_and_the_provider_of_a_model(tmp_path):
    (tmp_path / "base.yaml").write_text(json.dumps(CONFIG))
    (tmp_path / "run.yaml").write_text(json.dumps({"base": "base.yaml",
                                                   "overrides": {"openrouter": {"routing": {"order": ["Azure"]}},
                                                                 "recording": {"content": False}}}))
    cfg = load_llm_config(tmp_path / "run.yaml", overrides={"local": {"base_url": "http://other:8000/v1"}})
    assert cfg.get_provider("openai/gpt-6-luna").routing.order == ["Azure"]
    assert cfg.get_provider("local/qwen").base_url == "http://other:8000/v1"
    assert cfg.recording.content is False
    with pytest.raises(Exception, match="no enabled llm provider serves model 'anthropic/x'"):
        cfg.get_provider("anthropic/x")
    shipped = load_llm_config({"base": "sandboxes:openrouter", "overrides": {"openrouter": {"retries": 2}}})
    assert shipped.get_provider("any/model").retries == 2 and load_llm_config("sandboxes:openrouter").llm_providers
    catch_all = load_llm_config(CONFIG, overrides={"openrouter": {"models": ["*"]}})     # listed first, but less specific
    assert catch_all.get_provider("local/qwen").name == "local"
    assert catch_all.get_provider("anthropic/x").name == "openrouter"
    assert "api_key_env" not in cfg.llm_providers[0].to_public()           # a session never records where keys come from


def test_prices_have_tiers_and_the_endpoint_is_inferred_from_the_billed_cost():
    prices = OpenRouterPrices(BASE, refresh_s=0, fetch=lambda model: ENDPOINTS)
    flex = parse_endpoints("m", ENDPOINTS, 0)[0]
    tokens = {"prompt": 1000, "cached": 800, "completion": 50}
    # flex: 200 uncached x 0.05 + 800 cached x 0.005 + 50 x 0.25, per million
    assert flex.usd(tokens) == pytest.approx((200 * 0.05 + 800 * 0.005 + 50 * 0.25) / 1e6)
    assert flex.get_rates(300_000)[1] == ">=272000" and flex.get_rates(1000)[1] == "base"
    assert prices.choose("openai/gpt-6-luna", "OpenAI", tokens, flex.usd(tokens))[0].endpoint_tag == "openai/flex"
    standard = prices.choose("openai/gpt-6-luna", "OpenAI", tokens, 2 * flex.usd(tokens))
    assert (standard[0].endpoint_tag, standard[1]) == ("openai", "exact")
    assert prices.choose("openai/gpt-6-luna", "Azure", tokens, None)[1] == "only"
    assert prices.choose("openai/gpt-6-luna", "OpenAI", tokens, None)[1] == "unbilled"
    assert flex.version != parse_endpoints("m", ENDPOINTS, 0)[1].version


def test_a_failed_price_refresh_keeps_the_last_prices():
    calls = []

    def fetch(model):
        calls.append(model)
        if len(calls) > 1:
            raise OSError("down")
        return ENDPOINTS

    prices = OpenRouterPrices(BASE, refresh_s=0, fetch=fetch)
    assert len(prices.get("openai/gpt-6-luna")) == 3
    assert prices.refresh("openai/gpt-6-luna") is False and len(prices.get("openai/gpt-6-luna")) == 3


def test_an_openrouter_call_records_tokens_cost_price_provider_and_content(env):
    server = Server([(200, answer(cost=0.0000265))])
    with open_llm_session("greenhouse", "r-1") as s:
        r = get_client(server).chat([{"role": "system", "content": "be brief"}, {"role": "user", "content": "hi"}],
                                    model="openai/gpt-6-luna", reasoning_effort="medium", max_tokens=99,
                                    tags={"stage": "evolve", "task_id": "t-1"}, trajectory={"id": "T1", "kind": "fidelity"})
    sent = server.payload()
    assert sent["provider"] == {"order": ["OpenAI"], "allow_fallbacks": False} and sent["reasoning"] == {"effort": "medium"}
    assert server.requests[0].headers["authorization"] == "Bearer k1"
    assert r.usage == {"prompt_tokens": 1000, "completion_tokens": 50, "cached_tokens": 800, "cache_write_tokens": 0,
                       "reasoning_tokens": 20}
    [c] = s.get_calls()
    assert (c["status"], c["upstream"], c["model_served"], c["usd"], c["usd_source"]) == (
        "ok", "OpenAI", "openai/gpt-6-luna-20260922", 0.0000265, "api")
    assert c["price"]["endpoint_tag"] == "openai/flex" and c["price"]["endpoint_match"] == "exact"
    assert c["price"]["prompt_per_m"] == pytest.approx(0.05) and c["list_usd"] == pytest.approx(0.0000265)
    assert c["tokens"]["uncached"] == 200 and c["tokens_reported"] == ["prompt", "completion", "cached", "reasoning"]
    assert c["chars"]["prompt"] == len("be brief") + len("hi") and c["chars"]["reasoning"] == len("thinking")
    assert c["trajectory"] == {"id": "T1", "kind": "fidelity", "step": 0} and c["tags"]["stage"] == "evolve"
    messages = s.get_messages()
    assert len(messages) == 3 and messages[c["path"]["response"]]["reasoning"] == "thinking"
    assert [row["version"] for row in s._read("prices")] == [c["price"]["version"]]
    assert json.loads(s.meta_path.read_text())["config"]["llm_providers"][0]["name"] == "openrouter"


def test_trajectory_steps_link_by_path_retries_branch_and_rewrites_show(env):
    call = {"id": "c1", "type": "function", "function": {"name": "read_file", "arguments": '{"path": "a.py"}'}}
    server = Server([(200, answer("", tool_calls=[call])), (200, answer("done")), (200, answer("done again")),
                     (200, answer("after rewrite"))])
    s = open_llm_session("greenhouse", "r-1")
    client = get_client(server, session=s)
    t = {"id": "T", "kind": "implement"}
    m = [{"role": "system", "content": "sys"}, {"role": "user", "content": "task"}]
    first = client.chat(m, model="openai/gpt-6-luna", trajectory=t)
    m2 = m + [first.message, {"role": "tool", "tool_call_id": "c1", "content": "x" * 500}]
    client.chat(m2, model="openai/gpt-6-luna", trajectory=t)
    client.chat(m2, model="openai/gpt-6-luna", trajectory=t)                         # the same request again: a retry
    client.chat([m[0], {"role": "user", "content": "task, shortened"}], model="openai/gpt-6-luna", trajectory=t)
    c1, c2, c3, c4 = s.get_calls()
    assert c2["path"]["parent_call"] == c1["call_id"] and c2["path"]["shared_len"] == 3 and c2["trajectory"]["step"] == 1
    assert c2["chars"]["new_by_source"] == {"tool:read_file": 500}
    assert c3["path"]["branch_of"] == c2["call_id"] and c3["path"]["parent_call"] == c1["call_id"]
    assert c4["path"]["rewritten_at"] == 1 and c4["path"]["parent_call"] is None
    rebuilt = s.get_trajectory_messages("T")
    assert [x["content"] for x in rebuilt["messages"]] == ["sys", "task, shortened", "after rewrite"]
    tr = s.get_trajectories()["T"]
    assert (tr["steps"], tr["branches"], tr["rewrites"]) == (4, 1, 1)
    assert s.get_growth()["tool:read_file"] == 1000


def test_a_resent_answer_still_links_when_the_loop_normalizes_it(env):
    call = {"id": "c1", "type": "function", "function": {"name": "add", "arguments": '{"b": 2, "a": 1}'}}
    server = Server([(200, answer(None, tool_calls=[call])), (200, answer("3"))])
    s = open_llm_session("greenhouse", "r-1")
    client = get_client(server, session=s)
    m = [{"role": "user", "content": "add"}]
    client.chat(m, model="openai/gpt-6-luna", trajectory={"id": "T"})
    resent = {"role": "assistant", "content": "", "tool_calls": [            # how agent loops put the answer back
        {"type": "function", "index": 0, "id": "c1", "function": {"name": "add", "arguments": '{"a":1,"b":2}'}}]}
    client.chat(m + [resent, {"role": "tool", "tool_call_id": "c1", "content": "3"}], model="openai/gpt-6-luna",
                trajectory={"id": "T"})
    first, second = s.get_calls()
    assert second["path"]["parent_call"] == first["call_id"] and "rewritten_at" not in second["path"]
    assert s.get_messages()[first["path"]["response"]]["content"] is None          # stored as first seen


def test_recording_that_fails_never_fails_or_repeats_a_call(env, monkeypatch):
    s = open_llm_session("greenhouse", "r-1")
    monkeypatch.setattr(s, "_append", lambda name, row: (_ for _ in ()).throw(OSError(24, "Too many open files")))
    server = Server([(200, answer(cost=0.00002))])
    client = get_client(server, session=s)
    r = client.chat([{"role": "user", "content": "q"}], model="openai/gpt-6-luna", trajectory={"id": "T"})
    assert r.usage["prompt_tokens"] == 1000 and r.usd == 0.00002 and len(server.requests) == 1
    assert client.recording_errors["messages"] == 1 and client.recording_errors["call"] == 1
    failing = Server([(500, {"error": "boom"})] * 3)
    with pytest.raises(LLMRequestError):                      # the provider's error, not the recording's
        get_client(failing, session=s).chat([{"role": "user", "content": "q"}], model="openai/gpt-6-luna")


def test_session_metadata_survives_concurrent_writers(env):
    s = open_llm_session("greenhouse", "r-1")
    errors = []

    def write():
        for _ in range(200):
            try:
                s.write_meta({"llm_providers": []})
            except OSError as e:
                errors.append(e)

    threads = [threading.Thread(target=write) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == [] and json.loads(s.meta_path.read_text())["config"] == {"llm_providers": []}


def test_a_models_first_price_fetch_runs_once_for_concurrent_callers():
    fetches = []

    def slow_fetch(model):
        fetches.append(model)
        time.sleep(0.2)
        return ENDPOINTS

    prices = OpenRouterPrices(BASE, refresh_s=0, fetch=slow_fetch)
    results = []
    threads = [threading.Thread(target=lambda: results.append(len(prices.get("openai/gpt-6-luna")))) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert fetches == ["openai/gpt-6-luna"] and results == [3] * 8


def test_an_answer_the_loop_edited_keeps_its_call_in_the_atif_export(env):
    call = {"id": "c1", "type": "function", "function": {"name": "read", "arguments": '{"p": 1}'}}
    server = Server([(200, answer(None, tool_calls=[call, dict(call, id="c2")], reasoning_text="read it twice")),
                     (200, answer("done", reasoning_text=None))])
    s = open_llm_session("greenhouse", "r-1")
    client = get_client(server, session=s)
    m = [{"role": "user", "content": "go"}]
    client.chat(m, model="openai/gpt-6-luna", trajectory={"id": "T"})
    deduped = {"role": "assistant", "content": "", "tool_calls": [call]}                 # the loop dropped the repeat
    client.chat(m + [deduped, {"role": "tool", "tool_call_id": "c1", "content": "x"}], model="openai/gpt-6-luna",
                trajectory={"id": "T"})
    agent = [st for st in to_atif(s, "T")["steps"] if st["source"] == "agent"][0]
    assert agent.get("is_copied_context") is None and agent["llm_call_count"] == 1
    assert agent["extra"] == {"edited_by_loop": True} and agent["reasoning_content"] == "read it twice"
    assert agent["metrics"]["prompt_tokens"] == 1000


def test_a_process_that_inherits_a_session_links_to_the_calls_already_recorded(env):
    server = Server([(200, answer("one")), (200, answer("two"))])
    s = open_llm_session("greenhouse", "r-1")
    m = [{"role": "user", "content": "q"}]
    r = get_client(server, session=s).chat(m, model="openai/gpt-6-luna", trajectory={"id": "T"})
    child = llm_session.load_llm_session(s.dir)                           # what a worker process reads from the directory
    get_client(server, session=child).chat(m + [r.message, {"role": "user", "content": "more"}], model="openai/gpt-6-luna",
                                           trajectory={"id": "T"})
    first, second = s.get_calls()
    assert second["path"]["parent_call"] == first["call_id"] and second["trajectory"]["step"] == 1


def test_rate_limits_are_retried_and_a_call_that_fails_for_good_is_recorded(env):
    server = Server([(429, {"error": "slow down"}), (200, {"error": {"code": 502, "message": "upstream"}}),
                     (200, answer())])
    s = open_llm_session("greenhouse", "r-1")
    get_client(server, session=s).chat([{"role": "user", "content": "q"}], model="openai/gpt-6-luna")
    assert [r.headers["authorization"] for r in server.requests] == ["Bearer k1", "Bearer k2", "Bearer k1"]
    ok = s.get_calls()[0]
    assert (ok["attempts"], ok["retry_reasons"]) == (3, ["429", "502"])
    failing = Server([(500, {"error": "boom"})] * 3)
    with pytest.raises(LLMRequestError, match="after 3 attempts"):
        get_client(failing, session=s).chat([{"role": "user", "content": "q"}], model="openai/gpt-6-luna")
    bad = s.get_calls()[-1]
    assert bad["status"] == "error" and bad["attempts"] == 3 and bad["error"]["http_status"] == 500
    refused = Server([(400, {"error": "bad request"})])
    with pytest.raises(LLMRequestError) as e:
        get_client(refused, session=s).chat([{"role": "user", "content": "q"}], model="openai/gpt-6-luna")
    assert e.value.status == 400 and not e.value.retryable and len(refused.requests) == 1


def test_an_openai_compatible_server_gets_its_own_request_shape_and_static_price(env):
    server = Server([(200, answer(cost=None, field="reasoning_content", reasoning_text="local thoughts"))])
    s = open_llm_session("greenhouse", "r-1")
    get_client(server, session=s).chat([{"role": "user", "content": "q"}], model="local/qwen", reasoning_effort="high",
                                       routing={"order": ["X"]})
    sent = server.payload()
    assert str(server.requests[0].url) == "http://gpu-box:30000/v1/chat/completions"
    assert sent["model"] == "Qwen/Qwen3-Coder" and sent["chat_template_kwargs"] == {"enable_thinking": True}
    assert "provider" not in sent and "reasoning" not in sent and "reasoning_effort" not in sent
    assert server.requests[0].headers["authorization"] == "Bearer EMPTY"
    [c] = s.get_calls()
    assert c["usd_source"] == "static" and c["upstream"] is None and c["params"]["dropped"] == ["reasoning_effort", "routing"]
    assert c["usd"] == pytest.approx((200 * 0.02 + 800 * 0.02 + 50 * 0.08) / 1e6)       # cached at the prompt rate
    assert s.get_messages()[c["path"]["response"]]["reasoning"] == "local thoughts"


def test_counts_only_recording_keeps_no_content(env):
    cfg = {**CONFIG, "recording": {"content": False}}
    s = open_llm_session("greenhouse", "r-1")
    client = LLMClient(load_llm_config(cfg), session=s, transport=httpx.MockTransport(Server([(200, answer())])),
                       prices=OpenRouterPrices(BASE, refresh_s=0, fetch=lambda m: ENDPOINTS), log_retries=False)
    client.chat([{"role": "user", "content": "secret"}], model="openai/gpt-6-luna")
    assert all("content" not in row for row in s.get_messages().values())
    assert s.get_calls()[0]["chars"]["prompt"] == len("secret")


def test_embeddings_are_recorded(env):
    server = Server([(200, {"data": [{"index": 1, "embedding": [2.0]}, {"index": 0, "embedding": [1.0]}],
                            "usage": {"prompt_tokens": 7, "cost": 0.000001}})])
    s = open_llm_session("greenhouse", "r-1")
    assert get_client(server, session=s).embed(["a", "bb"], model="openai/text-embedding-3-small") == [[1.0], [2.0]]
    [c] = s.get_calls()
    assert (c["kind"], c["tokens"]["prompt"], c["usd"], c["chars"]["prompt"]) == ("embedding", 7, 0.000001, 3)


def test_a_trajectory_exports_as_atif_with_reasoning_only_where_the_model_gave_it(env):
    call = {"id": "c1", "type": "function", "function": {"name": "read_file", "arguments": '{"path": "a.py"}'}}
    server = Server([(200, answer("", tool_calls=[call], reasoning_text="look at a.py first")),
                     (200, answer("done", reasoning_text=None, cost=0.00002))])
    s = open_llm_session("greenhouse", "r-1", tags={"commit": "abc123"})
    client = get_client(server, session=s)
    carried = {"role": "assistant", "content": "notes from an earlier session"}          # not produced in this trajectory
    m = [{"role": "system", "content": "sys"}, {"role": "user", "content": "task"}, carried,
         {"role": "user", "content": "go on"}]
    first = client.chat(m, model="openai/gpt-6-luna", reasoning_effort="medium", trajectory={"id": "T", "kind": "implement"})
    client.chat(m + [first.message, {"role": "tool", "tool_call_id": "c1", "content": "print(1)"}], model="openai/gpt-6-luna",
                trajectory={"id": "T", "kind": "implement"})
    doc = to_atif(s, "T")
    assert doc["schema_version"] == "ATIF-v1.7" and doc["agent"]["version"] == "abc123"
    assert [st["source"] for st in doc["steps"]] == ["system", "user", "agent", "user", "agent", "agent"]
    copied, acting, final = doc["steps"][2], doc["steps"][4], doc["steps"][5]
    assert copied["is_copied_context"] is True and copied["llm_call_count"] == 0
    assert acting["reasoning_content"] == "look at a.py first" and acting["reasoning_effort"] == "medium"
    assert acting["tool_calls"] == [{"tool_call_id": "c1", "function_name": "read_file", "arguments": {"path": "a.py"}}]
    assert acting["observation"] == {"results": [{"source_call_id": "c1", "content": "print(1)"}]}
    assert acting["metrics"]["prompt_tokens"] == 1000 and acting["metrics"]["cached_tokens"] == 800
    assert "reasoning_content" not in final and final["metrics"]["cost_usd"] == 0.00002
    assert doc["final_metrics"]["total_prompt_tokens"] == 2000 and doc["final_metrics"]["total_steps"] == 6
    try:
        from harbor.models.trajectories import Trajectory
        Trajectory.model_validate(doc)                       # the producer's own schema, where the installed harbor has v1.7
    except ImportError:
        pass
    except Exception as e:  # noqa: BLE001 — an older harbor that does not know ATIF-v1.7 cannot judge it
        if "ATIF-v1.7" not in str(e):
            raise


def test_sessions_outside_the_cache_are_listed_and_the_cli_reads_them(env, tmp_path, capsys):
    root = tmp_path / "run" / "llm"
    s = open_llm_session("greenhouse", "r-9", root=root)
    get_client(Server([(200, answer(cost=0.00002))]), session=s).chat(
        [{"role": "user", "content": "q"}], model="openai/gpt-6-luna", tags={"stage": "gen"}, trajectory={"id": "T9"})
    assert s.dir.parent.parent == root and llm_session.get_llm_session("r-9").dir == s.dir
    for argv in (["sessions"], ["cost", "r-9", "--by", "model,stage"], ["calls", "r-9"], ["trajectories", "r-9"],
                 ["growth", "r-9"], ["trajectory", "r-9", "T9", "--tree"]):
        assert llm_cli.main(argv) == 0
    out = capsys.readouterr().out
    assert "r-9" in out and "openai/gpt-6-luna-20260922 / gen" in out and "user" in out
    assert llm_cli.main(["trajectory", "r-9", "T9", "--format", "sharegpt"]) == 0
    assert json.loads(capsys.readouterr().out)["conversations"][0] == {"from": "human", "value": "q"}
