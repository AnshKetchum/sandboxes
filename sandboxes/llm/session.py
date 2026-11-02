"""LLM sessions: every call a client makes, recorded so its tokens, characters, cost and content can be read back, and any
call rebuilt as a trajectory.

    with open_llm_session("greenhouse", name="r-20261102-NA1", root="<run dir>/llm") as s:
        ...                                  # every LLMClient call in this process (and its children) is recorded in s
    s.summary(by=("model", "stage"))

A session is a directory, <root or cache>/llm_sessions/<client>/<session id>/ (sessions outside the cache are listed in
<cache>/llm_sessions/index.jsonl), holding

    session.json     {"client", "id", "name", "started", "ended", "pid", "host", "tags", "config"}
    calls.jsonl      one row per call (CALL FIELDS in docs/llm.md): ids, tags, trajectory, routing, request, outcome,
                     tokens, characters, cost and price, and its path (the prompt as message hashes, the response's hash)
    messages.jsonl   each distinct message once, by the hash of what a prompt carries (role, content, tool calls, tool call id,
                     name); an answer's reasoning is kept on its row
    tools.jsonl      each distinct set of tool schemas once
    prices.jsonl     each price version the session's calls were charged at
    generations.jsonl  OpenRouter's own record of the sampled calls (verify_generations)

Paths: a prompt is a list of message hashes; its chain hash (node i = H(node i-1, hash i)) names that exact path, so a
call's parent is the earlier call whose prompt + answer is the longest prefix of its prompt. That links each step of a
trajectory to the one before it, shows a retry as a sibling, a resumed session as continuing the original, and a rewritten
history (the loop truncating or replacing earlier messages) as where it diverges. Callers also name trajectories
explicitly (`trajectory={"id", "parent", "kind"}`); links are made within one process, and a process that inherits a session
(SANDBOX_LLM_SESSION_DIR) first reads the links already recorded.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import secrets
import socket
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional

from ..cost import cache_dir
from ..session import _slug

LLM_SESSION_ENV = "SANDBOX_LLM_SESSION_DIR"
HASH_CHARS = 20
CHARS_PER_TOKEN = 4.0


def llm_sessions_dir() -> Path:
    return cache_dir() / "llm_sessions"


# -- hashing and measuring messages ------------------------------------------------------------------------------------------
def get_prompt_view(message: dict) -> dict:
    """What of a message a prompt carries: role, content, tool calls (id, name, arguments), tool call id, name. Reasoning is
    left out, since loops resend an answer without it."""
    calls = [{"id": c.get("id"), "name": (c.get("function") or {}).get("name", c.get("name")),
              "arguments": (c.get("function") or {}).get("arguments", c.get("arguments"))}
             for c in message.get("tool_calls") or []]
    view = {"role": message.get("role"), "content": message.get("content")}
    if calls:
        view["tool_calls"] = calls
    for key in ("tool_call_id", "name"):
        if message.get(key) is not None:
            view[key] = message[key]
    return view


def get_canonical_arguments(arguments):
    """Tool call arguments as compact, key-sorted JSON when they are JSON (loops often re-serialize them), else as given."""
    try:
        return json.dumps(json.loads(arguments), sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return arguments


def get_message_hash(message: dict) -> str:
    """The hash of a message's prompt view, canonicalized the way loops change a message they resend: null content as
    "", tool call arguments re-serialized. The stored row keeps the message as first seen."""
    view = get_prompt_view(message)
    if view["content"] is None:
        view["content"] = ""
    for call in view.get("tool_calls") or []:
        call["arguments"] = get_canonical_arguments(call["arguments"])
    body = json.dumps(view, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()[:HASH_CHARS]


def get_chain(previous: str, message_hash: str) -> str:
    return hashlib.sha256(f"{previous}:{message_hash}".encode()).hexdigest()[:HASH_CHARS]


def get_chains(hashes: list[str]) -> list[str]:
    nodes, prev = [], ""
    for h in hashes:
        prev = get_chain(prev, h)
        nodes.append(prev)
    return nodes


def get_text(content) -> str:
    """A message content's text: the string, or the text parts of a multimodal list."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(str(part.get("text", "")) if isinstance(part, dict) else str(part) for part in content)
    return ""


def get_message_chars(message: dict) -> int:
    """Characters a message adds to a prompt: its text, and each tool call's name and arguments."""
    n = len(get_text(message.get("content")))
    for c in message.get("tool_calls") or []:
        fn = c.get("function") or c
        n += len(str(fn.get("name") or "")) + len(str(fn.get("arguments") or ""))
    return n


def get_source(message: dict, tool_names: dict) -> str:
    """Where a message comes from, for growth accounting: its role, or `tool:<name>` for a tool result."""
    if message.get("role") == "tool":
        return f"tool:{tool_names.get(message.get('tool_call_id'), message.get('name') or '?')}"
    return str(message.get("role"))


def get_tool_names(messages: list[dict]) -> dict:
    """tool_call_id -> tool name, from the assistant messages of a prompt."""
    names = {}
    for m in messages:
        for c in m.get("tool_calls") or []:
            names[c.get("id")] = (c.get("function") or {}).get("name", c.get("name"))
    return names


@dataclass
class LLMSession:
    client: str
    id: str
    name: str = ""
    dir: Path = field(default_factory=Path)
    started: float = 0.0
    ended: Optional[float] = None
    tags: dict = field(default_factory=dict)

    def __post_init__(self):
        self.lock = threading.Lock()
        self.loaded = False
        self.seen_messages: set[str] = set()
        self.seen_tools: set[str] = set()
        self.seen_prices: set[str] = set()
        self.chains: dict[str, str] = {}             # chain hash of a call's prompt + answer -> that call
        self.trajectories: dict[str, dict] = {}      # trajectory id -> {"steps", "last_call", "last_hashes"}

    # -- paths ------------------------------------------------------------------------------------------------------------
    @property
    def meta_path(self) -> Path:
        return self.dir / "session.json"

    def get_path(self, name: str) -> Path:
        return self.dir / f"{name}.jsonl"

    # -- writing ----------------------------------------------------------------------------------------------------------
    def _append(self, name: str, row: dict) -> None:
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        fd = os.open(self.get_path(name), os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            os.write(fd, line.encode())
        finally:
            os.close(fd)

    def _load_state(self) -> None:
        """The hashes and links already recorded (by this session's earlier process, or a parent process)."""
        if self.loaded:
            return
        self.loaded = True
        for name, seen, key in (("messages", self.seen_messages, "hash"), ("tools", self.seen_tools, "hash"),
                                ("prices", self.seen_prices, "version")):
            for row in self._read(name):
                seen.add(row.get(key))
        for row in self._read("calls"):
            self._take_call(row)

    def _take_call(self, row: dict) -> None:
        path = row.get("path") or {}
        if row.get("status") == "ok" and path.get("chain"):
            self.chains[path["chain"]] = row["call_id"]
        trajectory = (row.get("trajectory") or {}).get("id")
        if trajectory:
            t = self.trajectories.setdefault(trajectory, {"steps": 0, "last_call": None, "last_hashes": []})
            t["steps"] += 1
            if row.get("status") == "ok":
                t["last_call"] = row["call_id"]
                t["last_hashes"] = list(path.get("prompt") or []) + ([path["response"]] if path.get("response") else [])

    def add_messages(self, messages: list[dict], *, call_id: str, t: float, content: bool) -> list[str]:
        """Each message's hash, recording the ones this session has not seen."""
        hashes = []
        with self.lock:
            self._load_state()
            for m in messages:
                h = get_message_hash(m)
                hashes.append(h)
                if h not in self.seen_messages:
                    self.seen_messages.add(h)
                    self._append("messages", self._get_message_row(h, m, call_id, t, content))
        return hashes

    def add_response(self, message: dict, *, call_id: str, t: float, content: bool) -> str:
        """The answer's hash; its row (first time) keeps the reasoning that resent copies drop."""
        h = get_message_hash(message)
        with self.lock:
            self._load_state()
            if h not in self.seen_messages:
                self.seen_messages.add(h)
                row = self._get_message_row(h, message, call_id, t, content)
                reasoning = message.get("reasoning") or message.get("reasoning_content")
                if content:
                    if reasoning:
                        row["reasoning"] = reasoning
                    if message.get("reasoning_details"):
                        row["reasoning_details"] = message["reasoning_details"]
                row["reasoning_chars"] = len(get_text(reasoning))
                self._append("messages", row)
        return h

    @staticmethod
    def _get_message_row(h: str, m: dict, call_id: str, t: float, content: bool) -> dict:
        chars = get_message_chars(m)
        row = {"hash": h, "role": m.get("role"), "chars": chars, "est_tokens": round(chars / CHARS_PER_TOKEN),
               "first_call": call_id, "first_t": round(t, 3)}
        if content:
            row.update({k: v for k, v in get_prompt_view(m).items() if k != "role"})
        return row

    def add_tools(self, tools: Optional[list], *, content: bool) -> Optional[str]:
        if not tools:
            return None
        body = json.dumps(tools, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        h = hashlib.sha256(body.encode()).hexdigest()[:HASH_CHARS]
        with self.lock:
            self._load_state()
            if h not in self.seen_tools:
                self.seen_tools.add(h)
                row = {"hash": h, "n_tools": len(tools), "chars": len(body)}
                if content:
                    row["tools"] = tools
                self._append("tools", row)
        return h

    def add_price(self, history_row: dict) -> None:
        with self.lock:
            self._load_state()
            if history_row["version"] not in self.seen_prices:
                self.seen_prices.add(history_row["version"])
                self._append("prices", {"t": round(time.time(), 3), **history_row})

    def get_links(self, hashes: list[str], trajectory: Optional[str]) -> dict:
        """A prompt's place among the session's paths: its chain hash, its parent call (whose prompt + answer is its longest
        prefix), how much it shares, its step in its trajectory, and, against that trajectory's latest call, whether it is a
        retry or fork of it (`branch_of`) or rewrote the history before it (`rewritten_at`)."""
        nodes = get_chains(hashes)
        with self.lock:
            self._load_state()
            parent, shared = None, 0
            for i in range(len(nodes) - 1, -1, -1):
                if nodes[i] in self.chains:
                    parent, shared = self.chains[nodes[i]], i + 1
                    break
            state = self.trajectories.get(trajectory) if trajectory else None
            links = {"prefix": nodes[-1] if nodes else "", "parent_call": parent, "shared_len": shared,
                     "new_from": shared, "step": state["steps"] if state else 0}
            if state and state["last_call"] and state["last_call"] != parent:
                last = state["last_hashes"]
                k = next((i for i, (a, b) in enumerate(zip(last, hashes)) if a != b), min(len(last), len(hashes)))
                if k < len(hashes) and k < len(last):
                    links["rewritten_at"] = k
                elif k == len(hashes):
                    links["branch_of"] = state["last_call"]
        return links

    def add_call(self, row: dict) -> None:
        with self.lock:
            self._load_state()
            self._take_call(row)
            self._append("calls", row)

    def add_generation(self, row: dict) -> None:
        with self.lock:
            self._append("generations", row)

    # -- reading ----------------------------------------------------------------------------------------------------------
    def _read(self, name: str) -> Iterator[dict]:
        path = self.get_path(name)
        if not path.is_file():
            return
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    yield json.loads(line)
                except ValueError:
                    continue

    def get_calls(self) -> list[dict]:
        return list(self._read("calls"))

    def get_messages(self) -> dict[str, dict]:
        return {row["hash"]: row for row in self._read("messages")}

    def get_tools(self) -> dict[str, dict]:
        return {row["hash"]: row for row in self._read("tools")}

    def summary(self, by: tuple = ("model",), calls: Optional[list[dict]] = None) -> dict:
        """Calls, tokens, characters, cost (by source) and latency, grouped by `by`: any of model, provider, upstream, kind,
        status, or a tag (stage, task_id ...). {} for an empty session."""
        groups: dict[tuple, dict] = {}
        for c in calls if calls is not None else self.get_calls():
            key = tuple(_get_field(c, name) for name in by)
            g = groups.setdefault(key, defaultdict(float))
            tokens, chars = c.get("tokens") or {}, c.get("chars") or {}
            g["calls"] += 1
            g["errors"] += c.get("status") != "ok"
            for k in ("prompt", "cached", "cache_write", "completion", "reasoning"):
                g[f"{k}_tokens"] += tokens.get(k) or 0
            g["prompt_chars"] += chars.get("prompt") or 0
            g["prompt_new_chars"] += chars.get("prompt_new") or 0
            g["usd"] += c.get("usd") or 0.0
            g[f"usd_{c.get('usd_source') or 'none'}"] += c.get("usd") or 0.0
            g["list_usd"] += c.get("list_usd") or 0.0
            g["latency_s"] += c.get("latency_s") or 0.0
        out = {}
        for key, g in groups.items():
            g = dict(g)
            g["cache_hit"] = round(g["cached_tokens"] / g["prompt_tokens"], 4) if g["prompt_tokens"] else 0.0
            g["mean_latency_s"] = round(g["latency_s"] / g["calls"], 2) if g["calls"] else 0.0
            out[" / ".join(str(k) for k in key)] = {k: round(v, 6) if isinstance(v, float) else v for k, v in g.items()}
        return out

    def get_trajectories(self, calls: Optional[list[dict]] = None) -> dict[str, dict]:
        """Each trajectory (by its id; calls without one by their root call, `auto:<call id>`) with its calls in order."""
        calls = calls if calls is not None else self.get_calls()
        by_id = {c["call_id"]: c for c in calls}
        out: dict[str, dict] = {}
        for c in calls:
            tid = (c.get("trajectory") or {}).get("id") or f"auto:{_get_root(c, by_id)}"
            t = out.setdefault(tid, {"id": tid, "kind": (c.get("trajectory") or {}).get("kind", ""),
                                     "parent": (c.get("trajectory") or {}).get("parent"), "tags": c.get("tags") or {},
                                     "calls": []})
            t["calls"].append(c)
        for t in out.values():
            cs = t["calls"]
            t["steps"] = len(cs)
            t["errors"] = sum(c.get("status") != "ok" for c in cs)
            t["branches"] = sum(bool((c.get("path") or {}).get("branch_of")) for c in cs)
            t["rewrites"] = sum((c.get("path") or {}).get("rewritten_at") is not None for c in cs)
            t["usd"] = round(sum(c.get("usd") or 0.0 for c in cs), 6)
            t["prompt_tokens"] = sum((c.get("tokens") or {}).get("prompt") or 0 for c in cs)
            t["max_prompt_tokens"] = max(((c.get("tokens") or {}).get("prompt") or 0 for c in cs), default=0)
            t["start"], t["end"] = cs[0].get("t_start"), cs[-1].get("t_end")
        return out

    def get_trajectory_messages(self, trajectory_id: str) -> dict:
        """One trajectory rebuilt: the messages of its last successful call's prompt and answer, with reasoning on the
        answers that had it, its tools, and its calls' ids. Raises KeyError when it has no successful call."""
        t = self.get_trajectories()[trajectory_id]
        ok = [c for c in t["calls"] if c.get("status") == "ok"]
        if not ok:
            raise KeyError(f"trajectory {trajectory_id} has no successful call")
        last = ok[-1]
        rows = self.get_messages()
        path = last.get("path") or {}
        messages = [_get_message(rows.get(h), h) for h in path.get("prompt") or []]
        if path.get("response"):
            messages.append(_get_message(rows.get(path["response"]), path["response"]))
        tools = self.get_tools().get(last.get("tools") or "", {}).get("tools")
        return {"id": trajectory_id, "kind": t["kind"], "tags": t["tags"], "model": last.get("model_served")
                or last.get("model_requested"), "messages": messages, "tools": tools,
                "calls": [c["call_id"] for c in t["calls"]]}

    def get_growth(self, calls: Optional[list[dict]] = None) -> dict[str, int]:
        """New prompt characters by where they came from (role or tool), summed over calls: what makes contexts grow."""
        out: dict[str, int] = defaultdict(int)
        for c in calls if calls is not None else self.get_calls():
            for source, n in ((c.get("chars") or {}).get("new_by_source") or {}).items():
                out[source] += n
        return dict(sorted(out.items(), key=lambda kv: -kv[1]))

    # -- lifecycle --------------------------------------------------------------------------------------------------------
    def close(self) -> None:
        self.ended = time.time()
        self.write_meta()

    def __enter__(self) -> "LLMSession":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
        if os.environ.get(LLM_SESSION_ENV) == str(self.dir):
            os.environ.pop(LLM_SESSION_ENV, None)

    def write_meta(self, config: Optional[dict] = None) -> None:
        meta = {"client": self.client, "id": self.id, "name": self.name, "started": self.started, "ended": self.ended,
                "pid": os.getpid(), "host": socket.gethostname(), "tags": self.tags}
        if config is not None:
            meta["config"] = config
        elif self.meta_path.is_file():
            try:
                meta["config"] = json.loads(self.meta_path.read_text()).get("config")
            except ValueError:
                pass
        tmp = self.meta_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(meta, indent=1))
        os.replace(tmp, self.meta_path)

    def to_dict(self) -> dict:
        return {"client": self.client, "id": self.id, "name": self.name, "dir": str(self.dir), "started": self.started,
                "ended": self.ended, "tags": self.tags}


def _get_field(call: dict, name: str):
    if name == "model":
        return call.get("model_served") or call.get("model_requested")
    if name == "provider":
        return (call.get("provider") or {}).get("name")
    if name == "kind":
        return (call.get("trajectory") or {}).get("kind") or call.get("kind")
    if name in call and not isinstance(call[name], (dict, list)):
        return call[name]
    return (call.get("tags") or {}).get(name)


def _get_root(call: dict, by_id: dict) -> str:
    seen = set()
    while (call.get("path") or {}).get("parent_call") in by_id and call["call_id"] not in seen:
        seen.add(call["call_id"])
        call = by_id[call["path"]["parent_call"]]
    return call["call_id"]


def _get_message(row: Optional[dict], h: str) -> dict:
    """A message as a chat API takes it, from its messages.jsonl row (a placeholder when content was not recorded)."""
    if row is None or "content" not in row:
        return {"role": (row or {}).get("role", "unknown"), "content": None, "missing": h}
    m = {"role": row["role"], "content": row.get("content")}
    if row.get("tool_calls"):
        m["tool_calls"] = [{"id": c["id"], "type": "function", "function": {"name": c["name"], "arguments": c["arguments"]}}
                           for c in row["tool_calls"]]
    for key in ("tool_call_id", "name", "reasoning", "reasoning_details"):
        if row.get(key) is not None:
            m[key] = row[key]
    return m


# -- opening and finding sessions ------------------------------------------------------------------------------------------
def open_llm_session(client: str, name: str = "", *, tags: Optional[dict] = None, root: Optional[os.PathLike] = None,
                     config: Optional[dict] = None, activate: bool = True) -> LLMSession:
    """A new session directory for `client` (under `root`, default the cache), made the active session of this process and
    the processes it starts unless `activate` is False. `config` (a provider list without keys) goes in session.json."""
    stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    sid = f"{_slug(name) + '-' if name else ''}{stamp}-{secrets.token_hex(2)}"
    d = Path(root or llm_sessions_dir()) / _slug(client) / sid
    d.mkdir(parents=True, exist_ok=True)
    s = LLMSession(client=_slug(client), id=sid, name=name, dir=d, started=time.time(), tags=dict(tags or {}))
    s.loaded = True
    s.write_meta(config)
    index = llm_sessions_dir() / "index.jsonl"
    index.parent.mkdir(parents=True, exist_ok=True)
    with open(index, "a") as f:
        f.write(json.dumps({**s.to_dict(), "ended": None}) + "\n")
    register_llm_session(s)
    if activate:
        os.environ[LLM_SESSION_ENV] = str(d)
    return s


def load_llm_session(d: os.PathLike) -> Optional[LLMSession]:
    d = Path(d)
    if not d.is_dir():
        return None
    try:
        m = json.loads((d / "session.json").read_text())
    except (OSError, ValueError):
        return LLMSession(client=d.parent.name, id=d.name, dir=d, started=d.stat().st_mtime)
    return LLMSession(client=m.get("client", d.parent.name), id=m.get("id", d.name), name=m.get("name", ""), dir=d,
                      started=m.get("started", 0.0), ended=m.get("ended"), tags=m.get("tags") or {})


_CURRENT: dict[str, LLMSession] = {}
_CURRENT_LOCK = threading.Lock()


def current_llm_session() -> Optional[LLMSession]:
    """The active session (SANDBOX_LLM_SESSION_DIR), one object per directory in this process."""
    d = os.environ.get(LLM_SESSION_ENV)
    if not d:
        return None
    with _CURRENT_LOCK:
        if d not in _CURRENT:
            s = load_llm_session(d)
            if s is None:
                return None
            _CURRENT[d] = s
        return _CURRENT[d]


def register_llm_session(s: LLMSession) -> None:
    """Make `s` the object current_llm_session() returns for its directory (open_llm_session's caller keeps writing to it)."""
    with _CURRENT_LOCK:
        _CURRENT[str(s.dir)] = s


@contextlib.contextmanager
def use_llm_session(s: LLMSession):
    prev = os.environ.get(LLM_SESSION_ENV)
    os.environ[LLM_SESSION_ENV] = str(s.dir)
    register_llm_session(s)
    try:
        yield s
    finally:
        if prev is None:
            os.environ.pop(LLM_SESSION_ENV, None)
        else:
            os.environ[LLM_SESSION_ENV] = prev


def list_llm_sessions(client: Optional[str] = None) -> list[LLMSession]:
    """Every session on this machine (the cache's, and those the index lists elsewhere), newest first."""
    dirs = set()
    root = llm_sessions_dir()
    if root.is_dir():
        for cdir in root.iterdir():
            if cdir.is_dir():
                dirs.update(d for d in cdir.iterdir() if d.is_dir())
    index = root / "index.jsonl"
    if index.is_file():
        for line in index.read_text().splitlines():
            try:
                dirs.add(Path(json.loads(line)["dir"]))
            except (ValueError, KeyError):
                continue
    out = [s for s in (load_llm_session(d) for d in dirs) if s and (not client or s.client == _slug(client))]
    return sorted(out, key=lambda s: s.started, reverse=True)


def get_llm_session(ref: str, client: Optional[str] = None) -> LLMSession:
    """By directory, exact id, name (the newest with it) or a unique id prefix."""
    if os.path.isdir(ref):
        s = load_llm_session(ref)
        if s:
            return s
    candidates = list_llm_sessions(client)
    for s in candidates:
        if s.id == ref:
            return s
    named = [s for s in candidates if s.name == ref]
    if named:
        return named[0]
    prefixed = [s for s in candidates if s.id.startswith(ref)]
    if len(prefixed) == 1:
        return prefixed[0]
    raise KeyError(f"no unique llm session matches {ref!r}")
