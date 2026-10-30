# sandboxes: standardized sandbox infra (docker, daytona, e2b, blaxel) for agentic data generation codebases

note: this is software vibecoded from scratch (ai slop). use at your own risk. it works for me.

challenges that led to me vibecoding this: 
    - switching between standard sandbox providers and docker is annoying
    - the execution logic is usually generic, but leveraged quite often for AI training env generation codebases
    - the optimal parallelism patterns are usually very similar, but we end up having to re-invent the wheel for each codebase

so this will serve as a useful primitive for codebases trying to use sandboxes. it will have an env toggle. 

```
    SANDBOX_PROVIDER=<docker/daytona> [run your python script]
```

which will trigger the sandboxes library to use the specific patterns. 


features
- a standardized API frontend for common sandbox ops you need for your infra to consume
- tests that guarentee that the API functions for that provider

to come soon after a v0:

- utilities to construct performant parallelism schemes to concurrently operate on sandbox fleets

supported providers:
1. docker (local)
2. daytona
3. e2b
4. blaxel

## several providers at once

List as many providers as you like; a provider is used only if its API key is in the environment and it is
not `enabled: false`:

| provider | env needed | cost model (list price) | limits seen |
|---|---|---|---|
| daytona | `DAYTONA_API_KEY` | $0.0504/vCPU-h + $0.0162/GiB-h + $0.000108/GiB-h disk (5 GiB free) | RAM quota (500 GiB: ~125 sandboxes at 4 GiB), creation hangs under load |
| e2b | `E2B_API_KEY` | $0.000014/vCPU-s + $0.0000045/GiB-s, storage free, Pro $150/mo | 100 concurrent (600/1100 add-ons: $650/$1150 per month) |
| blaxel | `BL_API_KEY`, `BL_WORKSPACE` | $0.0000115/GB-s (CPU included); snapshots $0.20, images $0.045, volumes $0.12 per GB-month | 200 concurrent (tier 2), 60 s per command call, RAM-backed root fs |

```yaml
# configs/multi.yaml — sandbox_providers: [daytona.yaml, e2b.yaml, blaxel.yaml]
sandbox_providers:
  base: /ansh/sandboxes/configs/multi.yaml
  overrides: {daytona: {peak_concurrent_sandboxes: 64}, e2b: {cpu: 2, memory_mb: 4096}}
```

* `get_sandbox_provider()` (sync, one sandbox per object) returns the first usable provider.
* `SandboxRouter.from_config(path)` (async) opens sandboxes on whichever usable provider has the most free
  capacity, fails over when one answers with a quota/429, and enforces `spend_cap_usd` per provider and
  `total_spend_cap_usd` overall. `require=("dind",)` keeps docker-in-docker work off providers that cannot host it.
* A new provider is one `RawSandbox` subclass plus `register_provider(ProviderInfo(name, env_keys, raw="module:Class"))`.
  The subclass is the whole contract; cost tracking and cost cutting are first class parts of it, not add-ons:

  | member | purpose |
  |---|---|
  | `provider`, `hard_lifetime` | its name; whether the provider ends a sandbox at its lifetime by itself (a missing declaration fails when the class is defined) |
  | `cost_model()` | what it bills for (default: `pricing.DEFAULT_MODELS[provider]`) |
  | `list_live()` (classmethod) | every sandbox of ours that exists now (on a shared account only ours) |
  | `kill(id)` (classmethod) | end one, quietly if already gone |
  | `stored_artifacts()` (classmethod, optional) | billed images / snapshots / volumes held for us |
  | `_create/_exec/_upload/_download/_delete` | the per-sandbox primitives |

  `fleet.py` is the generic side and knows no provider names: the uniform ledger (open / close / storage, with USD from the
  provider's cost model), spend caps (`guard.py`), the janitor, `reconcile()` (close entries whose sandbox is gone) and
  `accrue_storage()` all run through those members. `tests/test_provider_interface.py` checks every real provider against
  the contract and runs a fake provider through the generic flows, so a new provider is correct when those pass.

```python
router = SandboxRouter.from_config("/ansh/sandboxes/configs/multi-dind.yaml")
sbx = await router.open("registry/img:tag", require=("dind",), cpu=2, mem_gb=4, boot_cmd="dind-start.sh")
r = await sbx.exec("docker info"); await sbx.put_file("dir", "/work"); await sbx.get_file("/work", "out")
usd = await router.close(sbx)
```

## cost tracking

Every sandbox any provider opens writes `open`/`close` events (with its dollar cost from `pricing.py`) to one
JSONL ledger; storage (snapshots, images, volumes) and one-off charges are `storage` events. A sandbox whose
process was killed still counts as running until `SANDBOX_STALE_AFTER_S` (default: the 8 h lifetime ceiling).

```
SANDBOX_COST_LEDGER=/path/costs.jsonl   SANDBOX_RUN_ID=scale-200    # where / which run
python -m sandboxes.cost --run scale-200 --watch 30 --cap 120       # live table, exit 2 over the cap
```

`SANDBOX_CAP_DIVISOR=N` makes each of N data-parallel processes use 1/N of every provider's peak.

## measured comparison (2026-10-06, `scripts/benchmark_concurrency.py`, 2 vCPU / 4 GiB / 10 GB node, python:3.12-slim)

| provider | node $/h | concurrent ceiling seen | first command p50 (N=1 / 100 / max N) | CPU per sandbox as N grows | docker-in-docker |
|---|---|---|---|---|---|
| fly | 0.092 | ~190-260 ("organization machine limit", unpublished) | 7 s / 9 s / 11 s (200) | flat | yes: 5 GB loop file on the 8 GB disk |
| e2b | 0.166 | 100 (plan; 429 beyond) | 0.7 s / 4.5 s | flat | yes, 20 GB disk |
| blaxel | 0.166 | ~252 (HTTP 429 beyond; 200 earlier the same day) | 4 s / 7 s / 9 s (250) | falls 66% by N=250 | only with 16 GiB+ (RAM-backed disk) |
| daytona | 0.166 | RAM quota ~120 at 4 GiB (tested to 5 here) | 1 s / - | flat | yes, 10 GB disk |
| modal | 0.380 | 2000 of 2000 started (Team plan: 5,000) | 0.8 s / 1.1 s / 11 s (2000) | flat | yes with `runtime: vm` |

GKE (`name: gke`): one pod per sandbox on our own cluster's spot node pools (pods' storage on local NVMe where the family has
it); Google bills nodes, so a node meter puts the node bill in the ledger and a watchdog deletes what dead processes leave and
scales idle pools to zero. 2026-10-30, `scripts/gke/bench.py`: 800 of 800 at once for $23.6/h (~$0.03 per sandbox-hour,
seven machine families under today's quotas), warm start 0.54-0.64 s, exec ~0.2 s a call. Setup, quotas, cost model, deletion
layers, teardown and billing reconciliation: [docs/gke.md](docs/gke.md).

`python scripts/benchmark_concurrency.py --provider X --concurrencies 1 10 100` reproduces a row;
`python scripts/verify_costs.py` checks the cost model and the ledger against the published rates and, where the provider has an API,
its own billing (see below).

## one command set for every project

Projects that consume sandboxes (euphoria, petunia, greenhouse, pr_task_synthesis, anything new) hold NO provider logic: they
set their identity once (`sandboxes.set_context(owner="euphoria")`, optionally `run=`), open sandboxes through the library
(`SandboxRouter`, `harbor_adapter.make_env` for harbor tasks), and use the same commands as everyone else:

```
sandboxes ls    [--owner X] [--run R] [--older-than H]     what is alive, on every provider and local docker
sandboxes stop  --owner X | --run R | --older-than H | --all [--dry-run] [--batch N --pause S]
sandboxes reap  [--owner X]                                 delete broken sandboxes (error state, stuck creating)
sandboxes cost | guard | janitor | providers | audit
```

A project's own stop/clean command is a one-line wrapper (`sandboxes.cli.stop_for("greenhouse", run=...)`): `euphoria stop`,
`greenhouse clean`, `pr_task_synthesis/stop_lane.py` are all `sandboxes stop --owner <project>`. Every sandbox carries
`sandboxes.managed / sandboxes.owner / sandboxes.run` as labels, tags or metadata (whatever the provider calls them), applied once in
`RawSandbox.start`, the docker provider and the harbor adapter, so scoping needs no provider knowledge. Older markers
(`agentdist`, `euphoria.managed`, greenhouse's container name prefixes) are mapped to owners in `labels.py`.

**Enforcement:** `sandboxes audit PATH...` lists provider-specific code in a project (provider SDK imports, harbor provider
classes, provider credentials, `== "daytona"` style branches). `tests/test_fleet_commands.py` runs it over the consuming projects and
fails if any of them grows provider logic; a deliberate exception needs `# sandboxes-audit: ok (reason)` or a line in the
project's `.sandboxes-audit-ignore`.

## spend caps (env cost, enforced here, not in euphoria)

Spend is read from the cost ledger in the sandboxes cache dir (`~/.cache/sandboxes/costs.jsonl`: one `open`/`close` record
per sandbox with provider, size, run id and USD; running ones count at their cost so far). Every open path checks the caps
first (`RawSandbox.start`, `SandboxRouter.open`, the harbor Daytona provider, euphoria's `make_env`) and raises
`SpendCapExceeded` before anything is created. Defaults: $500 total. Change them without touching any project:

```
echo '{"total_usd": 800, "runs": {"final-e2b": 40}, "providers": {"daytona": 300}}' > ~/.cache/sandboxes/caps.json
SANDBOX_CAP_USD=300  SANDBOX_RUN_ID=my-run  SANDBOX_RUN_CAP_USD=50     # per process
python -m sandboxes.guard                      # caps, spend by run, burn rate, hours to the cap
python -m sandboxes.guard --watch 60 --kill    # also delete running sandboxes once a cap trips
```

A cap stops NEW sandboxes; `--kill` (or the janitor) ends running ones. `guard` and the janitor also reconcile the ledger:
an entry whose sandbox no longer exists on its provider is closed, so killed runs stop accruing phantom cost. Only
sandboxes opened through this library are in the ledger (the legacy agentdist Daytona backend is not).

## lifetime policy (no sandbox bills for ever)

Every sandbox gets a finite lifetime whatever the config says: `auto_delete_minutes` unset, 0 or negative becomes 240
(`SANDBOX_DEFAULT_LIFETIME_MIN`), and nothing may exceed 480 (`SANDBOX_MAX_LIFETIME_MIN`). E2B (`timeout`) and Blaxel
(`ttl`) end the sandbox at that age, hard; checked live with 3-minute sandboxes. Daytona only auto-stops after that many
minutes of inactivity, and harbor's own E2B environment lives 24 h, so `sandboxes.janitor` backs both up:

```
python -m sandboxes.janitor --dry-run                       # what is past 6 h
python -m sandboxes.janitor --max-age-hours 6 --watch 300   # keep deleting
```

It deletes only what this stack creates (daytona labels agentdist / sandboxes.managed / euphoria.managed, never other
projects' harbor.* ones; the E2B and Blaxel accounts). The pr_task_synthesis spend watchdog runs it every 10 minutes.

## checks

`pytest tests` runs offline; `pytest tests/test_providers_live.py -s` opens one real sandbox per provider
whose key is set (boots dockerd, builds and runs a container, round-trips a directory, runs a >60 s
command, checks the ledger).
## Sessions: one cost ledger per client

Every client of the library (euphoria, greenhouse, pr_task_synthesis, ...) opens a **session** and gets its own ledger instead of appending to one shared file:

```python
from sandboxes.session import open_session
with open_session("euphoria", "v3-b07") as s:       # ~/.cache/sandboxes/sessions/euphoria/<id>/costs.jsonl
    ...                                             # this process and the workers it spawns write to s's ledger
    print(s.cost())                                 # per-provider sandboxes, hours, $ of this session alone
```

- A launcher script can opt a process in with env vars alone: `SANDBOX_SESSION_CLIENT=euphoria SANDBOX_SESSION_NAME=v3-b07`.
- `python -m sandboxes sessions [--client euphoria]` lists sessions with their cost; `... sessions cost <id|name> [--json]` shows one.
- Spend caps are machine-wide and unchanged: they add up the legacy ledger and every session ledger (`cost.summary_all()`). Setting `SANDBOX_COST_LEDGER` still redirects everything to that one file (tests, experiments).
- `reconcile` and `close_dead_entries` visit every ledger. Euphoria writes `<state dir>/sessions.json` at launch; `euphoria export` reads it and adds `cost.sandbox_ledger` (real spend, per provider) next to its flat-rate `env_cost` estimate.
- Sandboxes opened before this change live in the legacy `~/.cache/sandboxes/costs.jsonl`; they cannot be attributed to a session after the fact.
