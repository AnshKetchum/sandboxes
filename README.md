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
* A new provider is one `RawSandbox` subclass (five primitives: create, exec, upload, download, delete) plus
  `register_provider(ProviderInfo(name, env_keys, raw="module:Class"))`.

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