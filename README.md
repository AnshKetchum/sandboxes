# sandboxes: standardized sandbox infra for agentic data generation codebases

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
1. docker
2. daytona

notes:
- no, this will assume that your logic only uses one sandbox provider at a time.