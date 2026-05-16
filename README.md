# reckon-e2e

End-to-end torture / chaos suite for the reckon-db stack.

> **Status:** v0.1.0 — initial scaffold + `integrity_torture` axis runs against `mem-evoq`. The remaining axes (load, key-lifecycle, multi-node, adapter-swap) are scoped but not yet implemented. See `docs/TORTURE_TAXONOMY.md`.

## What this is

Each unit-test suite in the constituent repos (`reckon-db`, `reckon-gater`, `evoq`, `reckon-evoq`, `mem-evoq`, `reckon-gateway`) tests its own slice. None of them can test *the whole stack under stress* — that requires pulling in multiple deps together and running adversarial scenarios with realistic time budgets.

reckon-e2e is that test bed. It asks questions like:

- Does the integrity layer detect a tampering attempt that lands mid-stream while five writers are hammering the store?
- Do snapshot anchors survive concurrent writes to neighbouring streams?
- Under sustained 10k events/sec load for 5 minutes, does tail latency drift or hold?
- When a Khepri leader is killed mid-append, does the chain stay continuous after recovery?
- Does the same workload behave identically against `mem_evoq_adapter` and `reckon_evoq_adapter`?

These questions don't have natural homes in any single repo. They live here.

## What this is NOT

- **Not a microbench.** Throughput numbers fall out of the suites, but they're observational, not the goal. For micro-benchmarks of individual primitives, use `reckon-db/benchmarks/`.
- **Not a unit test runner.** Tests here take *minutes*, not milliseconds. CI runs them on push to main, not on every PR.
- **Not a fuzzer.** PropEr-style state-machine fuzzing lives inside each package (e.g. `mem-evoq/test/prop/`). Here we run *scripted* adversarial scenarios.

## Torture axes

Each axis is its own app under `apps/`. Vertical slicing — one axis = one capability = one directory.

| Axis | App | Status |
|------|-----|--------|
| Integrity tampering under load | `integrity_torture` | ✅ implemented |
| Sustained throughput / tail latency | `load_torture` | 📝 scoped |
| Key lifecycle (rotation, absence, invalidation) | `key_lifecycle_torture` | 📝 scoped |
| Khepri multi-node partition / leader-change | `multi_node_torture` | 📝 scoped |
| Adapter behavioural equivalence (mem-evoq vs reckon-evoq vs clustered gateway) | `adapter_swap_torture` | ✅ scenario 1 (local + gRPC facades) |

## Running

Full quality gate:

```bash
./scripts/run-all.sh
```

A single axis:

```bash
./scripts/run-suite.sh integrity_torture
```

A single scenario:

```bash
rebar3 ct --suite=apps/integrity_torture/test/integrity_torture_concurrent_SUITE
```

## Tested versions

Pinned in `rebar.config`. Bump deliberately, document in `CHANGELOG.md`.

| Package | Version |
|---|---|
| `mem_evoq` | 0.1.2 |
| `reckon_evoq` | 2.1.1 |
| `reckon_db` | 2.1.1 |
| `evoq` | 1.15.0 |
| `reckon_gater` | 2.1.0 |
| `grpcbox` | 0.17.1 |

## Why mem-evoq for integrity_torture (and not reckon-db)?

The integrity logic is identical between mem-evoq and reckon-db — they both delegate to `reckon_gater_integrity`. mem-evoq has no Khepri/Ra startup overhead, so torture cycles complete in seconds rather than tens of seconds. We get more iterations per CI minute, find more races. The `adapter_swap_torture` axis (when implemented) will re-run the same scenarios against reckon-evoq to assert behavioural equivalence.

## License

Apache 2.0 — see [LICENSE](LICENSE).
