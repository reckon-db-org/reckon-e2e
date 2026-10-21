# reckon-e2e
[![GitHub Sponsors](https://img.shields.io/badge/GitHub%20Sponsors-support-ea4aaa.svg?logo=githubsponsors&logoColor=white)](https://github.com/sponsors/rgfaber)

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

## Version floors

The constraints are in `rebar.config`; this table is what they say, not a
list of pins. There is no committed lock (`rebar.lock` is gitignored), and
what a run resolves depends on the checkout:

- a fresh checkout, which is what CI does, has no lock, so each run resolves
  the NEWEST release matching each floor from hex - the floor is the
  contract, the resolution is whatever hex answered that day;
- a local copy resolves once and writes the `rebar.lock` that run produced;
  later runs reuse it until the file is removed or `rebar3 upgrade` runs.

Bump a floor deliberately, and document it in `CHANGELOG.md`.

| Package | Constraint in `rebar.config` |
|---|---|
| `mem_evoq` | 0.1.2 |
| `reckon_evoq` | `~> 2.9` |
| `reckon_db` | `~> 5.4` |
| `evoq` | `~> 1.21` |
| `reckon_gater` | `~> 3.6` |
| `grpcbox` | 0.17.1 |

CI runs OTP 28.4.3. Running on OTP 29 needs a khepri that builds there:
0.17.2 does not (`khepri_import_export.erl`, `warnings_as_errors`), 0.17.11
does. reckon-db 5.12.0 floors khepri at `~> 0.17.11`, so the floor is what
moves, not a claim made here.

## Why mem-evoq for integrity_torture (and not reckon-db)?

The integrity logic is identical between mem-evoq and reckon-db — they both delegate to `reckon_gater_integrity`. mem-evoq has no Khepri/Ra startup overhead, so torture cycles complete in seconds rather than tens of seconds. We get more iterations per CI minute, find more races. The `adapter_swap_torture` axis (when implemented) will re-run the same scenarios against reckon-evoq to assert behavioural equivalence.

## License

Apache 2.0 — see [LICENSE](LICENSE).
