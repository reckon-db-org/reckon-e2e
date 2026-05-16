# Changelog

All notable changes to reckon-e2e will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.2.0] - 2026-05-16

### Added

- `adapter_swap_torture` axis — first scenario implemented.
  - `adapter_swap_torture.erl` harness: `with_mem_evoq_store/1`, `with_reckon_evoq_store/1`, `compare_outcomes/2` (scrubs timing fields before structural-equality diff).
  - `adapter_swap_basic_scenario.erl` — append → read → snapshot → load through evoq APIs.
  - `adapter_swap_basic_SUITE.erl` — runs the scenario against mem-evoq (baseline) and against both adapters (cross-adapter comparison gated behind `RECKON_E2E_FULL=1` until the reckon-evoq emitter-pool bootstrap is wired).
- Pinned `mem_evoq` to 0.1.2 (snapshot-adapter behaviour fix).
- Pinned `reckon_evoq` 2.1.0 and `reckon_db` 2.1.1.

### Changed

- `project_apps` now includes `adapter_swap_torture`.
- `ct_opts` test dir list extended.

## [0.1.0] - 2026-05-15

Initial scaffold.

### Added

- Repo structure: umbrella with `apps/{integrity_torture, load_torture, key_lifecycle_torture, multi_node_torture, adapter_swap_torture}/`.
- `integrity_torture` axis with two scenarios:
  - `concurrent_writes_under_strict_reads_SUITE` — 5 writers + 3 strict readers hammering 100 streams for 10 seconds. Asserts no integrity violations, no crashes, throughput floor.
  - `tamper_detection_under_load_SUITE` — same workload, but mid-flight a single event is tampered. Asserts the targeted stream surfaces an integrity violation; neighbours stay clean.
- Run scripts: `scripts/run-all.sh`, `scripts/run-suite.sh`.
- Codeberg Actions CI (`.github/workflows/ci.yml`) running on push to main.
- Pinned to `mem_evoq 0.1.1`, `evoq 1.15.0`, `reckon_gater 2.1.0`.

### Scoped, not yet implemented

- `load_torture` — sustained throughput / tail latency.
- `key_lifecycle_torture` — key rotation, absence, invalidation.
- `multi_node_torture` — Khepri leader-change, partition / heal.
- `adapter_swap_torture` — behavioural equivalence across adapters.

See `docs/TORTURE_TAXONOMY.md` for the design behind each axis.
