# Changelog

All notable changes to reckon-e2e will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.3.0] - 2026-05-16

### Added — Facade abstraction (step 2 of the clustered fixture sequencing)

Scenarios are now facade-blind. A Driver map (`#{store_id, facade}`) is threaded through the harness; scenarios call `Facade:append/4`, `Facade:read/5`, `Facade:save_snapshot/5`, `Facade:load_snapshot/2` — they don't know whether the calls go to `evoq_event_store` in-process or out to gRPC.

- `reckon_e2e_facade` — `-callback` behaviour module declaring the four-function contract.
- `reckon_e2e_local_facade` — implements the behaviour against `evoq_event_store` + `evoq_snapshot_store`. Used by `with_mem_evoq_store/1` and `with_reckon_evoq_store/1`.
- `with_clustered_reckon_store/1` (next: step 3) will pass `reckon_e2e_grpc_facade` in the Driver instead.

### Changed

- `adapter_swap_basic_scenario:run/1` now takes a Driver map. The outcome map shape is unchanged.
- `adapter_swap_torture.erl` fixtures build the Driver and pass it through.

## [0.2.1] - 2026-05-16

### Changed

- Bumped `reckon_evoq` pin to 2.1.1 (snapshot read path fix).
- Removed the `RECKON_E2E_FULL` skip-gate on `adapters_produce_equivalent_outcomes` — the cross-adapter equivalence test now runs by default in `rebar3 ct`.

All 4 CT cases green: 2 in `integrity_torture` + 2 in `adapter_swap_torture` (mem-evoq baseline + cross-adapter comparison).

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
