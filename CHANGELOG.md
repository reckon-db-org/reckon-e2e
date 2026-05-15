# Changelog

All notable changes to reckon-e2e will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

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
