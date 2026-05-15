# adapter_swap_torture

Behavioural-equivalence torture across adapters.

Scoped — see `docs/TORTURE_TAXONOMY.md#adapter_swap_torture`. No Erlang code yet.

Planned approach: parametrise the scenarios from `integrity_torture` (and eventually `load_torture`) on the adapter module. Run each scenario twice — once against `mem_evoq_adapter`, once against `reckon_evoq_adapter` — and compare structured outputs.

Equivalence covers:
- Event ordering (intra-stream and global by epoch_us)
- Version assignment under contention
- Integrity-violation surfacing (same `kind`, same `version`)
- Subscription delivery shape (`{events, [#evoq_event{}]}`)
- Snapshot anchor + mac semantics
