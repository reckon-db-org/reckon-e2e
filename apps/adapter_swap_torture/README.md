# adapter_swap_torture

Behavioural-equivalence torture across `mem_evoq_adapter` and `reckon_evoq_adapter`.

## What's here (v0.1.0)

- `adapter_swap_torture.erl` — harness with two adapter fixtures (`with_mem_evoq_store/1`, `with_reckon_evoq_store/1`) and an outcome-comparison helper (`compare_outcomes/2`) that scrubs timing fields before asserting structural equality.
- `adapter_swap_basic_scenario.erl` — the first scenario: append three events, read them back, save a mid-stream snapshot, load it.
- `adapter_swap_basic_SUITE.erl` — two cases. One baselines the scenario shape against mem-evoq. The other runs both adapters and asserts equivalence.

## Why through evoq, not through the adapter

Scenarios drive `evoq_event_store` and `evoq_snapshot_store` — the seams downstream consumers actually use. The adapter under test is whatever's plumbed in via `set_adapter/1`. This is the test that would have caught both 0.1.1 and 0.1.2 bug classes in mem-evoq.

## Planned coverage

| Scenario | Status |
|---|---|
| Basic: append → read → snapshot → load | ✅ |
| Integrity: tamper detection produces equivalent error shapes | 📝 |
| Subscriptions: live + catch-up delivery shape | 📝 |
| Read-by-event-type, read-by-tags filter equivalence | 📝 |
| Backward reads with verify=strict | 📝 |
| Large-stream snapshot+replay (1k+ events) | 📝 |

## Why outcomes are scrubbed

Timestamps, `epoch_us`, and `event_id` will trivially differ between any two runs. `compare_outcomes/2` removes these before diffing — what we care about is *shape* (version sequence, event types, payload preservation, snapshot data), not *identity*.
