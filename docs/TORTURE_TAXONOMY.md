# Torture taxonomy

Each torture axis exists because some property of the reckon-db stack can only be observed under one specific kind of stress. Document the axis, then implement.

## integrity_torture

**Question:** under realistic concurrent load, does the integrity layer detect tampering reliably, with low false-positive rate (clean streams pass) and bounded detection latency?

**Stressors:**
- Multiple writer processes appending to overlapping streams
- Strict readers running in tight loops
- An adversary process that mutates a single event mid-flight via `sys:replace_state`

**Properties asserted:**
- Untampered streams never surface integrity violations
- Tampered streams surface violations on the next strict read
- The targeted stream's violation does not propagate to neighbours
- Throughput stays above a floor (regression guard, not a benchmark)

**Why this can't live inside `mem-evoq` or `reckon-db`:** their integrity unit tests are sequential. Tampering during concurrent writes exercises serialization paths the unit tests don't.

---

## load_torture

**Question:** does sustained throughput hold, and does tail latency drift?

**Stressors:**
- 10–50k events/sec sustained for 5+ minutes per scenario
- Snapshot operations interleaved
- Subscriber catch-up happening alongside live writes

**Properties asserted:**
- p50, p95, p99 latency stay within configured bounds
- No memory growth past a configured ceiling
- Subscribers don't fall infinitely behind under back-pressure

**Why separate:** these tests take 10+ minutes each; they don't belong in any package's CI critical path.

---

## key_lifecycle_torture

**Question:** how does the system behave when keys disappear, rotate, or invalidate mid-flight?

**Stressors:**
- Restart a store with a different key
- Remove the key file mid-operation
- Concurrent reads + writes during a hot rotation

**Properties asserted:**
- Reads of pre-rotation events with the post-rotation key surface integrity violations cleanly
- Stores fail-fast on missing keys; no silent fallback to plaintext
- Rotation operations leave the chain continuous from the rotation point forward

---

## multi_node_torture

**Question:** under Khepri/Ra cluster events (leader change, network partition, node restart), does the chain stay correct and continuous?

**Stressors:**
- 3-node cluster
- Kill the leader mid-append
- Network partition between minority + majority
- Heal the partition; verify the merged log

**Properties asserted:**
- No event is lost or duplicated across leader transitions
- No "phantom" version conflicts after a partition heals
- Subscribers eventually catch up to the same log on all nodes

**Why this lives here, not in reckon-db:** reckon-db's CT suites run against a single-node embedded store. True multi-node chaos requires an orchestrator (separate Erlang nodes, `slave:start` or systemd, controlled kill signals).

---

## adapter_swap_torture

**Question:** do `mem_evoq_adapter` and `reckon_evoq_adapter` produce *behaviourally indistinguishable* outputs for the same workload?

**Stressors:**
- Identical scenario script runs against both adapters
- Side-by-side output comparison (event ordering, version assignment, integrity errors)

**Properties asserted:**
- For any scenario S: `run(mem_evoq_adapter, S) ≡ run(reckon_evoq_adapter, S)` modulo timing
- Adapters that pass the integrity_torture suite against mem-evoq must also pass against reckon-evoq

**Why:** mem-evoq exists in part as a reference implementation. If reckon-evoq's behaviour diverges, one of them has a bug — this axis surfaces which.
