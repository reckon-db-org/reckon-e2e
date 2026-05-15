# load_torture

Sustained throughput + tail latency torture for the reckon-db stack.

Scoped — see `docs/TORTURE_TAXONOMY.md#load_torture`. No Erlang code yet; this is a placeholder app slot in the umbrella.

Planned scenarios:
- 10k events/sec sustained 5 min, 100 streams, 1 writer per stream, p99 latency floor
- Subscriber back-pressure: 50k catch-up events delivered while live writers continue at full rate
- Snapshot-during-load: every 30s, snapshot a random stream while writers continue
