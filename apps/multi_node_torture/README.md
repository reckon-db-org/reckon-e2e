# multi_node_torture

Khepri / Ra cluster torture — leader change, partition / heal, node restart.

Scoped — see `docs/TORTURE_TAXONOMY.md#multi_node_torture`. No Erlang code yet.

Planned scenarios:
- Kill the leader mid-append; verify no event lost or duplicated
- Symmetric partition (1+1+1); heal; verify all nodes converge
- Slow-leader chaos: pause the leader for 5s; verify followers don't double-elect
- Subscriber survives leader transition with no gap

This axis requires real reckon-db (not mem-evoq) and is the heaviest of the lot. Likely needs a podman-based orchestrator under `scripts/multi-node/`.
