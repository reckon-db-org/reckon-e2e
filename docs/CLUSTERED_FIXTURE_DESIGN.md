# Clustered fixture design

Design sketch for running reckon-e2e scenarios against the deployed reckon-gateway containers on the beam lab cluster, in addition to local single-node mem-evoq + reckon-db fixtures.

> **Status:** design only — not yet implemented. Single-node fixtures (`with_mem_evoq_store/1`, `with_reckon_evoq_store/1`) are in `adapter_swap_torture.erl`. This document describes how to add `with_clustered_reckon_store/1` and what changes that implies for the scenario layer.

## Motivation

The current single-node harness covers:
- `mem_evoq_adapter` (no infra — process-level)
- `reckon_evoq_adapter` against an embedded reckon-db store (khepri + ra, single node)

Neither path exercises:
- Real Raft consensus across nodes
- Leader change behaviour
- Network partitions (real, not simulated)
- Replication lag under load
- The containerized gRPC gateway surface that production consumers actually use

These are exactly the questions `multi_node_torture` exists to answer (see `docs/TORTURE_TAXONOMY.md`). The clustered fixture is the prerequisite.

## Lab assumptions

- 4-node beam cluster (`beam00.lab` … `beam03.lab`) running Ubuntu 20.04 + systemd + podman 3.4.2
- `reckon-gateway` containers deployed via `hecate-gitops/` per the workspace CLAUDE.md
- gRPC endpoint exposed by `reckon-gateway` (the current production surface — Bondy/WAMP and direct Erlang distribution are out of scope for this harness)
- Test runner can reach the gateway via the lab subnet (192.168.1.0/24) or a forwarded port

The fixture **does not** deploy or tear down containers. That's a hecate-gitops concern. Pre-condition: containers are running. Post-condition: containers are still running, the test-created stores are cleaned up.

## API

Add one fixture to `adapter_swap_torture`:

```erlang
%% Assumes reckon-gateway is reachable at the configured endpoint.
%% Creates a fresh store on the cluster, runs Scenario(Driver),
%% drops the store.
%%
%% Driver is a record/map carrying everything a scenario needs to
%% talk to the cluster:
%%   * grpc_channel  — the open gRPC channel
%%   * store_id      — the freshly-created cluster store
-spec with_clustered_reckon_store(fun((driver()) -> outcome())) -> outcome().
with_clustered_reckon_store(Scenario) ->
    Endpoint = clustered_endpoint(),
    {ok, Channel} = open_grpc_channel(Endpoint),
    StoreId = create_cluster_store(Channel),
    Driver = #{grpc_channel => Channel, store_id => StoreId,
               facade => reckon_e2e_grpc_facade},
    try Scenario(Driver)
    after drop_cluster_store(Channel, StoreId),
          close_grpc_channel(Channel)
    end.
```

Endpoint resolution priority:
1. `RECKON_E2E_GATEWAY` env var (host:port)
2. `reckon-e2e.config` file at repo root
3. Default `beam00.lab:50051`

## Scenario refactor

Today scenarios call `evoq_event_store:read/5` directly — that path doesn't exist for a remote cluster. Parametrize on a facade module:

```erlang
%% adapter_swap_basic_scenario.erl
-spec run(driver()) -> outcome().
run(#{store_id := StoreId, facade := Facade}) ->
    {ok, V} = Facade:append(StoreId, <<"agg">>, -1, [#{...}, ...]),
    {ok, Events} = Facade:read(StoreId, <<"agg">>, 0, 10, forward),
    ok = Facade:save_snapshot(StoreId, <<"agg">>, 1, #{...}, #{}),
    {ok, Snap} = Facade:load_snapshot(StoreId, <<"agg">>),
    #{appended_last_version => V, ...}.
```

Two facades:

| Facade | Drives |
|---|---|
| `reckon_e2e_local_facade` | `evoq_event_store` + `evoq_snapshot_store` (current behaviour) |
| `reckon_e2e_grpc_facade`  | gRPC calls against the deployed reckon-gateway |

Both expose the same five functions: `append/4`, `read/5`, `save_snapshot/5`, `load_snapshot/2`, `delete_snapshot/2`. Scenarios remain backend-agnostic.

## Cluster-specific scenarios (new — belong in `multi_node_torture`, not adapter-swap)

These have **no** single-node analogue, so they live in their own axis:

| Scenario | Sketch |
|---|---|
| `leader_kill_mid_write` | Drive sustained writes; SIGKILL the Raft leader pod via `ssh beamN podman kill`; verify chain stays continuous, no event loss/duplication, version sequence intact |
| `symmetric_partition_heal` | Use `iptables` rules (via ssh) to cut beam01 from {beam00, beam02, beam03}; write to majority; heal; verify minority converges and no phantom version conflicts |
| `subscription_survives_leader_change` | Subscribe via gRPC; kill leader; verify subscriber sees no gap in delivery |
| `replication_lag_under_load` | Sustained 5k events/sec for 2 min; sample follower-vs-leader version at 1Hz; assert lag stays under threshold |

All four need root on the beam nodes (for podman kill / iptables). Run via `ssh rl@beamN`.

## CI

Cannot run on free GitHub Actions runners — needs lab access. Two paths, not mutually exclusive:

1. **Self-hosted runner on the lab.** Deploy a `gh-action-runner` container on beam03 (least-loaded). The reckon-e2e CI workflow targets `runs-on: self-hosted`. Sees lab DNS, can ssh to peers.
2. **Manual workstation trigger.** `scripts/run-cluster.sh` runs from the developer's machine, assumes ssh + gateway access, prints structured results. Useful for ad-hoc soak runs.

Recommend doing (1) first because it gives regression coverage on every merge; (2) follows when the soak workloads emerge.

## Tradeoffs

| Benefit | Cost |
|---|---|
| Catches real Raft / replication bugs single-node tests miss | Each scenario takes 30s+ (container warm-up, healthcheck, Raft elections) |
| Validates the production gRPC surface, not just the Erlang adapter | Another transport (gRPC) becomes a contract to keep in sync — a third surface for adapter drift |
| Stress-tests `hecate-gitops` deploys indirectly (containers must be reachable before tests pass) | Test failures may root-cause to lab infra, not the code being tested — needs clean error messages distinguishing "infrastructure unavailable" from "behaviour wrong" |
| Single home for cross-stack chaos that no individual repo can host | Lab resource contention — long soak runs hold the cluster |

## Sequencing

1. Land a fix for the reckon-evoq snapshot read bug surfaced by the local fixture (out-of-scope for this design — needs its own release).
2. Implement `reckon_e2e_local_facade` + `reckon_e2e_grpc_facade`, refactor `adapter_swap_basic_scenario` to take a Driver.
3. Implement `with_clustered_reckon_store/1` against a pre-existing reckon-gateway endpoint. Add a basic CT suite that proves the round-trip works (same outcome as local).
4. Stand up the self-hosted GitHub Actions runner on beam03.
5. Open `multi_node_torture` and implement the four cluster-specific scenarios above.
6. Establish baseline metrics (throughput, replication lag) under steady load; bake into CI guards.
