%% @doc Adapter-swap torture harness.
%%
%% A "scenario" here is a fun/1 that takes a StoreId, drives the
%% evoq APIs (NOT the adapter directly — the point is to test the
%% seam), and returns a map of observable outcomes. The harness:
%%
%%   1. Plumbs the adapter into evoq_event_store + evoq_snapshot_store
%%   2. Starts a store for that adapter
%%   3. Runs the scenario, captures the outcome map
%%   4. Cleans up
%%
%% Compare two outcome maps with {@link compare_outcomes/2} — it
%% ignores timing fields (timestamps, epoch_us, event_ids) which
%% will differ run-to-run.
%%
%% Behavioural equivalence holds when every non-timing key produces
%% the same value across adapters.
%% @end
-module(adapter_swap_torture).

-include_lib("reckon_db/include/reckon_db.hrl").

-export([with_mem_evoq_store/1,
         with_reckon_evoq_store/1,
         with_clustered_reckon_store/1,
         compare_outcomes/2]).

%%====================================================================
%% Adapter fixtures
%%====================================================================

%% @doc Run Scenario(Driver) with mem_evoq_adapter plumbed into evoq.
%% The Driver carries the StoreId and the local facade module.
with_mem_evoq_store(Scenario) when is_function(Scenario, 1) ->
    {ok, _} = application:ensure_all_started(mem_evoq),
    ok = evoq_event_store:set_adapter(mem_evoq_adapter),
    ok = evoq_snapshot_store:set_adapter(mem_evoq_adapter),
    StoreId = unique_store_id("mem_evoq"),
    {ok, _} = mem_evoq:start_store(StoreId),
    Driver = #{store_id => StoreId, facade => reckon_e2e_local_facade},
    try Scenario(Driver)
    after catch mem_evoq:stop_store(StoreId)
    end.

%% @doc Run Scenario(Driver) with reckon_evoq_adapter plumbed into
%% evoq. Wraps the reckon-db / khepri / ra startup boilerplate.
with_reckon_evoq_store(Scenario) when is_function(Scenario, 1) ->
    ensure_reckon_db_started(),
    ok = evoq_event_store:set_adapter(reckon_evoq_adapter),
    ok = evoq_snapshot_store:set_adapter(reckon_evoq_adapter),
    {StoreId, DataDir} = start_reckon_store(),
    Driver = #{store_id => StoreId, facade => reckon_e2e_local_facade},
    try Scenario(Driver)
    after stop_reckon_store(StoreId, DataDir)
    end.

%% @doc Run Scenario(Driver) against a deployed reckon-gateway.
%%
%% Endpoint resolution: `RECKON_E2E_GATEWAY' env var (`host:port'),
%% falling back to `localhost:50051'. The harness opens a gRPC
%% channel for the duration of the scenario; the cluster itself is
%% NOT provisioned here — it's expected to be running already (via
%% hecate-gitops on the beam cluster, or a local podman container
%% for dev).
%%
%% Each scenario runs against the pre-configured `default_store' on
%% the cluster (the only store the gateway's sys.config declares).
%% Per-run uniqueness comes from the scenario's stream_id, not the
%% store_id — see adapter_swap_basic_scenario:run/1.
%%
%% Dynamic store creation via gRPC is not yet a feature of the
%% gateway (the proto layer accepts arbitrary store ids since
%% reckon-gateway 0.3.0+, but the reckon_db_sup:start_store/1 call
%% must happen out-of-band on the server). See the v0.4.0 TODO in
%% the gateway repo.
with_clustered_reckon_store(Scenario) when is_function(Scenario, 1) ->
    {ok, _} = application:ensure_all_started(grpcbox),
    %% grpcbox_channel:start_link/3 links the channel to the caller.
    %% If the channel dies (e.g. all endpoints unreachable), the caller
    %% process is killed by the EXIT signal. Trap exits so we can
    %% observe the death instead of dying with it.
    OldTrap = process_flag(trap_exit, true),
    {Host, Port} = resolve_gateway_endpoint(),
    ok = reckon_e2e_grpc_facade:start_channel(Host, Port),
    Driver = #{store_id => default_store, facade => reckon_e2e_grpc_facade},
    try Scenario(Driver)
    after
        catch reckon_e2e_grpc_facade:stop_channel(),
        process_flag(trap_exit, OldTrap)
    end.

resolve_gateway_endpoint() ->
    Env = os:getenv("RECKON_E2E_GATEWAY", "localhost:50051"),
    [Host, PortStr] = string:split(Env, ":"),
    {Host, list_to_integer(PortStr)}.

%%====================================================================
%% Outcome comparison
%%====================================================================

%% @doc Assert structural equality between two outcome maps, dropping
%% keys we know vary run-to-run (timestamps, event_ids). The intent
%% is to lock down the SHAPE produced by each adapter, not the
%% precise values that will trivially differ.
%%
%% Returns ok on match, or {differs, #{key := {LeftValue, RightValue}}}
%% as a structured diff.
compare_outcomes(Left, Right) when is_map(Left), is_map(Right) ->
    LeftScrubbed = normalize(scrub_volatile(Left)),
    RightScrubbed = normalize(scrub_volatile(Right)),
    compare_scrubbed(LeftScrubbed, RightScrubbed).

scrub_volatile(Map) ->
    Volatile = [timestamp, epoch_us, event_id, event_ids, timestamps],
    maps:without(Volatile, Map).

%% The gRPC facade goes through JSON on the wire, so payload map
%% keys come back as binaries. mem-evoq preserves Erlang term shape
%% so they stay atoms. Normalize both sides to binary keys before
%% diffing so the comparison reflects structural equality, not
%% serialization choices.
normalize(V) when is_map(V) ->
    maps:from_list([{norm_key(K), normalize(Val)} || {K, Val} <- maps:to_list(V)]);
normalize(V) when is_list(V) ->
    [normalize(E) || E <- V];
normalize(V) when is_atom(V), V =/= true, V =/= false, V =/= undefined, V =/= null ->
    atom_to_binary(V, utf8);
normalize(V) ->
    V.

norm_key(K) when is_atom(K) -> atom_to_binary(K, utf8);
norm_key(K)                  -> K.

compare_scrubbed(Same, Same) ->
    ok;
compare_scrubbed(Left, Right) ->
    Diff = maps:fold(
        fun(K, LeftV, Acc) ->
            RightV = maps:get(K, Right, '$missing'),
            include_if_differs(K, LeftV, RightV, Acc)
        end,
        #{}, Left),
    {differs, Diff}.

include_if_differs(_K, Same, Same, Acc) -> Acc;
include_if_differs(K, LeftV, RightV, Acc) ->
    maps:put(K, {LeftV, RightV}, Acc).

%%====================================================================
%% Internal — reckon-db lifecycle
%%====================================================================
%%
%% Uses reckon-db's own high-level entry: reckon_db_sup:start_store/1.
%% That dynamically adds the full supervision tree (emitter pool,
%% subscription manager, writer/reader/gateway pools, store registry)
%% under the running reckon_db_app. The harness no longer hand-rolls
%% khepri:put baseline structure — the store config carries everything
%% the supervision tree needs to bootstrap itself.

ensure_reckon_db_started() ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    %% ra's data_dir must be set BEFORE ra starts. Pick a stable dir
    %% so that nested test runs share the ra system process.
    RaDir = "/tmp/reckon_e2e_ra",
    ok = filelib:ensure_dir(filename:join(RaDir, "dummy")),
    application:set_env(ra, data_dir, RaDir),
    {ok, _} = application:ensure_all_started(reckon_db),
    ok.

start_reckon_store() ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_e2e_swap_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("swap_reckon_" ++ Rand),
    Config = #store_config{
        store_id = StoreId,
        data_dir = DataDir,
        mode     = single
    },
    {ok, _Pid} = reckon_db_sup:start_store(Config),
    {StoreId, DataDir}.

stop_reckon_store(StoreId, DataDir) ->
    catch reckon_db_sup:stop_store(StoreId),
    os:cmd("rm -rf " ++ DataDir),
    ok.

unique_store_id(Prefix) ->
    list_to_atom(
        "swap_" ++ Prefix ++ "_" ++
        integer_to_list(erlang:unique_integer([positive]))).
