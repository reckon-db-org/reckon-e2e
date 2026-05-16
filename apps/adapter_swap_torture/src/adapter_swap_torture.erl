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
    LeftScrubbed = scrub_volatile(Left),
    RightScrubbed = scrub_volatile(Right),
    compare_scrubbed(LeftScrubbed, RightScrubbed).

scrub_volatile(Map) ->
    Volatile = [timestamp, epoch_us, event_id, event_ids, timestamps],
    maps:without(Volatile, Map).

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
