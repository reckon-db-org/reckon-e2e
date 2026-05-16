%% @doc CT suite proving the basic adapter_swap scenario produces
%% behaviourally-equivalent outcomes against mem-evoq and reckon-evoq.
%%
%% Two test cases:
%%
%%   * mem_evoq_basic_scenario_runs  — runs the scenario against
%%     mem-evoq; baseline of expected outcomes.
%%   * adapters_produce_equivalent_outcomes — runs the same scenario
%%     against BOTH adapters and asserts equivalence via
%%     adapter_swap_torture:compare_outcomes/2.
%% @end
-module(adapter_swap_basic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([mem_evoq_basic_scenario_runs/1,
         adapters_produce_equivalent_outcomes/1,
         clustered_basic_scenario_matches_local/1]).

suite() -> [{timetrap, {minutes, 2}}].

all() ->
    [mem_evoq_basic_scenario_runs,
     adapters_produce_equivalent_outcomes,
     clustered_basic_scenario_matches_local].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

%% The clustered test needs a reachable reckon-gateway. Gate it
%% behind `RECKON_E2E_CLUSTER=1' so local + CI runs without the
%% lab don't fail. Set both `RECKON_E2E_CLUSTER=1' AND optionally
%% `RECKON_E2E_GATEWAY=host:port' (default: localhost:50051) to run.
init_per_testcase(clustered_basic_scenario_matches_local, Config) ->
    case os:getenv("RECKON_E2E_CLUSTER") of
        "1" -> Config;
        _   -> {skip, "RECKON_E2E_CLUSTER not set — clustered scenario "
                      "needs a reachable reckon-gateway (set "
                      "RECKON_E2E_GATEWAY=host:port)"}
    end;
init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(_TC, _Config) -> ok.

%%====================================================================
%% Cases
%%====================================================================

%% Baseline: confirm the scenario produces the expected SHAPE against
%% mem-evoq. If this regresses, we know the scenario itself drifted —
%% before we even compare adapters.
mem_evoq_basic_scenario_runs(_Config) ->
    Outcome = adapter_swap_torture:with_mem_evoq_store(
        fun adapter_swap_basic_scenario:run/1),

    ?assertEqual(2,                          maps:get(appended_last_version, Outcome)),
    ?assertEqual(3,                          maps:get(read_count, Outcome)),
    ?assertEqual([0, 1, 2],                  maps:get(read_versions, Outcome)),
    ?assertEqual([<<"swap_e_v1">>,
                  <<"swap_e_v1">>,
                  <<"swap_e_v1">>],          maps:get(read_event_types, Outcome)),
    ?assertEqual([#{n => 1}, #{n => 2}, #{n => 3}],
                                             maps:get(read_payloads, Outcome)),
    ?assertEqual(1,                          maps:get(snapshot_version, Outcome)),
    ?assertEqual(#{state => mid_stream, processed_count => 2},
                                             maps:get(snapshot_data, Outcome)),
    ?assertEqual(#{trace_id => <<"swap-trace-1">>},
                                             maps:get(snapshot_metadata, Outcome)),
    ok.

%% The main property: both adapters produce equivalent outcomes for
%% the same scenario. Differences should ONLY appear in volatile
%% fields (timestamps, event_ids), which compare_outcomes/2 scrubs.
adapters_produce_equivalent_outcomes(_Config) ->
    MemOutcome = adapter_swap_torture:with_mem_evoq_store(
        fun adapter_swap_basic_scenario:run/1),
    ReckonOutcome = adapter_swap_torture:with_reckon_evoq_store(
        fun adapter_swap_basic_scenario:run/1),

    ct:pal("mem-evoq outcome:    ~p", [MemOutcome]),
    ct:pal("reckon-evoq outcome: ~p", [ReckonOutcome]),

    case adapter_swap_torture:compare_outcomes(MemOutcome, ReckonOutcome) of
        ok ->
            ok;
        {differs, Diff} ->
            ct:fail("Adapter outcomes diverge: ~p", [Diff])
    end.

%% Drive the same scenario against a deployed reckon-gateway via the
%% gRPC facade and compare against the mem-evoq baseline. Locks
%% down behavioural equivalence across local AND remote transports.
clustered_basic_scenario_matches_local(_Config) ->
    MemOutcome = adapter_swap_torture:with_mem_evoq_store(
        fun adapter_swap_basic_scenario:run/1),
    ClusteredOutcome = adapter_swap_torture:with_clustered_reckon_store(
        fun adapter_swap_basic_scenario:run/1),

    ct:pal("mem-evoq outcome:   ~p", [MemOutcome]),
    ct:pal("clustered outcome:  ~p", [ClusteredOutcome]),

    case adapter_swap_torture:compare_outcomes(MemOutcome, ClusteredOutcome) of
        ok ->
            ok;
        {differs, Diff} ->
            ct:fail("Local vs clustered outcomes diverge: ~p", [Diff])
    end.
