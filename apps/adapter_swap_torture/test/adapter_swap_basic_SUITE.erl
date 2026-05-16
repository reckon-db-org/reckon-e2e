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
         adapters_produce_equivalent_outcomes/1]).

suite() -> [{timetrap, {minutes, 2}}].

all() ->
    [mem_evoq_basic_scenario_runs,
     adapters_produce_equivalent_outcomes].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

%% The cross-adapter comparison needs reckon-db's emitter pool +
%% gateway workers running, on top of khepri/ra. That bootstrap
%% lives outside the v0.1.0 skeleton — set RECKON_E2E_FULL=1 in
%% the environment to opt in once the setup is wired.
init_per_testcase(adapters_produce_equivalent_outcomes, Config) ->
    case os:getenv("RECKON_E2E_FULL") of
        "1" -> Config;
        _   -> {skip, "RECKON_E2E_FULL not set — reckon-evoq workers + "
                      "gateway pool bootstrap is pending"}
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
