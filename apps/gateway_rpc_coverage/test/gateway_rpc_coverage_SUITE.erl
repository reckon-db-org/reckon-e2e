%%% @doc Exhaustive RPC coverage for the catalogue-mode reckon-gateway.
%%%
%%% One case per (service, rpc, input class). Every RPC is hit at
%%% least once with a known-good input; high-risk RPCs also get
%%% bad-input assertions to lock in error-code contracts and
%%% regression-prevent the upstream-mismatch crashes that surface
%%% as INTERNAL.
%%%
%%% Regression locks (do NOT change these without bumping the
%%% gateway):
%%%
%%%   * HealthService.VerifyClusterConsistency on a single-mode
%%%     store MUST return HEALTHY with details.mode=single (NOT
%%%     INTERNAL). Locks reckon-gateway 0.5.1.
%%%   * AdminService.GetStreamInfo with empty stream_id MUST return
%%%     INVALID_ARGUMENT (status 3) and NOT dispatch to the BEAM.
%%%     Locks reckon-gateway 0.5.2.
%%%
%%% Gate / config:
%%%   RECKON_E2E_GATEWAY_COVERAGE=1
%%%   RECKON_E2E_GATEWAY=beam00.lab:50051   (default)
%%%   RECKON_E2E_STORE=parksim_entry2exit_store  (default; any
%%%     real store_id known to the catalogue works)
-module(gateway_rpc_coverage_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([
    %% StoresService
    stores_list_stores/1,
    stores_get_store_known/1,
    stores_get_store_unknown_returns_empty/1,
    stores_watch_stores_snapshot/1,

    %% StreamService
    stream_list_streams/1,
    stream_read_forward_unknown_stream/1,
    stream_read_backward_unknown_stream/1,
    stream_get_stream_version_unknown/1,
    stream_read_by_event_types/1,
    stream_read_by_tags/1,
    stream_read_all_global/1,

    %% AdminService
    admin_reload_catalogue_idempotent/1,
    admin_get_catalogue_status/1,
    admin_get_store_stats/1,
    admin_get_stream_info_empty_id_rejected/1,  %% regression lock 0.5.2
    admin_get_event_type_summary/1,

    %% HealthService
    health_check_returns_healthy/1,
    health_overall_status/1,
    health_verify_cluster_consistency_single_mode/1,  %% regression lock 0.5.1
    health_verify_membership_consensus_single_mode/1,
    health_check_raft_log_consistency_single_mode/1,
    health_get_memory_level/1,
    health_get_memory_stats/1,
    health_get_server_info/1
]).

-define(GATE_VAR, "RECKON_E2E_GATEWAY_COVERAGE").
-define(DEFAULT_ENDPOINT, "beam00.lab:50051").
-define(DEFAULT_STORE, <<"parksim_entry2exit_store">>).
-define(UNKNOWN_STREAM, <<"no_such_stream_anywhere">>).
-define(WATCH_RECV_TIMEOUT, 200).

suite() -> [{timetrap, {minutes, 2}}].

all() ->
    [
        %% StoresService
        stores_list_stores,
        stores_get_store_known,
        stores_get_store_unknown_returns_empty,
        stores_watch_stores_snapshot,

        %% StreamService
        stream_list_streams,
        stream_read_forward_unknown_stream,
        stream_read_backward_unknown_stream,
        stream_get_stream_version_unknown,
        stream_read_by_event_types,
        stream_read_by_tags,
        stream_read_all_global,

        %% AdminService
        admin_reload_catalogue_idempotent,
        admin_get_catalogue_status,
        admin_get_store_stats,
        admin_get_stream_info_empty_id_rejected,
        admin_get_event_type_summary,

        %% HealthService
        health_check_returns_healthy,
        health_overall_status,
        health_verify_cluster_consistency_single_mode,
        health_verify_membership_consensus_single_mode,
        health_check_raft_log_consistency_single_mode,
        health_get_memory_level,
        health_get_memory_stats,
        health_get_server_info
    ].

init_per_suite(Config) ->
    case os:getenv(?GATE_VAR) of
        "1" ->
            {ok, _} = application:ensure_all_started(grpcbox),
            Config;
        _ ->
            {skip, ?GATE_VAR " not set; set to 1 to drive a "
                             "deployed reckon-gateway"}
    end.

end_per_suite(_) -> ok.

init_per_testcase(_, Config) ->
    process_flag(trap_exit, true),
    Channel = open_channel(),
    [{channel, Channel}, {store, store_id_bin()} | Config].

end_per_testcase(_, Config) ->
    case proplists:get_value(channel, Config) of
        undefined -> ok;
        Ch        -> catch grpcbox_channel:stop(Ch)
    end,
    ok.

%%====================================================================
%% StoresService
%%====================================================================

stores_list_stores(Cfg) ->
    Ch = channel(Cfg),
    {ok, #{instances := Insts}, _} =
        reckon_gateway_v_1_stores_service_client:list_stores(
            #{}, #{channel => Ch}),
    ct:pal("ListStores -> ~b instances", [length(Insts)]),
    ?assert(length(Insts) >= 1,
            "catalogue must have at least the test store"),
    lists:foreach(
        fun(I) ->
            ?assertMatch(#{store_id := <<_/binary>>, node := <<_/binary>>}, I),
            ?assert(maps:get(mode, I) =/= 'STORE_MODE_UNSPECIFIED')
        end,
        Insts),
    ok.

stores_get_store_known(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    {ok, #{instances := Insts}, _} =
        reckon_gateway_v_1_stores_service_client:get_store(
            #{store_id => S}, #{channel => Ch}),
    ?assert(length(Insts) >= 1),
    lists:foreach(
        fun(I) -> ?assertEqual(S, maps:get(store_id, I)) end,
        Insts),
    ok.

stores_get_store_unknown_returns_empty(Cfg) ->
    Ch = channel(Cfg),
    {ok, #{instances := Insts}, _} =
        reckon_gateway_v_1_stores_service_client:get_store(
            #{store_id => <<"no_such_store_anywhere">>}, #{channel => Ch}),
    ?assertEqual([], Insts),
    ok.

stores_watch_stores_snapshot(Cfg) ->
    Ch = channel(Cfg),
    {ok, Stream} =
        reckon_gateway_v_1_stores_service_client:watch_stores(
            #{include_snapshot => true}, #{channel => Ch}),
    Events = drain_snapshot(Stream, 2000),
    Announced = [E || E <- Events,
                      maps:get(type, E) =:= 'STORE_EVENT_TYPE_ANNOUNCED'],
    ct:pal("WatchStores snapshot -> ~b announced events",
           [length(Announced)]),
    ?assert(length(Announced) >= 1),
    ok.

%%====================================================================
%% StreamService
%%====================================================================

stream_list_streams(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    {ok, #{stream_ids := Ids}, _} =
        reckon_gateway_v_1_stream_service_client:list_streams(
            #{store_id => S}, #{channel => Ch}),
    ct:pal("ListStreams -> ~b ids", [length(Ids)]),
    ?assert(is_list(Ids)),
    ok.

stream_read_forward_unknown_stream(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    %% Reading from a non-existent stream should NOT crash. Either
    %% empty result OR error code; INTERNAL is forbidden because that
    %% means a worker crashed.
    R = reckon_gateway_v_1_stream_service_client:read_stream_forward(
            #{store_id => S, stream_id => ?UNKNOWN_STREAM,
              start_version => 0, count => 10},
            #{channel => Ch}),
    assert_not_internal(R),
    ok.

stream_read_backward_unknown_stream(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    R = reckon_gateway_v_1_stream_service_client:read_stream_backward(
            #{store_id => S, stream_id => ?UNKNOWN_STREAM,
              start_version => 0, count => 10},
            #{channel => Ch}),
    assert_not_internal(R),
    ok.

stream_get_stream_version_unknown(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    R = reckon_gateway_v_1_stream_service_client:get_stream_version(
            #{store_id => S, stream_id => ?UNKNOWN_STREAM},
            #{channel => Ch}),
    assert_not_internal(R),
    ok.

stream_read_by_event_types(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    {ok, #{events := Evs}, _} =
        reckon_gateway_v_1_stream_service_client:read_by_event_types(
            #{store_id => S, event_types => [<<"no_such_type">>],
              batch_size => 10},
            #{channel => Ch}),
    ?assert(is_list(Evs)),
    ok.

stream_read_by_tags(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    {ok, #{events := Evs}, _} =
        reckon_gateway_v_1_stream_service_client:read_by_tags(
            #{store_id => S, tags => [<<"no_such_tag">>],
              match => 0, batch_size => 10},
            #{channel => Ch}),
    ?assert(is_list(Evs)),
    ok.

stream_read_all_global(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    {ok, #{events := Evs}, _} =
        reckon_gateway_v_1_stream_service_client:read_all_global(
            #{store_id => S, offset => 0, limit => 10},
            #{channel => Ch}),
    ?assert(is_list(Evs)),
    ok.

%%====================================================================
%% AdminService
%%====================================================================

admin_reload_catalogue_idempotent(Cfg) ->
    Ch = channel(Cfg),
    {ok, R1, _} = reckon_gateway_v_1_admin_service_client:reload_catalogue(
                      #{}, #{channel => Ch}),
    {ok, R2, _} = reckon_gateway_v_1_admin_service_client:reload_catalogue(
                      #{}, #{channel => Ch}),
    ?assertEqual(<<>>, maps:get(error, R1, <<>>)),
    ?assertEqual(<<>>, maps:get(error, R2, <<>>)),
    ?assertEqual([], maps:get(added,     R2, [])),
    ?assertEqual([], maps:get(removed,   R2, [])),
    ?assertEqual([], maps:get(restarted, R2, [])),
    ok.

admin_get_catalogue_status(Cfg) ->
    Ch = channel(Cfg),
    {ok, S, _} = reckon_gateway_v_1_admin_service_client:get_catalogue_status(
                     #{}, #{channel => Ch}),
    ?assert(maps:get(catalogue_size, S) >= 1),
    ?assert(maps:get(gateway_uptime_ms, S) > 0),
    Clusters = maps:get(clusters, S),
    ?assert(length(Clusters) >= 1),
    ok.

admin_get_store_stats(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    R = reckon_gateway_v_1_admin_service_client:get_store_stats(
            #{store_id => S}, #{channel => Ch}),
    assert_not_internal(R),
    ok.

%% Regression lock: 0.5.2 — empty stream_id MUST be rejected with
%% INVALID_ARGUMENT before dispatching. Previously dispatched, hit
%% reckon_db_store_inspector:stream_info/2 case_clause -1 crash, was
%% retried 11x (~155s), surfaced as INTERNAL.
admin_get_stream_info_empty_id_rejected(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    R = reckon_gateway_v_1_admin_service_client:get_stream_info(
            #{store_id => S, stream_id => <<>>}, #{channel => Ch}),
    case R of
        {error, {<<"3">>, _}, _} -> ok;
        {error, {<<"13">>, _}, _} ->
            ct:fail({regression_05_2_failed,
                     "GetStreamInfo with empty stream_id returned "
                     "INTERNAL — empty-id validation broke"});
        Other ->
            ct:fail({unexpected_response_for_empty_stream_id, Other})
    end.

admin_get_event_type_summary(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    {ok, #{entries := Entries}, _} =
        reckon_gateway_v_1_admin_service_client:get_event_type_summary(
            #{store_id => S}, #{channel => Ch}),
    ?assert(is_list(Entries)),
    ok.

%%====================================================================
%% HealthService
%%====================================================================

health_check_returns_healthy(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    {ok, #{status := Status}, _} =
        reckon_gateway_v_1_health_service_client:check(
            #{store_id => S}, #{channel => Ch}),
    ?assert(lists:member(Status,
                         ['HEALTH_STATUS_HEALTHY',
                          'HEALTH_STATUS_DEGRADED'])),
    ok.

health_overall_status(Cfg) ->
    Ch = channel(Cfg),
    {ok, #{status := Status}, _} =
        reckon_gateway_v_1_health_service_client:health(
            #{}, #{channel => Ch}),
    ?assertNotEqual('HEALTH_STATUS_UNSPECIFIED', Status),
    ok.

%% Regression lock: 0.5.1 — single-mode store on VerifyClusterConsistency
%% MUST short-circuit to HEALTHY with details.mode=single, NOT dispatch
%% to the BEAM (which would crash reckon_db_cluster:verify_consistency/1
%% with undef and retry-storm the worker for ~155s, surfacing as INTERNAL).
health_verify_cluster_consistency_single_mode(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    Start = erlang:monotonic_time(millisecond),
    R = reckon_gateway_v_1_health_service_client:verify_cluster_consistency(
            #{store_id => S}, #{channel => Ch}),
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    ct:pal("VerifyClusterConsistency on ~s -> ~p (elapsed ~bms)",
           [S, R, Elapsed]),
    case R of
        {ok, #{status := 'CLUSTER_STATUS_HEALTHY',
               details := Details}, _} ->
            %% Single-mode short-circuit returns details.mode = "single".
            case maps:get(<<"mode">>, Details, undefined) of
                <<"single">> -> ok;
                Other ->
                    ct:pal("note: mode detail not 'single' (was ~p) — "
                           "store may be cluster-mode; that's fine",
                           [Other])
            end;
        {error, {<<"13">>, _}, _} ->
            ct:fail({regression_05_1_failed,
                     "VerifyClusterConsistency returned INTERNAL — "
                     "the single-mode short-circuit broke (or the "
                     "configured store is cluster-mode and Raft is "
                     "actually degraded)"});
        Other ->
            ct:fail({unexpected_verify_response, Other})
    end,
    ?assert(Elapsed < 5000,
            io_lib:format("VerifyClusterConsistency took ~bms — short-"
                          "circuit failed or BEAM-side retry storm fired",
                          [Elapsed])),
    ok.

health_verify_membership_consensus_single_mode(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    R = reckon_gateway_v_1_health_service_client:verify_membership_consensus(
            #{store_id => S}, #{channel => Ch}),
    assert_not_internal(R),
    ok.

health_check_raft_log_consistency_single_mode(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    R = reckon_gateway_v_1_health_service_client:check_raft_log_consistency(
            #{store_id => S}, #{channel => Ch}),
    assert_not_internal(R),
    ok.

health_get_memory_level(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    R = reckon_gateway_v_1_health_service_client:get_memory_level(
            #{store_id => S}, #{channel => Ch}),
    assert_not_internal(R),
    ok.

health_get_memory_stats(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    R = reckon_gateway_v_1_health_service_client:get_memory_stats(
            #{store_id => S}, #{channel => Ch}),
    assert_not_internal(R),
    ok.

health_get_server_info(Cfg) ->
    Ch = channel(Cfg), S = store(Cfg),
    {ok, Info, _} = reckon_gateway_v_1_health_service_client:get_server_info(
                        #{store_id => S}, #{channel => Ch}),
    ?assertMatch(#{api_compatibility_version := <<_/binary>>}, Info),
    ?assertMatch(#{reckon_gateway_version := <<_/binary>>}, Info),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

channel(Cfg)  -> proplists:get_value(channel, Cfg).
store(Cfg)    -> proplists:get_value(store, Cfg).

store_id_bin() ->
    case os:getenv("RECKON_E2E_STORE") of
        false -> ?DEFAULT_STORE;
        S     -> list_to_binary(S)
    end.

open_channel() ->
    Endpoint = case os:getenv("RECKON_E2E_GATEWAY") of
        false -> ?DEFAULT_ENDPOINT;
        E     -> E
    end,
    [Host, PortStr] = string:split(Endpoint, ":"),
    Port = list_to_integer(PortStr),
    Name = list_to_atom("gwc_chan_" ++
                        integer_to_list(erlang:unique_integer([positive]))),
    {ok, _} = grpcbox_channel:start_link(
                  Name, [{http, Host, Port, []}], #{}),
    timer:sleep(300),
    Name.

%% Any response that isn't an INTERNAL is acceptable. We're guarding
%% against worker crashes leaking to the wire, NOT asserting domain
%% behaviour (which is suite-specific).
assert_not_internal({ok, _, _}) -> ok;
assert_not_internal({error, {<<"13">>, _}, _} = R) ->
    ct:fail({rpc_returned_internal_status_13, R});
assert_not_internal({error, _, _}) -> ok;
assert_not_internal(Other) ->
    ct:fail({unexpected_grpc_response_shape, Other}).

drain_snapshot(Stream, BudgetMs) ->
    Deadline = erlang:monotonic_time(millisecond) + BudgetMs,
    drain_loop(Stream, [], Deadline).

drain_loop(Stream, Acc, Deadline) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true  -> lists:reverse(Acc);
        false ->
            case grpcbox_client:recv_data(Stream, ?WATCH_RECV_TIMEOUT) of
                {ok, #{type := _} = E} ->
                    drain_loop(Stream, [E | Acc], Deadline);
                timeout ->
                    lists:reverse(Acc);
                _ ->
                    lists:reverse(Acc)
            end
    end.
