%% @doc Catalogue-mode discovery against a deployed reckon-gateway.
%%
%% Validates that the multi-cluster catalogue published by the
%% gateway in catalogue mode (0.5+) exposes the three single-mode
%% parksim stores as distinct StoreInstance entries via the standard
%% StoresService gRPC surface.
%%
%% This SUITE explicitly does NOT touch Raft chaos — single-mode
%% reckon-db nodes have no Raft. Failover/elections are validated
%% by the cluster-mode SUITEs in this app.
%%
%% Gate / endpoint:
%%   RECKON_E2E_CATALOGUE=1
%%   RECKON_E2E_GATEWAY=beam00.lab:50051   %% any beam works; gateways
%%                                          %% are idempotent.
%%
%% Expected catalogue (parksim fleet):
%%   - parksim_entry2exit_store on parksim_entry2exit@192.168.1.10
%%   - parksim_lot_store        on parksim_lot@192.168.1.11
%%   - parksim_pricing_store    on parksim_pricing@192.168.1.12
%% (parksim_simulator is a producer-only sibling, no store.)
-module(catalogue_stores_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([list_stores_returns_parksim_three/1,
         get_store_filters_by_id/1,
         get_store_unknown_returns_empty/1,
         watch_snapshot_matches_list/1]).

-define(EXPECTED_STORES,
        [{<<"parksim_entry2exit_store">>, <<"parksim_entry2exit@192.168.1.10">>},
         {<<"parksim_lot_store">>,        <<"parksim_lot@192.168.1.11">>},
         {<<"parksim_pricing_store">>,    <<"parksim_pricing@192.168.1.12">>}]).

-define(WATCH_RECV_TIMEOUT, 200).

suite() -> [{timetrap, {minutes, 1}}].

all() -> [list_stores_returns_parksim_three,
          get_store_filters_by_id,
          get_store_unknown_returns_empty,
          watch_snapshot_matches_list].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(grpcbox),
    Config.

end_per_suite(_) -> ok.

init_per_testcase(_, Config) ->
    case os:getenv("RECKON_E2E_CATALOGUE") of
        "1" ->
            process_flag(trap_exit, true),
            Config;
        _   -> {skip, "RECKON_E2E_CATALOGUE not set"}
    end.

end_per_testcase(_, _) -> ok.

%%====================================================================

list_stores_returns_parksim_three(_Config) ->
    Channel = open_channel(),
    {ok, #{instances := Instances}, _} =
        reckon_gateway_v_1_stores_service_client:list_stores(
            #{}, #{channel => Channel}),
    grpcbox_channel:stop(Channel),

    ct:pal("ListStores returned ~b instances:~n  ~p",
           [length(Instances),
            [{maps:get(store_id, I), maps:get(node, I), maps:get(mode, I)}
             || I <- Instances]]),

    %% Every expected (store_id, node) pair must be present.
    Returned = lists:sort([{maps:get(store_id, I), maps:get(node, I)}
                           || I <- Instances]),
    Expected = lists:sort(?EXPECTED_STORES),
    lists:foreach(
        fun({S, N} = Pair) ->
            ?assert(lists:member(Pair, Returned),
                    io_lib:format(
                        "expected store ~s on ~s missing from catalogue:~n  ~p",
                        [S, N, Returned]))
        end,
        Expected),

    %% Every instance carries the structural fields. Single-mode only.
    lists:foreach(
        fun(I) ->
            ?assertMatch(#{store_id := <<_/binary>>}, I),
            ?assertEqual('STORE_MODE_SINGLE', maps:get(mode, I)),
            ?assertMatch(#{data_dir := <<_/binary>>}, I)
        end,
        Instances),
    ok.

get_store_filters_by_id(_Config) ->
    Channel = open_channel(),
    lists:foreach(
        fun({StoreIdBin, ExpectedNodeBin}) ->
            {ok, #{instances := Instances}, _} =
                reckon_gateway_v_1_stores_service_client:get_store(
                    #{store_id => StoreIdBin},
                    #{channel => Channel}),
            ?assertEqual(
                1, length(Instances),
                io_lib:format("GetStore ~s returned ~b instances; want 1",
                              [StoreIdBin, length(Instances)])),
            [Only] = Instances,
            ?assertEqual(StoreIdBin,      maps:get(store_id, Only)),
            ?assertEqual(ExpectedNodeBin, maps:get(node, Only)),
            ?assertEqual('STORE_MODE_SINGLE', maps:get(mode, Only))
        end,
        ?EXPECTED_STORES),
    grpcbox_channel:stop(Channel),
    ok.

get_store_unknown_returns_empty(_Config) ->
    Channel = open_channel(),
    {ok, Resp, _} =
        reckon_gateway_v_1_stores_service_client:get_store(
            #{store_id => <<"no_such_store_anywhere">>},
            #{channel => Channel}),
    grpcbox_channel:stop(Channel),
    ?assertEqual([], maps:get(instances, Resp)),
    ok.

watch_snapshot_matches_list(_Config) ->
    Channel = open_channel(),

    {ok, WatchStream} =
        reckon_gateway_v_1_stores_service_client:watch_stores(
            #{include_snapshot => true},
            #{channel => Channel}),
    SnapshotEvents = drain_snapshot(WatchStream, 2000),

    {ok, #{instances := Instances}, _} =
        reckon_gateway_v_1_stores_service_client:list_stores(
            #{}, #{channel => Channel}),

    grpcbox_channel:stop(Channel),

    SnapshotKeys = lists:sort(
        [{maps:get(store_id, maps:get(instance, E)),
          maps:get(node, maps:get(instance, E))}
         || E <- SnapshotEvents,
            maps:get(type, E) =:= 'STORE_EVENT_TYPE_ANNOUNCED']),
    ListKeys = lists:sort(
        [{maps:get(store_id, I), maps:get(node, I)} || I <- Instances]),

    ct:pal("snapshot keys: ~p", [SnapshotKeys]),
    ct:pal("list    keys: ~p", [ListKeys]),
    ?assertEqual(ListKeys, SnapshotKeys,
                 "WatchStores snapshot disagrees with ListStores"),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

open_channel() ->
    Endpoint = case os:getenv("RECKON_E2E_GATEWAY") of
        false -> "beam00.lab:50051";
        E     -> E
    end,
    [Host, PortStr] = string:split(Endpoint, ":"),
    Port = list_to_integer(PortStr),
    Name = list_to_atom("cat_chan_" ++
                        integer_to_list(erlang:unique_integer([positive]))),
    {ok, _} = grpcbox_channel:start_link(
        Name, [{http, Host, Port, []}], #{}),
    %% grpcbox_channel:start_link returns before the HTTP/2 connection
    %% is established; a racing RPC sees {error, no_endpoints}. Brief
    %% settle is the path of least resistance.
    timer:sleep(300),
    Name.

drain_snapshot(Stream, BudgetMs) ->
    Deadline = erlang:monotonic_time(millisecond) + BudgetMs,
    drain_snapshot_loop(Stream, [], Deadline).

drain_snapshot_loop(Stream, Acc, Deadline) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true -> lists:reverse(Acc);
        false ->
            case grpcbox_client:recv_data(Stream, ?WATCH_RECV_TIMEOUT) of
                {ok, #{type := _} = E} ->
                    drain_snapshot_loop(Stream, [E | Acc], Deadline);
                timeout ->
                    lists:reverse(Acc);
                _Other ->
                    lists:reverse(Acc)
            end
    end.
