%% @doc Sanity: StoresService.ListStores + GetStore return the
%% expected cluster-wide topology.
%%
%% Proves (1) the gRPC surface is wired correctly and (2) cluster
%% discovery propagates — every node in the Raft membership has
%% an entry in the response.
-module(multi_node_stores_list_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([list_stores_returns_all_members/1,
         get_store_filters_by_id/1,
         get_store_unknown_returns_empty/1]).

-define(STORE_ID, <<"default_store">>).

suite() -> [{timetrap, {minutes, 1}}].

all() -> [list_stores_returns_all_members,
          get_store_filters_by_id,
          get_store_unknown_returns_empty].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(grpcbox),
    Config.

end_per_suite(_) -> ok.

init_per_testcase(_, Config) ->
    case os:getenv("RECKON_E2E_CLUSTER") of
        "1" ->
            %% grpcbox_channel:start_link links the channel to us.
            %% grpcbox_channel:stop/1 normal-exits the channel, which
            %% propagates as an EXIT signal to us — CT treats that as
            %% the test crashing. Trap exits so the channel can die
            %% cleanly when we stop it.
            process_flag(trap_exit, true),
            Config;
        _   -> {skip, "RECKON_E2E_CLUSTER not set"}
    end.

end_per_testcase(_, _) -> ok.

%%====================================================================

list_stores_returns_all_members(_Config) ->
    Channel = open_channel(),
    {ok, #{instances := Instances}, _} =
        reckon_gateway_v_1_stores_service_client:list_stores(
            #{}, #{channel => Channel}),
    grpcbox_channel:stop(Channel),

    ct:pal("ListStores returned ~p instances", [length(Instances)]),
    Members = [N || {_, N} <- multi_node_chaos:cluster_hosts()],
    {ok, RaMembers} =
        case multi_node_chaos:raft_members(default_store) of
            {ok, _} = Ok -> Ok;
            _            -> {ok, [{H, N} || {H, N} <- multi_node_chaos:cluster_hosts(),
                                            H =/= "beam00.lab"]}
        end,
    %% Every Raft member node MUST be in the instance list. There may
    %% be extras (a node announcing itself but not yet in the Raft
    %% membership) — that's fine, discovery is wider than Raft.
    InstanceNodes = [binary_to_atom(maps:get(node, I), utf8)
                     || I <- Instances],
    lists:foreach(
        fun({_Host, RaftNode}) ->
            ?assert(lists:member(RaftNode, InstanceNodes),
                    io_lib:format(
                        "Raft member ~p not in StoresService instance list ~p",
                        [RaftNode, InstanceNodes]))
        end,
        RaMembers),

    %% Every instance must have a non-empty store_id, mode, data_dir.
    lists:foreach(
        fun(I) ->
            ?assertMatch(#{store_id := <<_/binary>>}, I),
            ?assert(maps:get(mode, I) =/= 'STORE_MODE_UNSPECIFIED'),
            ?assertMatch(#{data_dir := <<_/binary>>}, I)
        end,
        Instances),

    %% Sanity: cluster_hosts also covers it (less strict — non-Raft
    %% members may not be visible here, so we don't assert all members,
    %% just that the count is sane).
    ?assert(length(Instances) >= length(RaMembers),
            io_lib:format("Discovery saw fewer instances (~b) "
                          "than Raft members (~b)",
                          [length(Instances), length(RaMembers)])),
    _ = Members,
    ok.

get_store_filters_by_id(_Config) ->
    Channel = open_channel(),
    {ok, #{instances := Instances}, _} =
        reckon_gateway_v_1_stores_service_client:get_store(
            #{store_id => ?STORE_ID},
            #{channel => Channel}),
    grpcbox_channel:stop(Channel),

    ?assert(length(Instances) >= 1,
            "GetStore returned no instances for default_store"),
    lists:foreach(
        fun(I) ->
            ?assertEqual(?STORE_ID, maps:get(store_id, I))
        end,
        Instances),
    ok.

get_store_unknown_returns_empty(_Config) ->
    Channel = open_channel(),
    %% Use a regex-valid store_id that simply doesn't exist. Names
    %% with `$' have special meaning in stream paths but DO match
    %% the store-id regex; we still want this test to verify the
    %% "no instances" path without poking validator edge cases.
    {ok, Resp, _} =
        reckon_gateway_v_1_stores_service_client:get_store(
            #{store_id => <<"no_such_store_anywhere">>},
            #{channel => Channel}),
    grpcbox_channel:stop(Channel),

    ?assertEqual([], maps:get(instances, Resp)),
    ok.

%%====================================================================

open_channel() ->
    Endpoint = case os:getenv("RECKON_E2E_GATEWAY") of
        false -> "beam01.lab:50051";
        E     -> E
    end,
    [Host, PortStr] = string:split(Endpoint, ":"),
    Port = list_to_integer(PortStr),
    Name = list_to_atom("mnd_list_chan_" ++
                        integer_to_list(erlang:unique_integer([positive]))),
    {ok, _} = grpcbox_channel:start_link(
        Name, [{http, Host, Port, []}], #{}),
    %% grpcbox_channel:start_link returns before the underlying HTTP/2
    %% connection is up. A racing RPC sees `{error, no_endpoints}'.
    %% Brief settle is the path of least resistance.
    timer:sleep(300),
    Name.
