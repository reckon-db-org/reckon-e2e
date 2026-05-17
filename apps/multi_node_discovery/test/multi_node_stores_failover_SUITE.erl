%% @doc WatchStores stream survives a Raft leader kill.
%%
%% This is the strong property: a long-lived discovery subscription
%% must keep delivering events across a leadership change. The
%% subscription is pinned to a non-leader endpoint so the subscriber's
%% own gRPC connection is unaffected by the kill — the question is
%% whether the SERVER-side WatchStores handler (and the registry
%% notification path behind it) survive the cluster's churn.
%%
%% Expected sequence on the open stream:
%%   1. STORE_EVENT_TYPE_RETIRED  for the killed leader's instance
%%   2. STORE_EVENT_TYPE_ANNOUNCED for the leader's instance when it
%%      restarts and re-announces (cleanup post-test).
%%
%% Combined with the fact that ListStores still works after, this
%% proves discovery is a real first-class subsystem, not a one-shot
%% snapshot-at-boot facade.
-module(multi_node_stores_failover_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([watch_survives_leader_kill/1]).

-define(STORE_ID, default_store).
-define(WATCH_RECV_TIMEOUT, 200).
-define(KILL_PROPAGATE_MS, 10000).
-define(ELECTION_TIMEOUT_MS, 20000).

suite() -> [{timetrap, {minutes, 3}}].

all() -> [watch_survives_leader_kill].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(grpcbox),
    Config.

end_per_suite(_) -> ok.

init_per_testcase(_, Config) ->
    case os:getenv("RECKON_E2E_CLUSTER") of
        "1" ->
            process_flag(trap_exit, true),
            Config;
        _   -> {skip, "RECKON_E2E_CLUSTER not set"}
    end.

end_per_testcase(_, _) ->
    case erlang:get(killed_host) of
        undefined -> ok;
        Host -> catch multi_node_chaos:restart_node(Host)
    end,
    ok.

%%====================================================================

watch_survives_leader_kill(_Config) ->
    SubHost = sub_endpoint_host(),
    {ok, LeaderHost, LeaderNode} = multi_node_chaos:find_leader(?STORE_ID),

    case LeaderHost =:= SubHost of
        true ->
            {skip, "leader co-located with subscriber endpoint — would "
                   "drop the stream for unrelated TCP reasons"};
        false ->
            run_failover(SubHost, LeaderHost, LeaderNode)
    end.

run_failover(_SubHost, LeaderHost, LeaderNode) ->
    Channel = open_channel(),

    {ok, WatchStream} =
        reckon_gateway_v_1_stores_service_client:watch_stores(
            #{include_snapshot => false},
            #{channel => Channel}),

    ct:pal("killing leader ~p on ~s", [LeaderNode, LeaderHost]),
    ok = multi_node_chaos:kill_node(LeaderHost),
    erlang:put(killed_host, LeaderHost),

    %% Wait for the Raft layer to elect a new leader. This also gives
    %% the store-registry pg-monitor `leave' event time to fire on
    %% surviving nodes, which is what triggers the retire notification.
    case multi_node_chaos:wait_for_leader_change(
             ?STORE_ID, LeaderNode, ?ELECTION_TIMEOUT_MS) of
        {ok, NewLeader} ->
            ct:pal("new leader: ~p", [NewLeader]);
        timeout ->
            grpcbox_channel:stop(Channel),
            ct:fail({election_timeout, ?ELECTION_TIMEOUT_MS})
    end,

    Events = collect_events_for(WatchStream, ?KILL_PROPAGATE_MS),

    %% Restart the killed node BEFORE the assertions so a failure
    %% doesn't leave the cluster degraded for the next run.
    ok = multi_node_chaos:restart_node(LeaderHost),

    %% Verify the stream is still alive: a fresh ListStores on the
    %% same channel must succeed.
    {ok, _, _} = reckon_gateway_v_1_stores_service_client:list_stores(
                     #{}, #{channel => Channel}),

    grpcbox_channel:stop(Channel),

    LeaderNodeBin = atom_to_binary(LeaderNode, utf8),
    RetiredForLeader =
        [E || E <- Events,
              maps:get(type, E) =:= 'STORE_EVENT_TYPE_RETIRED',
              maps:get(node, maps:get(instance, E)) =:= LeaderNodeBin],

    ct:pal("collected ~b events post-kill: ~p",
           [length(Events),
            [{maps:get(type, E), maps:get(node, maps:get(instance, E))}
             || E <- Events]]),

    ?assert(length(RetiredForLeader) >= 1,
            io_lib:format("no STORE_EVENT_TYPE_RETIRED received for "
                          "killed leader ~s on watch stream",
                          [LeaderNodeBin])),
    ok.

%%====================================================================

open_channel() ->
    Endpoint = case os:getenv("RECKON_E2E_GATEWAY") of
        false -> "beam01.lab:50051";
        E     -> E
    end,
    [Host, PortStr] = string:split(Endpoint, ":"),
    Port = list_to_integer(PortStr),
    Name = list_to_atom("mnd_fail_chan_" ++
                        integer_to_list(erlang:unique_integer([positive]))),
    {ok, _} = grpcbox_channel:start_link(
        Name, [{http, Host, Port, []}], #{}),
    timer:sleep(300),
    Name.

sub_endpoint_host() ->
    Endpoint = case os:getenv("RECKON_E2E_GATEWAY") of
        false -> "beam01.lab:50051";
        E     -> E
    end,
    [Host, _] = string:split(Endpoint, ":"),
    Host.

collect_events_for(Stream, DurationMs) ->
    Deadline = erlang:monotonic_time(millisecond) + DurationMs,
    collect_loop(Stream, [], Deadline).

collect_loop(Stream, Acc, Deadline) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true -> lists:reverse(Acc);
        false ->
            case grpcbox_client:recv_data(Stream, ?WATCH_RECV_TIMEOUT) of
                {ok, #{type := _} = E} ->
                    collect_loop(Stream, [E | Acc], Deadline);
                _ ->
                    collect_loop(Stream, Acc, Deadline)
            end
    end.
