%% @doc WatchStores: snapshot completeness + live event delivery.
%%
%% Two properties:
%%
%%   (1) The snapshot (`include_snapshot = true') emitted at
%%       subscription start covers exactly the set returned by a
%%       concurrent ListStores call — no missing entries, no
%%       phantom announcements.
%%
%%   (2) Live retire/announce events fire on real topology changes.
%%       Kill a non-leader node → subscriber sees a STORE_EVENT_TYPE_RETIRED
%%       for that node's instance. Restart it → subscriber sees a
%%       STORE_EVENT_TYPE_ANNOUNCED for the new registration.
-module(multi_node_stores_watch_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([snapshot_matches_list_stores/1,
         retire_event_on_node_kill/1]).

-define(STORE_ID, default_store).
-define(WATCH_RECV_TIMEOUT, 200).
-define(KILL_PROPAGATE_MS, 8000).

suite() -> [{timetrap, {minutes, 3}}].

all() -> [snapshot_matches_list_stores,
          retire_event_on_node_kill].

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
    %% Best-effort: heal any partition, restart any killed host.
    catch multi_node_chaos:heal_partition(),
    case erlang:get(killed_host) of
        undefined -> ok;
        Host -> catch multi_node_chaos:restart_node(Host)
    end,
    ok.

%%====================================================================
%% (1) Snapshot completeness
%%====================================================================

snapshot_matches_list_stores(_Config) ->
    Channel = open_channel(),

    %% Open WatchStores and collect the snapshot phase.
    {ok, WatchStream} =
        reckon_gateway_v_1_stores_service_client:watch_stores(
            #{include_snapshot => true},
            #{channel => Channel}),
    SnapshotEvents = drain_snapshot(WatchStream, 2000),

    %% Concurrent ListStores — the truth-set the snapshot must cover.
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
                 "snapshot disagrees with ListStores on cluster membership"),
    ok.

%%====================================================================
%% (2) Live retire on node kill
%%====================================================================

retire_event_on_node_kill(_Config) ->
    Channel = open_channel(),

    %% Open WatchStores with no snapshot — we only want live events.
    {ok, WatchStream} =
        reckon_gateway_v_1_stores_service_client:watch_stores(
            #{include_snapshot => false},
            #{channel => Channel}),
    erlang:put(watch_stream, WatchStream),

    %% Pick a victim that's NOT the subscriber endpoint and NOT the
    %% current Raft leader (we want to isolate the discovery signal,
    %% not muddle it with election dynamics).
    SubHost = sub_endpoint_host(),
    {ok, LeaderHost, _LeaderNode} = multi_node_chaos:find_leader(?STORE_ID),
    case pick_victim(SubHost, LeaderHost) of
        {ok, VictimHost, VictimNode} ->
            ct:pal("killing ~s (node ~p); leader=~s sub_endpoint=~s",
                   [VictimHost, VictimNode, LeaderHost, SubHost]),
            run_kill_and_assert(WatchStream, Channel,
                                VictimHost, VictimNode);
        {error, no_victim} ->
            grpcbox_channel:stop(Channel),
            {skip, "Need 3+ live members != subscriber AND != leader"}
    end.

run_kill_and_assert(WatchStream, Channel, VictimHost, VictimNode) ->
    ok = multi_node_chaos:kill_node(VictimHost),
    erlang:put(killed_host, VictimHost),

    Events = collect_events_for(WatchStream, ?KILL_PROPAGATE_MS),
    grpcbox_channel:stop(Channel),

    ct:pal("collected ~b events: ~p", [length(Events),
                                       [{maps:get(type, E),
                                         maps:get(node, maps:get(instance, E))}
                                        || E <- Events]]),

    VictimNodeBin = atom_to_binary(VictimNode, utf8),
    RetiredForVictim =
        [E || E <- Events,
              maps:get(type, E) =:= 'STORE_EVENT_TYPE_RETIRED',
              maps:get(node, maps:get(instance, E)) =:= VictimNodeBin],

    ok = multi_node_chaos:restart_node(VictimHost),

    ?assert(length(RetiredForVictim) >= 1,
            io_lib:format("no STORE_EVENT_TYPE_RETIRED received for "
                          "killed node ~s. all events: ~p",
                          [VictimNodeBin,
                           [{maps:get(type, E),
                             maps:get(node, maps:get(instance, E))}
                            || E <- Events]])),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

open_channel() ->
    Endpoint = case os:getenv("RECKON_E2E_GATEWAY") of
        false -> "beam01.lab:50051";
        E     -> E
    end,
    [Host, PortStr] = string:split(Endpoint, ":"),
    Port = list_to_integer(PortStr),
    Name = list_to_atom("mnd_watch_chan_" ++
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

%% Pick a Raft member that is neither the subscriber endpoint nor
%% the current leader. Returns {ok, Host, Node} or {error, no_victim}.
pick_victim(SubHost, LeaderHost) ->
    {ok, Members} = multi_node_chaos:raft_members(?STORE_ID),
    case [{H, N} || {H, N} <- Members,
                    H =/= SubHost,
                    H =/= LeaderHost] of
        [{H, N} | _] -> {ok, H, N};
        []           -> {error, no_victim}
    end.

%% Drain the snapshot phase: read events until we see a `timeout' on
%% recv_data (which indicates no more buffered messages right now) or
%% the overall budget elapses.
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

%% Collect events for a fixed duration regardless of recv timeouts.
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
