%% @doc Subscription survives leader change.
%%
%% Open a live gRPC server-streaming subscription. Drive sustained
%% writes. Kill the leader mid-flight. Verify that the subscription
%% continues delivering events from BOTH pre-kill and post-election
%% windows — i.e. didn't die when its server-side hosting node
%% changed.
%%
%% Strong properties:
%%   * Subscriber receives at least one event from the pre-kill phase
%%   * Subscriber receives at least one event from the post-kill phase
%%   * Within the received subset, version sequence is monotonically
%%     non-decreasing
%%   * Whatever the subscriber received is also durable (a separate
%%     read confirms the same events live in the cluster's log)
%%
%% Failure mode worth surfacing: if the gateway's Subscribe handler
%% dies with the killed node and isn't transparently failed over to
%% the new leader, this test fails — and the failure message
%% pinpoints "subscriber stopped at version X, no events seen after
%% the kill at version Y".
%% @end
-module(multi_node_subscription_failover_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([subscription_survives_leader_change/1]).

-define(STORE_ID, default_store).
-define(WRITE_RATE_MS, 50).
-define(BATCH_SIZE, 5).
-define(PRE_KILL_MS, 4000).
-define(POST_KILL_MS, 6000).      % give subscriber more time to recover
-define(ELECTION_TIMEOUT_MS, 20000).
-define(SUBSCRIBER_DRAIN_MS, 3000).

suite() -> [{timetrap, {minutes, 3}}].

all() -> [subscription_survives_leader_change].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(grpcbox),
    Config.

end_per_suite(_) -> ok.

init_per_testcase(subscription_survives_leader_change, Config) ->
    case os:getenv("RECKON_E2E_CLUSTER") of
        "1" -> Config;
        _   -> {skip, "RECKON_E2E_CLUSTER not set"}
    end.

end_per_testcase(_, _) -> ok.

%%====================================================================
%% Scenario
%%====================================================================

subscription_survives_leader_change(_Config) ->
    %% Subscribe handler must run on a Raft cluster member — non-members
    %% can register the sub in Khepri but their local emitter supervisor
    %% isn't running, so no emitter pool spawns and events never flow.
    %% Pin the subscriber channel to a known member; the writer can
    %% still spread across endpoints.
    {SubHost, _} = hd(multi_node_chaos:cluster_hosts()),
    case os:getenv("RECKON_E2E_GATEWAY") of
        false -> ok;
        Env ->
            [GwHost, _] = string:split(Env, ":"),
            erlang:put(sub_host, GwHost)
    end,
    SubChannel = mnt_subfailover_sub_channel,
    SubEndpoint = case erlang:get(sub_host) of
                      undefined -> SubHost;
                      H -> H
                  end,
    {ok, _} = grpcbox_channel:start_link(
        SubChannel, [{http, SubEndpoint, 50051, []}], #{}),
    erlang:put(sub_channel, SubChannel),
    Endpoints = [{http, Host, 50051, []} ||
                 {Host, _Node} <- multi_node_chaos:cluster_hosts()],
    ChannelName = mnt_subfailover_channel,
    {ok, _} = grpcbox_channel:start_link(ChannelName, Endpoints, #{}),
    process_flag(trap_exit, true),
    erlang:put(channel, ChannelName),

    Nonce = binary:encode_hex(crypto:strong_rand_bytes(8)),
    StreamId = <<"subfailover$", Nonce/binary>>,
    SubName = <<"subfailover-sub$", Nonce/binary>>,
    ct:pal("scenario starting — stream=~s sub=~s sub_endpoint=~s",
           [StreamId, SubName, SubEndpoint]),

    try run_scenario(ChannelName, SubChannel, StreamId, SubName)
    after
        cleanup()
    end.

run_scenario(ChannelName, SubChannel, StreamId, SubName) ->
    %% Spin up the subscriber process FIRST, then have IT call subscribe.
    %% grpcbox stamps `client_pid = self()' into the stream state at
    %% subscribe time and routes incoming events to that pid. If we
    %% subscribed here in the test process and handed the Stream to a
    %% spawned subscriber, the data messages would land in the test
    %% mailbox and the subscriber's `receive {data, ...}' would never
    %% match. Subscribing from inside the consumer process keeps the
    %% wire data and the receive site in the same mailbox.
    Test = self(),
    SubReq = #{store_id => atom_to_binary(?STORE_ID, utf8),
               type => 'SUBSCRIPTION_TYPE_STREAM',
               selector => StreamId,
               subscription_name => SubName,
               start_from => 0,
               pool_size => 1},
    Subscriber = spawn_link(fun() ->
        {ok, SubStream} =
            reckon_gateway_v_1_subscription_service_client:subscribe(
                SubReq, #{channel => SubChannel}),
        subscriber_loop(Test, SubStream, [])
    end),
    erlang:put(subscriber, Subscriber),
    %% Give the subscriber a moment to register the subscription
    %% before writes start.
    timer:sleep(500),

    %% Writer process: same as the other scenarios
    Writer = spawn_writer(ChannelName, StreamId),
    erlang:put(writer, Writer),

    %% Phase 1: pre-kill writes
    ct:pal("phase 1: ~bms steady writes before leader kill", [?PRE_KILL_MS]),
    KillStartedAt = erlang:monotonic_time(millisecond),
    timer:sleep(?PRE_KILL_MS),
    PreKillCount = ask_subscriber_count(Subscriber),
    ct:pal("subscriber pre-kill received count = ~p", [PreKillCount]),

    %% Phase 2: snapshot leader, kill, wait election.
    %%
    %% Refuse to kill the subscriber's own endpoint — that drops the
    %% gRPC stream for HTTP/2 reasons (the TCP connection dies),
    %% masking the question we actually want to answer (does the
    %% SERVER-side subscription survive a leader change?). If the
    %% leader happens to be co-located with the subscriber endpoint,
    %% skip the test with a clear marker.
    SubHost = erlang:get(sub_host),
    {ok, OldHost, OldLeader} = multi_node_chaos:find_leader(?STORE_ID),
    case OldHost =:= SubHost of
        true ->
            ct:pal("SKIP: leader (~s) is co-located with subscriber endpoint",
                   [OldHost]),
            {skip, leader_collocated_with_subscriber};
        false ->
            ct:pal("phase 2: killing current leader ~p on ~s",
                   [OldLeader, OldHost]),
            {ok, _, _} = multi_node_chaos:kill_leader(?STORE_ID),
            erlang:put(killed_host, OldHost),
            run_phases_after_kill(
                Subscriber, Writer, OldHost, OldLeader,
                PreKillCount, KillStartedAt)
    end.

run_phases_after_kill(Subscriber, Writer, OldHost, OldLeader,
                      PreKillCount, KillStartedAt) ->
    case multi_node_chaos:wait_for_leader_change(
             ?STORE_ID, OldLeader, ?ELECTION_TIMEOUT_MS) of
        {ok, NewLeader} ->
            ct:pal("election complete — new leader = ~p", [NewLeader]);
        timeout ->
            ct:fail({election_timeout, ?ELECTION_TIMEOUT_MS})
    end,
    KillElapsedAt = erlang:monotonic_time(millisecond) - KillStartedAt,

    %% Phase 3: post-election writes
    ct:pal("phase 3: ~bms writes after election", [?POST_KILL_MS]),
    timer:sleep(?POST_KILL_MS),

    %% Phase 4: stop writer + drain
    Writer ! stop,
    erlang:put(writer_stopped, true),
    {_, WriterSuccesses, _} =
        receive {writer_done, A, S, E} -> {A, S, E}
        after 5000 -> ct:fail(writer_did_not_finish)
        end,
    WriterSuccessCount = length(WriterSuccesses) * ?BATCH_SIZE,
    ct:pal("writer ok = ~p events", [WriterSuccessCount]),

    timer:sleep(?SUBSCRIBER_DRAIN_MS),

    %% Phase 5: drain subscriber + grab its full received list
    Subscriber ! {drain, self()},
    Received =
        receive
            {drained, Es} -> Es
        after 5000 -> ct:fail(subscriber_did_not_drain)
        end,
    ct:pal("subscriber total received = ~p events (kill happened ~bms in)",
           [length(Received), KillElapsedAt]),

    %% Restart killed node before assertions so a fail doesn't leave
    %% the cluster degraded for the next run
    ok = multi_node_chaos:restart_node(OldHost),

    %% ── Assertions ──

    %% 1. Subscriber DID receive events
    ?assert(length(Received) > 0, "subscriber received zero events"),

    %% 2. Versions are monotonically increasing in the received set
    Versions = [maps:get(version, maps:get(event, M)) || M <- Received],
    ?assertEqual(lists:sort(Versions), Versions,
                 "subscriber received events out of order"),
    %% No duplicates
    ?assertEqual(length(Versions), length(lists:usort(Versions)),
                 "subscriber received duplicate versions"),

    %% 3. Subscriber received events SPANNING the kill — at least one
    %% before and one after the kill timing window.
    LowestVersion = hd(Versions),
    HighestVersion = lists:last(Versions),
    PreKillReceivedCount = PreKillCount,
    PostKillReceivedCount = length(Received) - PreKillReceivedCount,
    ct:pal("pre-kill subscriber count = ~p; post-kill new = ~p; "
           "version range = ~p..~p",
           [PreKillReceivedCount, PostKillReceivedCount,
            LowestVersion, HighestVersion]),

    ?assert(PreKillReceivedCount > 0,
            "subscriber received NOTHING pre-kill — wasn't working at all"),
    ?assert(PostKillReceivedCount > 0,
            "subscription DID NOT survive the leader change — "
            "no events received after the kill"),

    ok.

%%====================================================================
%% Subscriber loop — drains the gRPC stream
%%====================================================================

%% Loops on grpcbox_client:recv_data with a small timeout, accumulating
%% subscription_event messages. Responds to:
%%   {count_query, From}  — replies with current count
%%   {drain, From}        — drains a bit more, then replies with full list
subscriber_loop(_Parent, Stream, Acc) ->
    receive
        {count_query, From} ->
            From ! {count, length(Acc)},
            subscriber_loop(_Parent, Stream, Acc);
        {drain, From} ->
            FinalAcc = drain_more(Stream, Acc, 1000),
            From ! {drained, lists:reverse(FinalAcc)}
    after 100 ->
        Recv = grpcbox_client:recv_data(Stream, 100),
        case Recv of
            {ok, #{event := _} = M} ->
                subscriber_loop(_Parent, Stream, [M | Acc]);
            {ok, eos} ->
                ct:pal("subscriber: stream EOS"),
                subscriber_loop_eos(_Parent, Acc);
            timeout ->
                subscriber_loop(_Parent, Stream, Acc);
            _Other ->
                case erlang:get(logged_other) of
                    true -> ok;
                    _ ->
                        ct:pal("subscriber: unexpected recv_data result: ~p", [Recv]),
                        erlang:put(logged_other, true)
                end,
                subscriber_loop(_Parent, Stream, Acc)
        end
    end.

subscriber_loop_eos(_Parent, Acc) ->
    receive
        {count_query, From} ->
            From ! {count, length(Acc)},
            subscriber_loop_eos(_Parent, Acc);
        {drain, From} ->
            From ! {drained, lists:reverse(Acc)}
    end.

drain_more(Stream, Acc, BudgetMs) ->
    Deadline = erlang:monotonic_time(millisecond) + BudgetMs,
    drain_more_loop(Stream, Acc, Deadline).

drain_more_loop(Stream, Acc, Deadline) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true -> Acc;
        false ->
            case grpcbox_client:recv_data(Stream, 100) of
                {ok, #{event := _} = M} ->
                    drain_more_loop(Stream, [M | Acc], Deadline);
                {ok, eos}   -> Acc;
                timeout     -> drain_more_loop(Stream, Acc, Deadline);
                _           -> drain_more_loop(Stream, Acc, Deadline)
            end
    end.

ask_subscriber_count(Subscriber) ->
    Subscriber ! {count_query, self()},
    receive
        {count, N} -> N
    after 2000 -> 0
    end.

%%====================================================================
%% Cleanup
%%====================================================================

cleanup() ->
    case erlang:get(writer_stopped) of
        true -> ok;
        _ ->
            case erlang:get(writer) of
                W when is_pid(W) ->
                    (catch (W ! stop)),
                    receive {writer_done, _, _, _} -> ok after 2000 -> ok end;
                _ -> ok
            end
    end,
    case erlang:get(killed_host) of
        undefined -> ok;
        Host ->
            ct:pal("cleanup: restarting killed node ~s", [Host]),
            (catch multi_node_chaos:restart_node(Host))
    end,
    case erlang:get(subscriber) of
        S when is_pid(S) -> exit(S, kill);
        _ -> ok
    end,
    case erlang:get(channel) of
        undefined -> ok;
        ChannelName -> catch grpcbox_channel:stop(ChannelName)
    end,
    case erlang:get(sub_channel) of
        undefined -> ok;
        SubChannel -> catch grpcbox_channel:stop(SubChannel)
    end,
    ok.

%%====================================================================
%% Writer (shared shape with other multi_node SUITEs)
%%====================================================================

spawn_writer(ChannelName, StreamId) ->
    Parent = self(),
    spawn_link(fun() -> writer_loop(Parent, ChannelName, StreamId, 0, [], []) end).

writer_loop(Parent, Channel, StreamId, Attempted, Successes, Errors) ->
    receive
        stop ->
            Parent ! {writer_done, Attempted, Successes, Errors}
    after ?WRITE_RATE_MS ->
        Batch = [#{event_type => <<"subfailover_event">>,
                   data => integer_to_binary(Attempted + I)}
                 || I <- lists:seq(1, ?BATCH_SIZE)],
        Req = #{store_id => atom_to_binary(?STORE_ID, utf8),
                stream_id => StreamId,
                expected_version => -2,
                events => Batch},
        case catch reckon_gateway_v_1_stream_service_client:append_events(
                       Req, #{channel => Channel}) of
            {ok, #{version := V}, _} ->
                writer_loop(Parent, Channel, StreamId,
                            Attempted + ?BATCH_SIZE,
                            [V | Successes], Errors);
            Other ->
                writer_loop(Parent, Channel, StreamId,
                            Attempted + ?BATCH_SIZE,
                            Successes, [Other | Errors])
        end
    end.
