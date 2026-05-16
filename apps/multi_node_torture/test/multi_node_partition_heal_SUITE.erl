%% @doc Symmetric partition + heal torture.
%%
%% Steady writes to a stream. Partition one node (minority) off the
%% cluster with iptables. Continue writing to the majority. Heal the
%% partition. Verify the minority converges to the majority's log
%% with no phantom versions / split-brain.
%%
%% With 4 nodes the only stable partition is 3-vs-1 (a 2-vs-2 would
%% lose quorum on both sides — separate scenario). Majority has 3,
%% quorum=3 → writes succeed.
%%
%% Requires `RECKON_E2E_CLUSTER=1' + a reachable cluster.
%% @end
-module(multi_node_partition_heal_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([symmetric_partition_heal/1]).

-define(STORE_ID, default_store).
-define(WRITE_RATE_MS, 50).
-define(BATCH_SIZE, 5).
-define(PRE_PARTITION_MS, 3000).
-define(DURING_PARTITION_MS, 5000).
-define(CONVERGE_TIMEOUT_MS, 30000).

suite() -> [{timetrap, {minutes, 3}}].

all() -> [symmetric_partition_heal].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(grpcbox),
    Config.

end_per_suite(_) -> ok.

init_per_testcase(symmetric_partition_heal, Config) ->
    case os:getenv("RECKON_E2E_CLUSTER") of
        "1" -> Config;
        _   -> {skip, "RECKON_E2E_CLUSTER not set"}
    end.

end_per_testcase(_, _Config) ->
    %% Idempotent — even if the body cleaned up already, this is safe.
    catch multi_node_chaos:heal_partition(),
    ok.

%%====================================================================
%% Scenario
%%====================================================================

symmetric_partition_heal(_Config) ->
    Endpoints = [{http, Host, 50051, []} ||
                 {Host, _Node} <- multi_node_chaos:cluster_hosts()],
    ChannelName = mnt_partition_channel,
    {ok, _} = grpcbox_channel:start_link(ChannelName, Endpoints, #{}),
    process_flag(trap_exit, true),
    erlang:put(channel, ChannelName),

    StreamId = iolist_to_binary([
        <<"partition$">>, binary:encode_hex(crypto:strong_rand_bytes(8))]),
    ct:pal("scenario starting — stream=~s", [StreamId]),

    try run_scenario(ChannelName, StreamId)
    after
        cleanup()
    end.

run_scenario(ChannelName, StreamId) ->
    %% Pick the minority node — anything that's NOT the current
    %% leader, so writes survive when we cut it off.
    {ok, LeaderHost, _LeaderNode} = multi_node_chaos:find_leader(?STORE_ID),
    [{MinorityHost, _} | _] =
        [P || {H, _} = P <- multi_node_chaos:cluster_hosts(),
              H =/= LeaderHost],
    ct:pal("leader=~s, will isolate minority=~s", [LeaderHost, MinorityHost]),

    %% Phase 1: steady writes before partition
    Writer = spawn_writer(ChannelName, StreamId),
    erlang:put(writer, Writer),
    ct:pal("phase 1: ~bms steady writes pre-partition", [?PRE_PARTITION_MS]),
    timer:sleep(?PRE_PARTITION_MS),

    %% Phase 2: partition minority off
    ct:pal("phase 2: partitioning ~s off the cluster", [MinorityHost]),
    {ok, MinorityHost, _OtherHosts} =
        multi_node_chaos:partition_minority(MinorityHost),
    erlang:put(partitioned, true),

    %% Phase 3: keep writing to the majority
    ct:pal("phase 3: ~bms writes during partition (majority should accept)",
           [?DURING_PARTITION_MS]),
    timer:sleep(?DURING_PARTITION_MS),

    %% Phase 4: heal
    ct:pal("phase 4: healing partition"),
    ok = multi_node_chaos:heal_partition(),
    erlang:put(partitioned, false),

    %% Phase 5: wait for minority to converge
    ct:pal("phase 5: waiting up to ~bms for minority convergence",
           [?CONVERGE_TIMEOUT_MS]),
    Writer ! stop,
    erlang:put(writer_stopped, true),
    {_Att, SuccessBatches, _Errs} =
        receive {writer_done, A, S, E} -> {A, S, E}
        after 5000 -> ct:fail(writer_did_not_finish)
        end,
    ExpectedCount = length(SuccessBatches) * ?BATCH_SIZE,
    ct:pal("writer ok_batches=~p (=~p events)",
           [length(SuccessBatches), ExpectedCount]),

    %% Open one channel per host and reuse it — creating a fresh
    %% channel per poll iteration accumulates overhead and risks
    %% name-collision if the previous stop hadn't completed.
    MinChan = open_pinned(mnt_min, MinorityHost),
    MajChan = open_pinned(mnt_maj, LeaderHost),
    erlang:put(pinned_channels, [MinChan, MajChan]),

    ok = wait_for_convergence(MinChan, StreamId,
                              ExpectedCount, ?CONVERGE_TIMEOUT_MS),

    %% Phase 6: read back from MINORITY specifically + from MAJORITY,
    %% assert they agree
    {ok, MinorityEvents} = read_from_channel(MinChan, StreamId),
    {ok, MajorityEvents} = read_from_channel(MajChan, StreamId),
    ct:pal("minority read=~p, majority read=~p",
           [length(MinorityEvents), length(MajorityEvents)]),

    %% Assertions.
    %%
    %% NOTE on counts: the writer's `success' count is a LOWER BOUND
    %% on what landed durably. During chaos, some appends return
    %% error to the client AFTER the write actually committed (the
    %% server-side ack got lost in transit). The strong property is
    %% the agreement between minority + majority on the full log
    %% AND a contiguous version sequence — not exact equality with
    %% the writer's bookkeeping.
    MajCount = length(MajorityEvents),
    ?assert(MajCount >= ExpectedCount,
            "majority lost events the writer thought were durable"),
    ?assertEqual(MajCount, length(MinorityEvents),
                 "minority did not converge to majority's log"),

    MinorityVersions = [maps:get(version, E) || E <- MinorityEvents],
    MajorityVersions = [maps:get(version, E) || E <- MajorityEvents],
    ?assertEqual(MajorityVersions, MinorityVersions,
                 "minority + majority versions diverge after heal"),
    ?assertEqual(lists:seq(0, MajCount - 1), MajorityVersions,
                 "version sequence has gaps or duplicates"),

    %% Bonus: per-event prev_event_hash must be byte-identical
    %% across both reads (no log fork)
    MinorityHashes = [maps:get(prev_event_hash, E, <<>>) || E <- MinorityEvents],
    MajorityHashes = [maps:get(prev_event_hash, E, <<>>) || E <- MajorityEvents],
    ?assertEqual(MajorityHashes, MinorityHashes,
                 "prev_event_hash chain diverges across nodes"),

    ok.

%%====================================================================
%% Cleanup
%%====================================================================

cleanup() ->
    case erlang:get(writer_stopped) of
        true -> ok;
        _    ->
            case erlang:get(writer) of
                Pid when is_pid(Pid) ->
                    catch (Pid ! stop),
                    receive {writer_done, _, _, _} -> ok after 2000 -> ok end;
                _ -> ok
            end
    end,
    case erlang:get(partitioned) of
        true ->
            ct:pal("cleanup: healing partition"),
            catch multi_node_chaos:heal_partition();
        _ -> ok
    end,
    case erlang:get(channel) of
        undefined -> ok;
        ChannelName -> catch grpcbox_channel:stop(ChannelName)
    end,
    case erlang:get(pinned_channels) of
        undefined -> ok;
        Pins -> [catch grpcbox_channel:stop(P) || P <- Pins]
    end,
    ok.

%%====================================================================
%% Convergence helper
%%====================================================================

%% Open a single-host channel, register its name, return the name.
open_pinned(Name, Host) ->
    {ok, _} = grpcbox_channel:start_link(
        Name, [{http, Host, 50051, []}], #{}),
    Name.

%% Poll the minority channel until it reports `ExpectedCount' events,
%% or Deadline expires.
wait_for_convergence(ChannelName, StreamId, ExpectedCount, DeadlineMs) ->
    Deadline = erlang:monotonic_time(millisecond) + DeadlineMs,
    converge_loop(ChannelName, StreamId, ExpectedCount, Deadline).

converge_loop(ChannelName, StreamId, ExpectedCount, Deadline) ->
    case read_from_channel(ChannelName, StreamId) of
        {ok, Events} when length(Events) >= ExpectedCount ->
            ct:pal("minority converged: ~p events", [length(Events)]),
            ok;
        {ok, Events} ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    ct:fail({convergence_timeout,
                             #{channel => ChannelName,
                               got  => length(Events),
                               expected => ExpectedCount}});
                false ->
                    timer:sleep(500),
                    converge_loop(ChannelName, StreamId, ExpectedCount, Deadline)
            end;
        _ ->
            timer:sleep(500),
            converge_loop(ChannelName, StreamId, ExpectedCount, Deadline)
    end.

read_from_channel(ChannelName, StreamId) ->
    Req = #{store_id => atom_to_binary(?STORE_ID, utf8),
            stream_id => StreamId,
            start_version => 0,
            count => 100000},
    case reckon_gateway_v_1_stream_service_client:read_stream_forward(
              Req, #{channel => ChannelName}) of
        {ok, #{events := Es}, _} -> {ok, Es};
        Other                    -> {error, Other}
    end.

%%====================================================================
%% Writer (same shape as leader-kill SUITE)
%%====================================================================

spawn_writer(ChannelName, StreamId) ->
    Parent = self(),
    spawn_link(fun() -> writer_loop(Parent, ChannelName, StreamId, 0, [], []) end).

writer_loop(Parent, Channel, StreamId, Attempted, Successes, Errors) ->
    receive
        stop ->
            Parent ! {writer_done, Attempted, Successes, Errors}
    after ?WRITE_RATE_MS ->
        Batch = [#{event_type => <<"partition_event">>,
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
