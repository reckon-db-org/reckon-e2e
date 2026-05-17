%% @doc Replication lag under sustained write load.
%%
%% Slam the cluster with bursty writes for a fixed window. Sample
%% every node's event count every second to measure how far followers
%% drift behind the leader. Stop the load. Verify:
%%
%%   * Followers converge to leader within a bounded deadline
%%   * All four nodes' final logs are byte-identical (same versions,
%%     same prev_event_hash chain — no forks)
%%
%% Failure modes worth surfacing:
%%   * Unbounded lag growth: spread keeps widening even though leader
%%     stops writing → replication is failing, not just slow
%%   * Non-convergence: nodes settle at different counts → a real
%%     fork or a dropped log segment
%%   * Hash chain mismatch: same count but different contents → silent
%%     corruption (the worst kind of bug)
%% @end
-module(multi_node_replication_lag_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([replication_lag_under_load/1]).

-define(STORE_ID, default_store).
-define(LOAD_DURATION_MS, 15000).
-define(BATCH_SIZE, 20).
-define(SAMPLE_INTERVAL_MS, 2000).
-define(SAMPLE_READ_COUNT, 50000).
-define(CONVERGE_TIMEOUT_MS, 45000).

suite() -> [{timetrap, {minutes, 3}}].

all() -> [replication_lag_under_load].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(grpcbox),
    Config.

end_per_suite(_) -> ok.

init_per_testcase(replication_lag_under_load, Config) ->
    case os:getenv("RECKON_E2E_CLUSTER") of
        "1" -> Config;
        _   -> {skip, "RECKON_E2E_CLUSTER not set"}
    end.

end_per_testcase(_, _) -> ok.

%%====================================================================
%% Scenario
%%====================================================================

replication_lag_under_load(_Config) ->
    process_flag(trap_exit, true),

    %% Per-host pinned channels so each sample reads from a specific
    %% node — the whole point is to compare across nodes.
    PinnedChans =
        [{Host, open_pinned(channel_name(Host), Host)} ||
         {Host, _Node} <- multi_node_chaos:cluster_hosts()],
    erlang:put(pinned_chans, [Chan || {_, Chan} <- PinnedChans]),

    %% Writer uses a multi-endpoint channel so an unlucky single-node
    %% blip doesn't take it down — the point is to test replication
    %% under load, not failover.
    WriterEndpoints = [{http, H, 50051, []} ||
                       {H, _} <- multi_node_chaos:cluster_hosts()],
    WriterChan = mnt_replag_writer_channel,
    {ok, _} = grpcbox_channel:start_link(WriterChan, WriterEndpoints, #{}),
    erlang:put(writer_chan, WriterChan),

    Nonce = binary:encode_hex(crypto:strong_rand_bytes(8)),
    StreamId = <<"replag$", Nonce/binary>>,
    ct:pal("scenario starting — stream=~s", [StreamId]),

    try run_scenario(PinnedChans, WriterChan, StreamId)
    after
        cleanup()
    end.

run_scenario(PinnedChans, WriterChan, StreamId) ->
    %% Phase 1: start writer at no-throttle burst mode
    Writer = spawn_writer(WriterChan, StreamId),
    erlang:put(writer, Writer),

    %% Phase 2: sample for LOAD_DURATION_MS
    ct:pal("phase 1: ~bms of bursty writes, sampling every ~bms",
           [?LOAD_DURATION_MS, ?SAMPLE_INTERVAL_MS]),
    Samples = sample_loop(PinnedChans, StreamId,
                          erlang:monotonic_time(millisecond),
                          ?LOAD_DURATION_MS, []),
    report_samples(Samples),

    %% Phase 3: stop writer, drain stats
    Writer ! stop,
    erlang:put(writer_stopped, true),
    {Attempted, Successes, Errors} =
        receive {writer_done, A, S, E} -> {A, S, E}
        after 10000 -> ct:fail(writer_did_not_finish)
        end,
    ExpectedCount = length(Successes) * ?BATCH_SIZE,
    ct:pal("writer: attempted=~p ok_batches=~p (=~p events) errs=~p",
           [Attempted, length(Successes), ExpectedCount, length(Errors)]),

    %% Phase 4: wait for convergence
    ct:pal("phase 2: waiting up to ~bms for cluster convergence",
           [?CONVERGE_TIMEOUT_MS]),
    {ok, ConvergedCount} =
        wait_for_convergence(PinnedChans, StreamId,
                             ExpectedCount, ?CONVERGE_TIMEOUT_MS),
    ct:pal("converged at ~p events", [ConvergedCount]),

    %% Phase 5: byte-identical log assertion
    AllReads = [{Host, read_from_channel(Chan, StreamId)} ||
                {Host, Chan} <- PinnedChans],
    {RefHost, {ok, RefEvents}} = hd(AllReads),
    RefVersions = [maps:get(version, E) || E <- RefEvents],
    RefHashes   = [maps:get(prev_event_hash, E, <<>>) || E <- RefEvents],
    ct:pal("reference (~s): ~p events, versions ~p..~p",
           [RefHost, length(RefEvents),
            hd_or(RefVersions, '-'), last_or(RefVersions, '-')]),

    %% Every other node must match byte-for-byte
    [begin
         ?assertEqual({ok, ok}, {ok, ok}),  %% noop to keep block uniform
         {ok, Es} = Result,
         Vs = [maps:get(version, E) || E <- Es],
         Hs = [maps:get(prev_event_hash, E, <<>>) || E <- Es],
         ?assertEqual(RefVersions, Vs,
                      io_lib:format("~s versions differ from ~s",
                                    [Host, RefHost])),
         ?assertEqual(RefHashes, Hs,
                      io_lib:format("~s prev_event_hash differs from ~s",
                                    [Host, RefHost]))
     end || {Host, Result} <- tl(AllReads)],

    %% Sanity: writer's success count is a LOWER bound on durable
    %% events (same caveat as the partition-heal scenario).
    ?assert(length(RefEvents) >= ExpectedCount,
            "converged log smaller than writer-acked count"),

    %% Sanity: version sequence is dense (0..N-1)
    ?assertEqual(lists:seq(0, length(RefEvents) - 1), RefVersions,
                 "version sequence has gaps or duplicates"),

    ok.

%%====================================================================
%% Sampling
%%====================================================================

sample_loop(_PinnedChans, _StreamId, _StartT, BudgetMs, Samples)
        when BudgetMs =< 0 ->
    lists:reverse(Samples);
sample_loop(PinnedChans, StreamId, StartT, BudgetMs, Samples) ->
    SampleStart = erlang:monotonic_time(millisecond),
    Counts = [{Host, count_from_channel(Chan, StreamId)} ||
              {Host, Chan} <- PinnedChans],
    SampleAt = SampleStart - StartT,
    Sample = #{at_ms => SampleAt, counts => Counts},
    SampleElapsed = erlang:monotonic_time(millisecond) - SampleStart,
    SleepFor = max(0, ?SAMPLE_INTERVAL_MS - SampleElapsed),
    timer:sleep(SleepFor),
    Remaining = BudgetMs - ?SAMPLE_INTERVAL_MS,
    sample_loop(PinnedChans, StreamId, StartT, Remaining,
                [Sample | Samples]).

count_from_channel(Chan, StreamId) ->
    case read_from_channel_sized(Chan, StreamId, ?SAMPLE_READ_COUNT) of
        {ok, Es}    -> length(Es);
        {error, _}  -> -1
    end.

read_from_channel_sized(ChannelName, StreamId, Count) ->
    Req = #{store_id => atom_to_binary(?STORE_ID, utf8),
            stream_id => StreamId,
            start_version => 0,
            count => Count},
    try reckon_gateway_v_1_stream_service_client:read_stream_forward(
            Req, #{channel => ChannelName, timeout => 3000}) of
        {ok, #{events := Es}, _} -> {ok, Es};
        Other                    -> {error, Other}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

report_samples(Samples) ->
    ct:pal("sample window — ~p samples", [length(Samples)]),
    MaxSpread =
        lists:foldl(
            fun(#{at_ms := T, counts := Counts}, AccMax) ->
                Values = [V || {_, V} <- Counts, V >= 0],
                case Values of
                    [] -> AccMax;
                    _ ->
                        Max = lists:max(Values),
                        Min = lists:min(Values),
                        Spread = Max - Min,
                        ct:pal("  t=~bms counts=~p spread=~p",
                               [T, Counts, Spread]),
                        erlang:max(AccMax, Spread)
                end
            end, 0, Samples),
    ct:pal("peak inter-node spread observed during load: ~p events",
           [MaxSpread]),
    MaxSpread.

%%====================================================================
%% Convergence
%%====================================================================

wait_for_convergence(PinnedChans, StreamId, ExpectedCount, DeadlineMs) ->
    Deadline = erlang:monotonic_time(millisecond) + DeadlineMs,
    converge_loop(PinnedChans, StreamId, ExpectedCount, Deadline).

converge_loop(PinnedChans, StreamId, ExpectedCount, Deadline) ->
    Counts = [{Host, count_from_channel(Chan, StreamId)} ||
              {Host, Chan} <- PinnedChans],
    Values = [V || {_, V} <- Counts, V >= 0],
    AllAgree = (length(Values) =:= length(Counts))
        andalso (lists:max(Values) =:= lists:min(Values)),
    AtOrAbove = (Values =/= [])
        andalso (lists:min(Values) >= ExpectedCount),
    case AllAgree andalso AtOrAbove of
        true ->
            {ok, hd(Values)};
        false ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    ct:fail({convergence_timeout,
                             #{counts => Counts,
                               expected_at_least => ExpectedCount}});
                false ->
                    timer:sleep(500),
                    converge_loop(PinnedChans, StreamId,
                                  ExpectedCount, Deadline)
            end
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
                    catch (W ! stop),
                    receive {writer_done, _, _, _} -> ok after 2000 -> ok end;
                _ -> ok
            end
    end,
    case erlang:get(writer_chan) of
        undefined -> ok;
        WC -> catch grpcbox_channel:stop(WC)
    end,
    case erlang:get(pinned_chans) of
        undefined -> ok;
        Pins -> [catch grpcbox_channel:stop(P) || P <- Pins]
    end,
    ok.

%%====================================================================
%% gRPC helpers
%%====================================================================

open_pinned(Name, Host) ->
    {ok, _} = grpcbox_channel:start_link(
        Name, [{http, Host, 50051, []}], #{}),
    Name.

%% Per-host channel name needs to be a unique atom. The set of cluster
%% hosts is fixed at four — hard-code them rather than risk dynamic
%% atom creation.
channel_name("beam00.lab") -> mnt_replag_chan_beam00;
channel_name("beam01.lab") -> mnt_replag_chan_beam01;
channel_name("beam02.lab") -> mnt_replag_chan_beam02;
channel_name("beam03.lab") -> mnt_replag_chan_beam03.

%% Final read for byte-identical assertion — needs full log,
%% accepts slower call.
read_from_channel(ChannelName, StreamId) ->
    Req = #{store_id => atom_to_binary(?STORE_ID, utf8),
            stream_id => StreamId,
            start_version => 0,
            count => 200000},
    try reckon_gateway_v_1_stream_service_client:read_stream_forward(
            Req, #{channel => ChannelName, timeout => 10000}) of
        {ok, #{events := Es}, _} -> {ok, Es};
        Other                    -> {error, Other}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

%%====================================================================
%% Writer — burst mode, no throttle
%%====================================================================

spawn_writer(ChannelName, StreamId) ->
    Parent = self(),
    spawn_link(fun() -> writer_loop(Parent, ChannelName, StreamId, 0, [], []) end).

%% No `after Sleep' clause — we want to slam the cluster. The writer
%% only checks for the stop signal via a non-blocking receive between
%% appends so the loop stays tight.
writer_loop(Parent, Channel, StreamId, Attempted, Successes, Errors) ->
    case stop_requested() of
        true ->
            Parent ! {writer_done, Attempted, Successes, Errors};
        false ->
            Batch = [#{event_type => <<"replag_event">>,
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

stop_requested() ->
    receive
        stop -> true
    after 0 -> false
    end.

%%====================================================================
%% Misc
%%====================================================================

hd_or([], Default)   -> Default;
hd_or([H | _], _)    -> H.

last_or([], Default) -> Default;
last_or(L, _)        -> lists:last(L).
