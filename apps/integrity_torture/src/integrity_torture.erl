%% @doc Facade for the integrity_torture app — the helpers shared
%% across CT suites.
%%
%% Two roles:
%%
%%   * Spawn writer / reader workers that hammer a store concurrently.
%%   * Tamper an event in storage state directly, the way an on-disk
%%     attacker would (bypassing the API guards).
%%
%% Writers and readers run for a fixed duration and report a count
%% of operations performed back to the controlling process.
%% @end
-module(integrity_torture).

-include_lib("reckon_gater/include/reckon_gater_types.hrl").
-include_lib("evoq/include/evoq_types.hrl").

-export([start_store_with_integrity/0,
         start_store_with_integrity/1,
         spawn_writer/4,
         spawn_strict_reader/3,
         await_workers/2,
         tamper_event/4,
         random_stream/1]).

-type worker_report() :: #{ops := non_neg_integer(),
                          errors := [term()]}.

%%====================================================================
%% Store setup
%%====================================================================

start_store_with_integrity() ->
    start_store_with_integrity(crypto:strong_rand_bytes(32)).

start_store_with_integrity(Key) when is_binary(Key), byte_size(Key) =:= 32 ->
    {ok, _} = application:ensure_all_started(mem_evoq),
    StoreId = unique_store_id(),
    {ok, _} = mem_evoq:start_store(
        StoreId, #{integrity => #{enabled => true, key => Key}}),
    {StoreId, Key}.

unique_store_id() ->
    list_to_atom(
        "integrity_torture_store_" ++
        integer_to_list(erlang:unique_integer([positive]))).

%%====================================================================
%% Workers
%%====================================================================

%% @doc Spawn a writer that appends batched events to random streams
%% for DurationMs. Reports {writer_done, Pid, #{ops, errors}} back
%% to Controller when finished.
-spec spawn_writer(atom(), pos_integer(), pos_integer(), pid()) -> pid().
spawn_writer(StoreId, StreamCount, DurationMs, Controller) ->
    spawn_link(fun() -> writer_loop(StoreId, StreamCount, DurationMs, Controller) end).

writer_loop(StoreId, StreamCount, DurationMs, Controller) ->
    Deadline = erlang:monotonic_time(millisecond) + DurationMs,
    Report = run_writer(StoreId, StreamCount, Deadline, #{ops => 0, errors => []}),
    Controller ! {writer_done, self(), Report}.

run_writer(StoreId, StreamCount, Deadline, #{ops := Ops, errors := Errors} = Acc) ->
    deadline_branch(
        erlang:monotonic_time(millisecond) >= Deadline,
        StoreId, StreamCount, Deadline, Ops, Errors, Acc).

deadline_branch(true, _StoreId, _StreamCount, _Deadline, _Ops, _Errors, Acc) ->
    Acc;
deadline_branch(false, StoreId, StreamCount, Deadline, Ops, Errors, _Acc) ->
    StreamId = random_stream(StreamCount),
    BatchSize = rand:uniform(3),  %% 1..3 events per append
    Batch = [random_event() || _ <- lists:seq(1, BatchSize)],
    NewAcc = record_append_result(
        mem_evoq_adapter:append(StoreId, StreamId, ?ANY_VERSION, Batch),
        Ops, Errors),
    run_writer(StoreId, StreamCount, Deadline, NewAcc).

record_append_result({ok, _}, Ops, Errors) ->
    #{ops => Ops + 1, errors => Errors};
record_append_result({error, Reason}, Ops, Errors) ->
    %% {wrong_expected_version, _, _} is not a real error under
    %% ?ANY_VERSION — guard anyway.
    #{ops => Ops, errors => [Reason | Errors]}.

%% @doc Spawn a strict reader that, for DurationMs, repeatedly picks
%% a random stream and reads forward with `verify => strict'.
%%
%% Reports {reader_done, Pid, #{ops, errors}} where `errors' contains
%% any {integrity_violation, _} the reader saw. The controlling test
%% inspects this list to assert against the scenario expectations.
-spec spawn_strict_reader(atom(), pos_integer(), pid()) -> pid().
spawn_strict_reader(StoreId, DurationMs, Controller) ->
    spawn_link(fun() -> reader_loop(StoreId, DurationMs, Controller) end).

reader_loop(StoreId, DurationMs, Controller) ->
    Deadline = erlang:monotonic_time(millisecond) + DurationMs,
    Report = run_reader(StoreId, Deadline, #{ops => 0, errors => []}),
    Controller ! {reader_done, self(), Report}.

run_reader(StoreId, Deadline, Acc) ->
    Now = erlang:monotonic_time(millisecond),
    reader_deadline_branch(Now >= Deadline, StoreId, Deadline, Acc).

reader_deadline_branch(true, _StoreId, _Deadline, Acc) ->
    Acc;
reader_deadline_branch(false, StoreId, Deadline, Acc) ->
    {ok, Streams} = mem_evoq_adapter:list_streams(StoreId),
    NewAcc = pick_and_read(Streams, StoreId, Acc),
    run_reader(StoreId, Deadline, NewAcc).

pick_and_read([], _StoreId, Acc) ->
    %% No streams to read yet — spin briefly, return for next deadline check.
    timer:sleep(5),
    Acc;
pick_and_read(Streams, StoreId, #{ops := Ops, errors := Errors} = _Acc) ->
    StreamId = lists:nth(rand:uniform(length(Streams)), Streams),
    record_read_result(
        mem_evoq_adapter:read(
            StoreId, StreamId, 0, 1000, forward, #{verify => strict}),
        StreamId, Ops, Errors).

record_read_result({ok, _Events}, _StreamId, Ops, Errors) ->
    #{ops => Ops + 1, errors => Errors};
record_read_result({error, {stream_not_found, _}}, _StreamId, Ops, Errors) ->
    %% Benign — list_streams + read are not atomic, stream may have
    %% been deleted between the two calls.
    #{ops => Ops + 1, errors => Errors};
record_read_result({error, Reason}, StreamId, Ops, Errors) ->
    #{ops => Ops + 1, errors => [{StreamId, Reason} | Errors]}.

%% @doc Block until N reports have been collected. Returns the merged
%% report — total ops + the concatenation of all errors lists.
-spec await_workers(non_neg_integer(), pos_integer()) -> worker_report().
await_workers(N, TimeoutMs) ->
    collect_reports(N, TimeoutMs, #{ops => 0, errors => []}).

collect_reports(0, _Timeout, Acc) ->
    Acc;
collect_reports(N, Timeout, #{ops := Ops, errors := Errors}) ->
    receive
        {writer_done, _Pid, #{ops := WOps, errors := WErrs}} ->
            collect_reports(N - 1, Timeout,
                #{ops => Ops + WOps, errors => WErrs ++ Errors});
        {reader_done, _Pid, #{ops := ROps, errors := RErrs}} ->
            collect_reports(N - 1, Timeout,
                #{ops => Ops + ROps, errors => RErrs ++ Errors})
    after Timeout ->
        error({worker_timeout, #{remaining => N, partial_report =>
            #{ops => Ops, errors => Errors}}})
    end.

%%====================================================================
%% Adversary
%%====================================================================

%% @doc Reach into the store gen_server's state and apply Fun to the
%% event at (StreamId, Version). Same trick the unit tests use. The
%% mutated event is left in place; the test's next strict read
%% observes it.
-spec tamper_event(atom(), binary(), non_neg_integer(),
                   fun((event()) -> event())) -> ok.
tamper_event(StoreId, StreamId, Version, Fun) ->
    {ok, Pid} = mem_evoq_registry:lookup(StoreId),
    _ = sys:replace_state(Pid, fun(State) ->
        Streams = element(3, State),
        Events = maps:get(StreamId, Streams),
        NewEvents = [maybe_apply(E, Version, Fun) || E <- Events],
        setelement(3, State, maps:put(StreamId, NewEvents, Streams))
    end),
    ok.

maybe_apply(#event{version = V} = E, V, Fun) -> Fun(E);
maybe_apply(E, _, _) -> E.

%%====================================================================
%% Helpers
%%====================================================================

random_stream(StreamCount) ->
    N = rand:uniform(StreamCount) - 1,
    iolist_to_binary([<<"stream$">>, integer_to_binary(N)]).

random_event() ->
    #{event_type => <<"torture_event_v1">>,
      data => #{n => rand:uniform(1000000),
                seed => crypto:strong_rand_bytes(8)}}.
