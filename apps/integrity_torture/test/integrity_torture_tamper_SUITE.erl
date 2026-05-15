%% @doc Adversarial-load scenario: writers + strict readers running
%% as in the concurrent suite, but mid-flight a single event in one
%% targeted stream is tampered.
%%
%% Three properties:
%%
%%   1. The targeted stream surfaces an integrity_violation on the
%%      first strict read that touches it after the tamper.
%%   2. Neighbouring streams continue to read clean. The damage is
%%      contained to the tampered stream.
%%   3. The detection's `kind' is one of mac_mismatch | chain_mismatch
%%      (not e.g. a crash, not e.g. missing_integrity).
%% @end
-module(integrity_torture_tamper_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("reckon_gater/include/reckon_gater_types.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([tampered_stream_caught_neighbours_clean/1]).

-define(STREAM_COUNT,     20).
-define(WRITER_COUNT,     5).
-define(READER_COUNT,     3).
-define(SEED_PER_STREAM,  10).      %% events seeded per stream before chaos
-define(DURATION_MS,      5000).    %% 5s of concurrent load
-define(WORKER_TIMEOUT,   30000).

%%====================================================================
%% CT boilerplate
%%====================================================================

suite() -> [{timetrap, {seconds, 60}}].

all() -> [tampered_stream_caught_neighbours_clean].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(mem_evoq),
    Config.

end_per_suite(_) -> ok.

init_per_testcase(_TC, Config) ->
    {StoreId, Key} = integrity_torture:start_store_with_integrity(),
    [{store_id, StoreId}, {key, Key} | Config].

end_per_testcase(_TC, Config) ->
    catch mem_evoq:stop_store(?config(store_id, Config)),
    ok.

%%====================================================================
%% Scenario
%%====================================================================

tampered_stream_caught_neighbours_clean(Config) ->
    StoreId = ?config(store_id, Config),

    %% Seed all streams so list_streams/0 has work to do and the
    %% tamper has something to land on.
    [seed_stream(StoreId, I, ?SEED_PER_STREAM)
     || I <- lists:seq(0, ?STREAM_COUNT - 1)],

    TargetStream = iolist_to_binary([<<"stream$">>,
                                     integer_to_binary(?STREAM_COUNT div 2)]),
    ct:pal("Seeded ~p streams; will tamper version 3 of ~s",
           [?STREAM_COUNT, TargetStream]),

    Controller = self(),
    Writers = [integrity_torture:spawn_writer(
                   StoreId, ?STREAM_COUNT, ?DURATION_MS, Controller)
               || _ <- lists:seq(1, ?WRITER_COUNT)],
    Readers = [integrity_torture:spawn_strict_reader(
                   StoreId, ?DURATION_MS, Controller)
               || _ <- lists:seq(1, ?READER_COUNT)],

    %% Let workers run briefly, then tamper a seeded event in the target
    %% stream. The tamper happens against a stream all readers are
    %% sampling — eventually at least one reader hits it.
    timer:sleep(500),
    ok = integrity_torture:tamper_event(
        StoreId, TargetStream, 3,
        fun(E) -> E#event{data = #{forged => true, when_ => os:timestamp()}} end),
    ct:pal("Tampered ~s version 3", [TargetStream]),

    Total = length(Writers) + length(Readers),
    #{errors := Errors} =
        integrity_torture:await_workers(Total, ?WORKER_TIMEOUT),

    %% Property 1: at least one violation was observed on the
    %% targeted stream.
    TargetViolations = [E || E = {Sid, {integrity_violation, _}} <- Errors,
                             Sid =:= TargetStream],
    ?assertNotEqual([], TargetViolations),
    ct:pal("Target violations: ~p", [length(TargetViolations)]),

    %% Property 2: no other stream surfaced a violation. The damage
    %% is contained.
    NeighbourViolations =
        [E || E = {Sid, {integrity_violation, _}} <- Errors,
              Sid =/= TargetStream],
    ?assertEqual([], NeighbourViolations),

    %% Property 3: detection kind is one we expect from a payload
    %% mutation — mac_mismatch (because the MAC won't recompute) or
    %% chain_mismatch (because downstream events' prev_event_hash
    %% will no longer line up).
    Kinds = lists:usort(
        [maps:get(kind, Ctx)
         || {_, {integrity_violation, Ctx}} <- TargetViolations]),
    ct:pal("Violation kinds observed: ~p", [Kinds]),
    [?assert(lists:member(K, [mac_mismatch, chain_mismatch]))
     || K <- Kinds],
    ok.

%%====================================================================
%% Helpers
%%====================================================================

seed_stream(StoreId, Idx, N) ->
    StreamId = iolist_to_binary([<<"stream$">>, integer_to_binary(Idx)]),
    Events = [#{event_type => <<"seed_v1">>, data => #{i => I}}
              || I <- lists:seq(1, N)],
    {ok, _} = mem_evoq_adapter:append(StoreId, StreamId, -1, Events),
    ok.
