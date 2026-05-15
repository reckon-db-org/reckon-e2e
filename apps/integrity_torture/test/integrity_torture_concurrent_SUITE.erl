%% @doc Concurrent-load scenario: many writers + several strict
%% readers hammering a fresh integrity-enabled store for a fixed
%% duration. No tampering. The store must produce zero integrity
%% violations and clear a throughput floor.
%%
%% This is the "regression guard" scenario — the unit tests prove
%% the integrity primitives are correct in isolation; this proves
%% they stay correct under serialisation pressure.
%% @end
-module(integrity_torture_concurrent_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([writers_and_readers_run_clean/1]).

%%====================================================================
%% Tunables (kept short for CI; bump locally for soak runs)
%%====================================================================

-define(STREAM_COUNT,     100).
-define(WRITER_COUNT,     5).
-define(READER_COUNT,     3).
-define(DURATION_MS,      10000).   %% 10s
-define(WORKER_TIMEOUT,   30000).   %% deadline + 20s slack
-define(WRITE_FLOOR,      1000).    %% ops across all writers (very loose)
-define(READ_FLOOR,       100).     %% ops across all readers

%%====================================================================
%% CT boilerplate
%%====================================================================

suite() -> [{timetrap, {seconds, 60}}].

all() -> [writers_and_readers_run_clean].

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

writers_and_readers_run_clean(Config) ->
    StoreId = ?config(store_id, Config),

    ct:pal("Spawning ~p writers and ~p strict readers against ~p streams "
           "for ~pms",
           [?WRITER_COUNT, ?READER_COUNT, ?STREAM_COUNT, ?DURATION_MS]),

    Controller = self(),
    Writers = [integrity_torture:spawn_writer(
                   StoreId, ?STREAM_COUNT, ?DURATION_MS, Controller)
               || _ <- lists:seq(1, ?WRITER_COUNT)],
    Readers = [integrity_torture:spawn_strict_reader(
                   StoreId, ?DURATION_MS, Controller)
               || _ <- lists:seq(1, ?READER_COUNT)],

    Total = length(Writers) + length(Readers),
    #{ops := Ops, errors := Errors} =
        integrity_torture:await_workers(Total, ?WORKER_TIMEOUT),

    ct:pal("Workers reported ~p total ops, ~p errors",
           [Ops, length(Errors)]),

    %% Zero integrity violations — the headline property.
    Violations = [E || E <- Errors,
                       case E of
                           {_, {integrity_violation, _}} -> true;
                           _ -> false
                       end],
    ?assertEqual([], Violations),

    %% Loose throughput floor — guards against catastrophic regression.
    ?assert(Ops >= ?WRITE_FLOOR + ?READ_FLOOR,
            io_lib:format("Throughput floor: ops=~p", [Ops])),
    ok.
