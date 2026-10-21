%% @doc The one test nothing in the workspace could answer until now: write
%% an event THROUGH the evoq adapter and find it again by its lineage
%% metadata through the store's own query.
%%
%% Every piece of this was tested in isolation and the chain was not:
%% reckon-evoq tested that it binary-keys the metadata (against a mocked
%% gateway), and reckon-db tested that read_by_metadata matches binary keys
%% (against events it wrote itself). Neither could see the other, so a
%% mismatch between them - an adapter writing atom keys into an index built
%% for binary ones - passed both suites and made every lineage query answer
%% nothing. This suite runs the whole path in one VM: adapter -> gater ->
%% Khepri -> {meta, Key} index -> read_by_metadata.
%%
%% See reckon-db-org/evoq#22.
-module(lineage_roundtrip_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("reckon_db/include/reckon_db.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([appended_event_is_findable_by_causation_id/1,
         an_applications_own_metadata_key_is_stored_as_written/1,
         an_event_with_no_lineage_is_not_found_by_one/1]).

suite() -> [{timetrap, {minutes, 2}}].

all() ->
    [appended_event_is_findable_by_causation_id,
     an_applications_own_metadata_key_is_stored_as_written,
     an_event_with_no_lineage_is_not_found_by_one].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    RaDataDir = "/tmp/reckon_e2e_lineage_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),

    ok = start_pg(?RECKON_DB_PG_SCOPE),
    ok = start_pg(reckon_gater_pg),
    {ok, _} = application:ensure_all_started(reckon_gater),

    %% The adapter, plugged into evoq as its event store.
    application:set_env(evoq, store_id, lineage_roundtrip_store_0),
    application:set_env(evoq, event_store_adapter, reckon_evoq_adapter),
    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    application:stop(reckon_gater),
    os:cmd("rm -rf " ++ proplists:get_value(ra_data_dir, Config)),
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    TCStr = atom_to_list(TestCase),
    DataDir = "/tmp/reckon_e2e_lineage_" ++ TCStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    StoreId = list_to_atom("lineage_" ++ TCStr ++ "_" ++ Rand),
    case khepri:start(DataDir, StoreId) of
        {ok, _} ->
            ok = khepri:put(StoreId, [streams], #{}),
            ok = khepri:put(StoreId, [subscriptions], #{}),
            Worker = start_gateway_worker(StoreId, DataDir),
            timer:sleep(200),
            [{data_dir, DataDir}, {store_id, StoreId}, {worker, Worker} | Config];
        {error, Reason} ->
            ct:fail("Failed to start Khepri: ~p", [Reason])
    end.

end_per_testcase(_TestCase, Config) ->
    _ = (try gen_server:stop(proplists:get_value(worker, Config))
         catch _:_ -> ok end),
    khepri:stop(proplists:get_value(store_id, Config)),
    os:cmd("rm -rf " ++ proplists:get_value(data_dir, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% GIVEN the evoq adapter and a store indexing causation_id
%% WHEN an event is appended through the adapter with atom-keyed lineage
%%      metadata, the way evoq writes it
%% THEN the store's own query finds it by that key
appended_event_is_findable_by_causation_id(Config) ->
    StoreId = store(Config),
    StreamId = reckon_gater_stream_id:new(<<"lineage">>),
    {ok, _} = reckon_evoq_adapter:append(
                StoreId, StreamId, -1,
                [nested_event(<<"thing_happened_v1">>,
                              #{causation_id => <<"cmd-1">>,
                                correlation_id => <<"conv-1">>})]),

    {ok, Found} = reckon_gater_api:read_by_metadata(StoreId, <<"causation_id">>,
                                                    <<"cmd-1">>),
    ?assertEqual(1, length(Found)),
    ?assertEqual(<<"thing_happened_v1">>, event_type(Found)),

    {ok, Same} = reckon_gater_api:read_by_metadata(StoreId, <<"correlation_id">>,
                                                   <<"conv-1">>),
    ?assertEqual(1, length(Same)).

%% The adapter renames the reserved keys and nothing else, so an
%% application's own metadata key is stored exactly as the application wrote
%% it. Which means an application that wants its key indexed writes it as a
%% binary, and gets found - and one that writes an atom is stored under the
%% atom and is not. Both halves are the contract, so both are asserted.
an_applications_own_metadata_key_is_stored_as_written(Config) ->
    StoreId = store(Config),
    StreamId = reckon_gater_stream_id:new(<<"lineage">>),
    {ok, _} = reckon_evoq_adapter:append(
                StoreId, StreamId, -1,
                [nested_event(<<"thing_happened_v1">>,
                              #{causation_id => <<"cmd-2">>,
                                correlation_id => <<"conv-2">>,
                                <<"user_id">> => <<"usr-1">>})]),
    {ok, _} = reckon_evoq_adapter:append(
                StoreId, StreamId, -2,
                [nested_event(<<"thing_happened_v1">>,
                              #{causation_id => <<"cmd-2b">>,
                                correlation_id => <<"conv-2b">>,
                                user_id => <<"usr-1">>})]),

    %% Written as a binary, indexed, found.
    {ok, [Found]} = reckon_gater_api:read_by_metadata(StoreId, <<"user_id">>,
                                                      <<"usr-1">>),
    ?assertEqual(<<"thing_happened_v1">>, event_type([Found])),

    %% A value nobody wrote finds nothing, which is also what a query for the
    %% wrong key spelling looks like: only the three reserved lineage keys are
    %% translated, so an application key written as an atom is stored under
    %% the atom and a query for it answers {ok, []}.
    ?assertMatch({ok, []},
                 reckon_gater_api:read_by_metadata(StoreId, <<"user_id">>,
                                                   <<"no-such-value">>)).

%% The query is a query: a value nobody wrote finds nothing.
an_event_with_no_lineage_is_not_found_by_one(Config) ->
    StoreId = store(Config),
    StreamId = reckon_gater_stream_id:new(<<"lineage">>),
    {ok, _} = reckon_evoq_adapter:append(
                StoreId, StreamId, -1,
                [nested_event(<<"thing_happened_v1">>,
                              #{causation_id => <<"cmd-3">>,
                                correlation_id => <<"conv-3">>})]),

    ?assertMatch({ok, []},
                 reckon_gater_api:read_by_metadata(StoreId, <<"causation_id">>,
                                                   <<"cmd-never-written">>)).

%%====================================================================
%% Helpers
%%====================================================================

store(Config) ->
    proplists:get_value(store_id, Config).

%% evoq's shape: a nested map with atom-keyed metadata, which is what
%% evoq_aggregate produces before the adapter sees it.
nested_event(Type, Metadata) ->
    #{event_type => Type,
      data => #{<<"k">> => 1},
      metadata => Metadata}.

event_type([#event{event_type = Type} | _]) -> Type.

start_pg(Scope) ->
    case pg:start(Scope) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end.

%% The {meta, Key} declarations are what the whole test is about: without
%% them the lookup is a whole-store scan that matches the binary spelling
%% anyway, and the test would not be proving the index works.
start_gateway_worker(StoreId, DataDir) ->
    GatewayConfig = #store_config{
        store_id = StoreId,
        data_dir = DataDir,
        mode = single,
        timeout = 5000,
        writer_pool_size = 1,
        reader_pool_size = 1,
        gateway_pool_size = 1,
        options = #{},
        indexes = [{meta, <<"causation_id">>},
                   {meta, <<"correlation_id">>},
                   {meta, <<"user_id">>}]
    },
    ok = reckon_db_index_config:load(GatewayConfig),
    {ok, Pid} = reckon_db_gateway_worker:start_link(GatewayConfig),
    Pid.
