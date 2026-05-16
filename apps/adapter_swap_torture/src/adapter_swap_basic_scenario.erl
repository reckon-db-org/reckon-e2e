%% @doc The "basic" adapter-swap scenario: append a few events,
%% read them back, snapshot midway, load the snapshot. Returns a
%% map of observed outcomes for behavioural-equivalence comparison.
%%
%% Drives through `evoq_event_store' + `evoq_snapshot_store' — the
%% actual seam consumers use. The configured adapter is whatever
%% `adapter_swap_torture' has plumbed in.
%% @end
-module(adapter_swap_basic_scenario).

-export([run/1]).

%% @doc Run the scenario against StoreId. Returns the outcome map.
run(StoreId) ->
    StreamId = <<"swap-agg$1">>,

    %% 1. Append three events.
    {ok, AppendedVersion} = evoq_event_store_append(StoreId, StreamId),

    %% 2. Read them back through evoq.
    {ok, Events} = evoq_event_store:read(
        StoreId, StreamId, 0, 10, forward),

    %% 3. Save a snapshot at mid-stream.
    ok = evoq_snapshot_store:save(
        StoreId, StreamId, 1,
        #{state => mid_stream, processed_count => 2},
        #{trace_id => <<"swap-trace-1">>}),

    %% 4. Load it back.
    {ok, Snapshot} = evoq_snapshot_store:load(StoreId, StreamId),

    #{
        appended_last_version => AppendedVersion,
        read_count            => length(Events),
        read_versions         => [maps:get(version, E) || E <- Events],
        read_event_types      => [maps:get(event_type, E) || E <- Events],
        read_payloads         => [drop_envelope(E) || E <- Events],
        snapshot_version      => maps:get(version, Snapshot),
        snapshot_data         => maps:get(data, Snapshot),
        snapshot_metadata     => maps:get(metadata, Snapshot)
    }.

%%====================================================================
%% Internal
%%====================================================================

%% mem-evoq exposes append on the adapter directly; reckon-evoq goes
%% via the same surface. evoq_event_store doesn't currently expose
%% append as a top-level helper, so call the configured adapter.
evoq_event_store_append(StoreId, StreamId) ->
    Adapter = evoq_event_store:get_adapter(),
    Adapter:append(StoreId, StreamId, -1,
        [#{event_type => <<"swap_e_v1">>, data => #{n => 1}},
         #{event_type => <<"swap_e_v1">>, data => #{n => 2}},
         #{event_type => <<"swap_e_v1">>, data => #{n => 3}}]).

%% Strip envelope keys from a flat-map event, leaving just the
%% payload. Envelope keys come from event_to_map/1's envelope branch.
drop_envelope(Event) ->
    Envelope = [event_id, event_type, stream_id, version, metadata,
                timestamp, epoch_us, data_content_type,
                metadata_content_type, prev_event_hash, tags],
    maps:without(Envelope, Event).
