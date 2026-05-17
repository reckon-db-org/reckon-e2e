%% @doc The "basic" adapter-swap scenario: append a few events,
%% read them back, snapshot midway, load the snapshot. Returns a
%% map of observed outcomes for behavioural-equivalence comparison.
%%
%% Scenarios are facade- and transport-blind. The Driver carries
%% the StoreId and the `reckon_e2e_facade' module to call. Same
%% scenario runs against:
%%
%%   * mem-evoq + local facade
%%   * embedded reckon-db + local facade
%%   * deployed reckon-gateway + gRPC facade (step 3)
%% @end
-module(adapter_swap_basic_scenario).

-export([run/1]).

-type driver() :: #{store_id := atom(),
                    facade   := module()}.

-spec run(driver()) -> map().
run(#{store_id := StoreId, facade := Facade}) ->
    %% Stream-id unique per run — cross-VM uniqueness needed because
    %% the clustered store is long-running while each `rebar3 ct'
    %% invocation gets a fresh BEAM. `unique_integer/1' resets per
    %% VM, so we use a crypto-random nonce instead.
    StreamId = iolist_to_binary([
        <<"swapagg-">>,
        binary:encode_hex(crypto:strong_rand_bytes(8))
    ]),

    {ok, AppendedVersion} = Facade:append(StoreId, StreamId, -1,
        [#{event_type => <<"swap_e_v1">>, data => #{n => 1}},
         #{event_type => <<"swap_e_v1">>, data => #{n => 2}},
         #{event_type => <<"swap_e_v1">>, data => #{n => 3}}]),

    {ok, Events} = Facade:read(StoreId, StreamId, 0, 10, forward),

    ok = Facade:save_snapshot(StoreId, StreamId, 1,
        #{state => mid_stream, processed_count => 2},
        #{trace_id => <<"swap-trace-1">>}),

    {ok, Snapshot} = Facade:load_snapshot(StoreId, StreamId),

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

%% Strip envelope keys from a flat-map event, leaving just the
%% payload. Envelope keys come from event_to_map/1's envelope branch.
drop_envelope(Event) ->
    Envelope = [event_id, event_type, stream_id, version, metadata,
                timestamp, epoch_us, data_content_type,
                metadata_content_type, prev_event_hash, tags],
    maps:without(Envelope, Event).
