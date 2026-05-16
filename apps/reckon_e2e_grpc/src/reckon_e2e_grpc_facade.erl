%% @doc gRPC facade — talks to a deployed reckon-gateway via the
%% generated grpcbox client stubs.
%%
%% Channel lifecycle is the harness's job (`with_clustered_reckon_store/1');
%% the facade reads a well-known channel name configured via
%% `application:set_env(reckon_e2e_grpc, channel, Name)' at the
%% start of each test. Same `reckon_e2e_facade' callback surface as
%% `reckon_e2e_local_facade' — scenarios are transport-blind.
%%
%% Wire format for `data' and `metadata' fields is canonical JSON
%% (matching what reckon-db / mem-evoq use internally via
%% reckon_gater_canonical). The facade encodes Erlang maps to JSON
%% on send and decodes on receive.
%% @end
-module(reckon_e2e_grpc_facade).

-behaviour(reckon_e2e_facade).

-export([append/4, read/5, save_snapshot/5, load_snapshot/2]).

-export([start_channel/2, stop_channel/0]).

%%====================================================================
%% Channel lifecycle (called by the harness)
%%====================================================================

%% @doc Open a named gRPC channel to {Host, Port}. The facade reaches
%% it by reading `{reckon_e2e_grpc, channel}' from application env.
start_channel(Host, Port) when is_list(Host), is_integer(Port) ->
    Name = channel_name(),
    Endpoint = {http, Host, Port, []},
    {ok, _Pid} = grpcbox_channel:start_link(Name, [Endpoint], #{}),
    application:set_env(reckon_e2e_grpc, channel, Name),
    ok.

stop_channel() ->
    Name = application:get_env(reckon_e2e_grpc, channel, undefined),
    stop_named(Name).

stop_named(undefined) ->
    ok;
stop_named(Name) ->
    catch grpcbox_channel:stop(Name),
    application:unset_env(reckon_e2e_grpc, channel),
    ok.

channel_name() -> reckon_e2e_clustered.

%%====================================================================
%% Facade callbacks
%%====================================================================

append(StoreId, StreamId, ExpectedVersion, Events) ->
    Req = #{
        store_id         => atom_to_binary(StoreId, utf8),
        stream_id        => StreamId,
        expected_version => ExpectedVersion,
        events           => [event_to_proposed(E) || E <- Events]
    },
    map_append_result(
        reckon_gateway_v_1_stream_service_client:append_events(
            Req, call_options())).

map_append_result({ok, #{version := V}, _Meta}) ->
    {ok, V};
map_append_result({error, _} = E) ->
    E;
map_append_result(Other) ->
    {error, {grpc_error, Other}}.

read(StoreId, StreamId, FromVersion, Count, Direction) ->
    Req = #{
        store_id      => atom_to_binary(StoreId, utf8),
        stream_id     => StreamId,
        start_version => FromVersion,
        count         => Count
    },
    Client = client_for_direction(Direction),
    map_read_result(
        Client(Req, call_options())).

client_for_direction(forward) ->
    fun reckon_gateway_v_1_stream_service_client:read_stream_forward/2;
client_for_direction(backward) ->
    fun reckon_gateway_v_1_stream_service_client:read_stream_backward/2.

map_read_result({ok, #{events := Events}, _Meta}) ->
    {ok, [recorded_to_flat_map(E) || E <- Events]};
map_read_result({error, _} = E) ->
    E;
map_read_result(Other) ->
    {error, {grpc_error, Other}}.

save_snapshot(StoreId, StreamId, Version, Data, Metadata) ->
    Req = #{
        store_id    => atom_to_binary(StoreId, utf8),
        source_uuid => StreamId,
        stream_uuid => StreamId,
        version     => Version,
        data        => encode_payload(Data),
        metadata    => encode_payload(Metadata)
    },
    map_record_result(
        reckon_gateway_v_1_snapshot_service_client:record_snapshot(
            Req, call_options())).

map_record_result({ok, _Resp, _Meta}) -> ok;
map_record_result({error, _} = E)     -> E;
map_record_result(Other)              -> {error, {grpc_error, Other}}.

load_snapshot(StoreId, StreamId) ->
    Req = #{
        store_id    => atom_to_binary(StoreId, utf8),
        source_uuid => StreamId,
        stream_uuid => StreamId
    },
    pick_latest_snapshot(
        reckon_gateway_v_1_snapshot_service_client:list_snapshots(
            Req, call_options()),
        StreamId).

pick_latest_snapshot({ok, #{snapshots := []}, _Meta}, _StreamId) ->
    {error, not_found};
pick_latest_snapshot({ok, #{snapshots := Snapshots}, _Meta}, StreamId) ->
    Latest = lists:foldl(fun pick_higher/2, undefined, Snapshots),
    {ok, snapshot_to_flat_map(StreamId, Latest)};
pick_latest_snapshot({error, _} = E, _StreamId) ->
    E;
pick_latest_snapshot(Other, _StreamId) ->
    {error, {grpc_error, Other}}.

pick_higher(S, undefined) -> S;
pick_higher(#{version := V} = S, #{version := Av} = Acc) ->
    higher(S, Acc, V, Av).

higher(S, _Acc, V, Av) when V > Av -> S;
higher(_S, Acc, _V, _Av)            -> Acc.

%%====================================================================
%% Encoding helpers
%%====================================================================
%%
%% `data' and `metadata' are wire-format bytes. Scenarios pass Erlang
%% maps; encode via the JSON facility built into OTP 26+. Empty
%% payloads stay as <<>>.

event_to_proposed(#{event_type := Type} = E) ->
    Map0 = #{event_type => Type,
             data       => encode_payload(maps:get(data, E, #{}))},
    maps_put_optional(metadata, encode_payload(maps:get(metadata, E, #{})), Map0).

maps_put_optional(_Key, <<>>, Map) -> Map;
maps_put_optional(Key, Value, Map) -> maps:put(Key, Value, Map).

encode_payload(Bin) when is_binary(Bin) ->
    Bin;
encode_payload(Map) when is_map(Map), map_size(Map) =:= 0 ->
    <<>>;
encode_payload(Map) when is_map(Map) ->
    iolist_to_binary(json:encode(Map)).

decode_payload(<<>>) ->
    #{};
decode_payload(Bin) when is_binary(Bin) ->
    json:decode(Bin).

%% Convert a recorded_event() wire map to the flat envelope+payload
%% shape that evoq's event_to_map/1 produces, so scenario outcomes
%% match across local and gRPC facades.
recorded_to_flat_map(#{} = E) ->
    Envelope = #{
        event_id              => maps:get(event_id, E, undefined),
        event_type            => maps:get(event_type, E, undefined),
        stream_id             => maps:get(stream_id, E, undefined),
        version               => maps:get(version, E, 0),
        metadata              => decode_payload(maps:get(metadata, E, <<>>)),
        timestamp             => maps:get(timestamp, E, 0),
        epoch_us              => maps:get(epoch_us, E, 0),
        data_content_type     => maps:get(data_content_type, E, <<>>),
        metadata_content_type => maps:get(metadata_content_type, E, <<>>),
        tags                  => maps:get(tags, E, []),
        prev_event_hash       => prev_hash(maps:get(prev_event_hash, E, <<>>))
    },
    Data = decode_payload(maps:get(data, E, <<>>)),
    merge_payload_into_envelope(Data, Envelope).

prev_hash(<<>>)                          -> undefined;
prev_hash(Bin) when is_binary(Bin)       -> Bin.

merge_payload_into_envelope(Data, Envelope) when is_map(Data) ->
    maps:merge(Data, Envelope);
merge_payload_into_envelope(_, Envelope) ->
    Envelope.

%% snapshot_record() wire map → flat map matching evoq_snapshot_store
%% return shape.
%% The gateway's RecordSnapshot handler currently stores the FULL
%% request map (with `data`, `metadata`, `version` keys) as the
%% snapshot's Data field — so when ListSnapshots returns it, our
%% .data is a JSON-encoded wrapper, not the original payload.
%% Same wrap-on-store bug class as reckon-evoq 2.1.0 had; the proper
%% server-side fix belongs in reckon-gateway. Until then, unwrap
%% defensively here.
snapshot_to_flat_map(StreamId, #{} = S) ->
    RawData = decode_payload(maps:get(data, S, <<>>)),
    RawMetadata = decode_payload(maps:get(metadata, S, <<>>)),
    {InnerData, InnerMetadata} = unwrap_snapshot_payload(RawData, RawMetadata),
    #{
        stream_id => StreamId,
        version   => maps:get(version, S, 0),
        data      => InnerData,
        metadata  => InnerMetadata,
        timestamp => maps:get(timestamp, S, 0)
    }.

%% If the decoded data has the wrapper shape (`{<<"data">>: ..., <<"metadata">>: ...}'),
%% unwrap. Otherwise pass through.
unwrap_snapshot_payload(#{<<"data">> := Inner} = Wrapper, _OuterMeta) ->
    InnerMeta = maps:get(<<"metadata">>, Wrapper, #{}),
    {Inner, InnerMeta};
unwrap_snapshot_payload(Data, Metadata) ->
    {Data, Metadata}.

%%====================================================================
%% Internal
%%====================================================================

call_options() ->
    #{channel => application:get_env(reckon_e2e_grpc, channel, channel_name())}.
