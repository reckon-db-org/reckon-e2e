%% @doc Local-VM facade — delegates to `evoq_event_store' +
%% `evoq_snapshot_store'. The configured adapter for each is whatever
%% was plumbed in via `set_adapter/1' before calling.
%% @end
-module(reckon_e2e_local_facade).

-behaviour(reckon_e2e_facade).

-export([append/4, read/5, save_snapshot/5, load_snapshot/2]).

append(StoreId, StreamId, ExpectedVersion, Events) ->
    %% evoq_event_store doesn't expose append at the top level — go
    %% through the configured event-store adapter directly. This is
    %% the same pattern reckon-evoq and mem-evoq use internally.
    Adapter = evoq_event_store:get_adapter(),
    Adapter:append(StoreId, StreamId, ExpectedVersion, Events).

read(StoreId, StreamId, FromVersion, Count, Direction) ->
    evoq_event_store:read(StoreId, StreamId, FromVersion, Count, Direction).

save_snapshot(StoreId, StreamId, Version, Data, Metadata) ->
    evoq_snapshot_store:save(StoreId, StreamId, Version, Data, Metadata).

load_snapshot(StoreId, StreamId) ->
    evoq_snapshot_store:load(StoreId, StreamId).
