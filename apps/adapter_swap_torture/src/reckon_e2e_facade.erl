%% @doc Facade contract for torture scenarios.
%%
%% Scenarios call `Facade:append/4', `Facade:read/5', etc. The
%% configured facade decides how those calls cross the wire:
%%
%%   * `reckon_e2e_local_facade'  — delegates to `evoq_event_store'
%%     and `evoq_snapshot_store' in the same VM. Used by single-node
%%     scenarios against mem-evoq or embedded reckon-db.
%%   * `reckon_e2e_grpc_facade'   — translates calls to gRPC against
%%     a deployed reckon-gateway. Used by clustered scenarios.
%%     (Lands in step 3 of `docs/CLUSTERED_FIXTURE_DESIGN.md'.)
%%
%% The contract is identical across facades so scenarios are
%% adapter-blind and transport-blind.
%% @end
-module(reckon_e2e_facade).

-callback append(StoreId :: atom(),
                 StreamId :: binary(),
                 ExpectedVersion :: integer(),
                 Events :: [map()]) ->
    {ok, NewVersion :: non_neg_integer()} | {error, term()}.

-callback read(StoreId :: atom(),
               StreamId :: binary(),
               FromVersion :: non_neg_integer(),
               Count :: pos_integer(),
               Direction :: forward | backward) ->
    {ok, [map()]} | {error, term()}.

-callback save_snapshot(StoreId :: atom(),
                        StreamId :: binary(),
                        Version :: non_neg_integer(),
                        Data :: map() | binary(),
                        Metadata :: map()) ->
    ok | {error, term()}.

-callback load_snapshot(StoreId :: atom(), StreamId :: binary()) ->
    {ok, map()} | {error, not_found | term()}.
