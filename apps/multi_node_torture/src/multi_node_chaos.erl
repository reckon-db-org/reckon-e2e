%% @doc Chaos primitives for the multi_node_torture scenarios.
%%
%% Drives chaos via plain SSH + docker against the running
%% reckon-gateway cluster. Every primitive shells out — keeps the
%% test runner decoupled from the cluster's BEAM dist, which is
%% essential when you're about to KILL nodes in that dist.
%%
%% Cluster topology is hardcoded for now (matches
%% reckon-cluster-compose/env/<host>.env). When/if the dev box
%% joins, add `host00' here.
-module(multi_node_chaos).

-export([cluster_hosts/0,
         find_leader/1,
         kill_leader/1,
         restart_node/1,
         wait_for_leader_change/3]).

-define(SSH_OPTS, "-o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=5").

%%====================================================================
%% Topology
%%====================================================================

%% Hostname -> node name (the BEAM long-name set in env/<host>.env).
-spec cluster_hosts() -> [{Host :: string(), Node :: atom()}].
cluster_hosts() ->
    [{"beam00.lab", 'reckon_gateway@192.168.1.10'},
     {"beam01.lab", 'reckon_gateway@192.168.1.11'},
     {"beam02.lab", 'reckon_gateway@192.168.1.12'},
     {"beam03.lab", 'reckon_gateway@192.168.1.13'}].

%%====================================================================
%% Leader discovery
%%====================================================================

%% @doc Find the current Raft leader for StoreId by asking each
%% node for `:ra_leaderboard.lookup_leader(StoreId)' via
%% `docker exec ... reckon_gateway eval'. The leader is whichever
%% node reports the leader_node matching its own node().
%%
%% Returns {ok, Host, Node} or {error, no_leader_found}.
-spec find_leader(atom()) -> {ok, string(), atom()} | {error, no_leader_found}.
find_leader(StoreId) ->
    find_leader_among(cluster_hosts(), StoreId).

find_leader_among([], _StoreId) ->
    {error, no_leader_found};
find_leader_among([{Host, _Node} | Rest], StoreId) ->
    case query_leader(Host, StoreId) of
        {ok, LeaderNode} ->
            case node_to_host(LeaderNode) of
                {ok, LeaderHost} -> {ok, LeaderHost, LeaderNode};
                error           -> find_leader_among(Rest, StoreId)
            end;
        error ->
            find_leader_among(Rest, StoreId)
    end.

query_leader(Host, StoreId) ->
    %% Eval `ra_leaderboard:lookup_leader(StoreId)' inside the
    %% running release. The release's `eval' subcommand wraps the
    %% expression in a remote call.
    Cmd = lists:flatten(io_lib:format(
        "ssh ~s rl@~s 'docker exec reckon-gateway /app/bin/reckon_gateway "
        "eval \"ra_leaderboard:lookup_leader(~p).\"' 2>/dev/null",
        [?SSH_OPTS, Host, StoreId])),
    Output = os:cmd(Cmd),
    case parse_leader_output(Output) of
        {ok, _Node} = Ok -> Ok;
        error            -> error
    end.

%% Eval output looks like `{default_store, 'reckon_gateway@192.168.1.12'}'.
%% The atom is single-quoted because the @ + dots in the IP make it
%% not a bare-atom literal.
parse_leader_output(Output) ->
    case re:run(Output,
                "{[^,]+,\\s*'?(reckon_gateway@[0-9.]+)'?\\s*}",
                [{capture, [1], list}]) of
        {match, [NodeStr]} -> {ok, list_to_atom(NodeStr)};
        _                  -> error
    end.

node_to_host(Node) ->
    case lists:keyfind(Node, 2, cluster_hosts()) of
        {Host, Node} -> {ok, Host};
        false        -> error
    end.

%%====================================================================
%% Destructive primitives
%%====================================================================

%% @doc Kill the current leader for StoreId by sending SIGKILL to
%% its reckon-gateway container.
-spec kill_leader(atom()) -> {ok, string(), atom()} | {error, term()}.
kill_leader(StoreId) ->
    case find_leader(StoreId) of
        {ok, Host, Node} ->
            Cmd = lists:flatten(io_lib:format(
                "ssh ~s rl@~s 'docker kill reckon-gateway' 2>&1",
                [?SSH_OPTS, Host])),
            _ = os:cmd(Cmd),
            {ok, Host, Node};
        {error, _} = E ->
            E
    end.

%% @doc Restart the reckon-gateway container on Host.
-spec restart_node(string()) -> ok.
restart_node(Host) ->
    Cmd = lists:flatten(io_lib:format(
        "ssh ~s rl@~s 'cd /home/rl/reckon-cluster-compose && "
        "docker compose --env-file=.env --env-file=env/~s.env up -d' 2>&1",
        [?SSH_OPTS, Host, host_short(Host)])),
    _ = os:cmd(Cmd),
    ok.

host_short(Host) ->
    hd(string:split(Host, ".")).

%%====================================================================
%% Synchronization helpers
%%====================================================================

%% @doc Block until the leader for StoreId is NOT OldNode anymore
%% (election completed), or DeadlineMs elapses.
-spec wait_for_leader_change(atom(), atom(), non_neg_integer()) ->
    {ok, atom()} | timeout.
wait_for_leader_change(StoreId, OldNode, DeadlineMs) ->
    Deadline = erlang:monotonic_time(millisecond) + DeadlineMs,
    wait_loop(StoreId, OldNode, Deadline).

wait_loop(StoreId, OldNode, Deadline) ->
    case find_leader(StoreId) of
        {ok, _Host, NewNode} when NewNode =/= OldNode ->
            {ok, NewNode};
        _ ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true  -> timeout;
                false ->
                    timer:sleep(500),
                    wait_loop(StoreId, OldNode, Deadline)
            end
    end.
