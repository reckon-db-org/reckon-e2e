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
         wait_for_leader_change/3,
         partition_minority/1,
         heal_partition/0]).

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

%%====================================================================
%% Network partition (iptables)
%%====================================================================

%% @doc Create a 1-vs-N partition by isolating `MinorityHost' (a
%% string like "beam03.lab") from all other cluster hosts. Drops
%% traffic in BOTH directions so TCP handshakes fail fast rather
%% than waiting for SYN-timeout.
%%
%% All inserted rules carry a comment so `heal_partition/0' can find
%% and remove just our rules, leaving any pre-existing iptables
%% configuration intact.
%%
%% Returns {ok, MinorityHost, [OtherHost]} or {error, _}.
-spec partition_minority(string()) ->
    {ok, string(), [string()]} | {error, term()}.
partition_minority(MinorityHost) ->
    All = cluster_hosts(),
    case lists:keyfind(MinorityHost, 1, All) of
        false -> {error, {unknown_host, MinorityHost}};
        _ ->
            OtherHosts = [H || {H, _} <- All, H =/= MinorityHost],
            MinorityIp = host_ip(MinorityHost),
            OtherIps = [host_ip(H) || H <- OtherHosts],
            %% Drop on minority side: block to/from each OtherIp.
            lists:foreach(
                fun(OtherIp) ->
                    drop_rule(MinorityHost, "OUTPUT", "-d", OtherIp),
                    drop_rule(MinorityHost, "INPUT",  "-s", OtherIp)
                end, OtherIps),
            %% Drop on each majority side: block to/from MinorityIp.
            lists:foreach(
                fun(OtherHost) ->
                    drop_rule(OtherHost, "OUTPUT", "-d", MinorityIp),
                    drop_rule(OtherHost, "INPUT",  "-s", MinorityIp)
                end, OtherHosts),
            {ok, MinorityHost, OtherHosts}
    end.

%% @doc Remove every iptables rule tagged with our comment, on every
%% cluster host. Idempotent — re-running has no effect.
-spec heal_partition() -> ok.
heal_partition() ->
    [remove_torture_rules(Host) || {Host, _} <- cluster_hosts()],
    ok.

%%====================================================================
%% Internal — iptables helpers
%%====================================================================

-define(TORTURE_TAG, "reckon-torture").

drop_rule(Host, Chain, Dir, Ip) ->
    %% -C exists-check first so we don't stack duplicate rules if the
    %% scenario double-partitions.
    Check = lists:flatten(io_lib:format(
        "ssh ~s rl@~s 'sudo -n iptables -C ~s ~s ~s -m comment "
        "--comment ~s -j DROP' 2>/dev/null && echo EXISTS",
        [?SSH_OPTS, Host, Chain, Dir, Ip, ?TORTURE_TAG])),
    case string:trim(os:cmd(Check)) of
        "EXISTS" -> ok;
        _ ->
            Add = lists:flatten(io_lib:format(
                "ssh ~s rl@~s 'sudo -n iptables -I ~s ~s ~s -m comment "
                "--comment ~s -j DROP' 2>&1",
                [?SSH_OPTS, Host, Chain, Dir, Ip, ?TORTURE_TAG])),
            _ = os:cmd(Add),
            ok
    end.

remove_torture_rules(Host) ->
    %% Dump rules in -S format, grep ours, rewrite -A as -D, run
    %% them back. Handles however many we inserted (input + output
    %% across multiple peer IPs).
    Cmd = lists:flatten(io_lib:format(
        "ssh ~s rl@~s 'sudo -n iptables -S | grep -- \"--comment ~s\" "
        "| sed \"s/^-A/-D/\" | while read R; do sudo -n iptables $R; done' "
        "2>&1",
        [?SSH_OPTS, Host, ?TORTURE_TAG])),
    _ = os:cmd(Cmd),
    ok.

host_ip("beam00.lab") -> "192.168.1.10";
host_ip("beam01.lab") -> "192.168.1.11";
host_ip("beam02.lab") -> "192.168.1.12";
host_ip("beam03.lab") -> "192.168.1.13".
