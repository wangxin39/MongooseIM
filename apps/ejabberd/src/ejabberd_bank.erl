%%%===================================================================
%%% @copyright (C) 2013, Erlang Solutions Ltd.
%%% @doc Module for executing SQL queries on bank
%%% @end
%%%===================================================================

-module(ejabberd_bank).
-include("ejabberd.hrl").

%% Queries
-export([%% helpers
         to_bool/1,
         transaction/1,
         transaction/2,
         update_fun/6]).

%% Bank pool starter
-export([start/0]).

%% Bank module behaviour
-export([behaviour_info/1]).

%%%===================================================================
%%% Behaviour
%%%===================================================================
behaviour_info(callbacks) ->
    [{prepared_statements, 0}];
behaviour_info(_) ->
    undefined.

%%%===================================================================
%%% Bank pool starter
%%%===================================================================
start() ->
    lists:foreach(
        fun(Host) ->
                case needs_bank(Host) of
                    true ->
                        ok = init(Host);
                    _ ->
                        ok
                end
        end, ?MYHOSTS).

needs_bank(Host) ->
    LHost = jlib:nameprep(Host),
    case ejabberd_config:get_local_option({bank_server, LHost}) of
        undefined -> false;
        _ -> true
    end.

init(Host) ->
    {Driver, Args, WorkerCount} = ejabberd_config:get_local_option({bank_server, Host}),
    InitFun = fun() ->
            {ok, State} = apply(Driver, connect, Args),
            {ok, 0, 0, State2} = Driver:sql_query(<<"set names 'utf8'">>, State),
            {ok, 0, 0, State3} = Driver:sql_query(<<"set session query_cache_type=1">>, State2),
            AuthStatements = case ejabberd_config:get_local_option({auth_method, Host}) of
               bank ->
                    ejabberd_auth_bank:prepared_statements();
               _ ->
                    []
            end, 
            PreparedStatements = lists:foldl(
                    fun(Module, Acc) ->
                            case lists:reverse(atom_to_list(Module)) of
                                "knab_" ++ _Rest ->
                                    Module:prepared_statements() ++ Acc;
                                _ ->
                                    Acc
                            end
                    end, AuthStatements, gen_mod:loaded_modules(Host)),
            NewState = lists:foldl(
                    fun({Name, Query}, AccState) ->
                            {ok, CurrState} = Driver:prepare(Name, Query, AccState),
                            CurrState
                    end, State3, PreparedStatements),
            {ok, Driver, NewState}
    end,
    bank:start_pool(Host, WorkerCount, [{init_fun, InitFun}]).

%%%===================================================================
%%% Helpers
%%%===================================================================

transaction(Server, Fun) ->
    case bank:batch(Server, transaction(Fun)) of
        {ok, Result} ->
            Result;
        _ ->
            aborted
    end.

to_bool(1) -> true;
to_bool(0) -> false.

update_fun(SelectStmt, SelectArgs, UpdateStmt, UpdateArgs, InsertStmt, InsertArgs) ->
    fun(Module, State) ->
            {result_set, _, State2} = Module:execute(SelectStmt, SelectArgs, State),
            NewState = case Module:fetch_all(State2) of
                {rows, [], State3} ->
                    {ok, 1, 0, State4} = Module:execute(InsertStmt, InsertArgs, State3),
                    State4;
                {rows, _, State3} ->
                    {ok, _, 0, State4} = Module:execute(UpdateStmt, UpdateArgs, State3),
                    State4
            end,
            {ok, ok, NewState}
    end.

transaction(TransactionFun) ->
    fun(Module, State) ->
            {ok, 0, 0, State2} = Module:sql_query(<<"begin">>, State),
            try
                {ok, Result, State3} = TransactionFun(Module, State2),
                {ok, 0, 0, State4} = Module:sql_query(<<"commit">>, State3),
                {ok, {ok, Result}, State4}
            catch
                _:_Error ->
                    {ok, 0, 0, State5} = Module:sql_query(<<"rollback">>, State2),
                    {ok, aborted, State5}
            end
    end.
