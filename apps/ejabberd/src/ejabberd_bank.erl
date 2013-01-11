%%%===================================================================
%%% @copyright (C) 2013, Erlang Solutions Ltd.
%%% @doc Module for executing SQL queries on bank
%%% @end
%%%===================================================================

-module(ejabberd_bank).
-behaviour(gen_server).
-include("ejabberd.hrl").

%% Queries
-export([add_user/3,
         del_user/2,
         del_user_return_password/3,
         list_users/1,
         list_users/2,
         users_number/1,
         users_number/2,
         get_password/2,
         set_password/3,
         transaction/1]).

%% Worker starter
-export([start/0]).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {host,
                driver}).

%%%===================================================================
%%% Queries
%%%===================================================================

add_user(Server, Username, Password) ->
    bank:execute(Server, add_user, [Username, Password]).

del_user(Server, Username) ->
    bank:execute(Server, del_user, [Username]).

del_user_return_password(Server, Username, Password) ->
    T = fun(Module, State) ->
            {result_set, _, State2} = Module:execute(get_password, [Username], State),
            {rows, Result, State3} = Module:fetch_all(State2),
            {ok, _, _, State4} = Module:execute(del_user_password, [Username, Password], State3),
            {ok, Result, State4}
    end,
    bank:batch(Server, transaction(T)).

list_users(Server) ->
    bank:execute(Server, list_users, []).

list_users(LServer, [{from, Start}, {to, End}]) ->
    list_users(LServer, [{limit, End-Start+1}, {offset, Start-1}]);
list_users(LServer, [{prefix, Prefix}, {from, Start}, {to, End}]) ->
    list_users(LServer, [{prefix, Prefix}, {limit, End-Start+1}, {offset, Start-1}]);
list_users(LServer, [{limit, Limit}, {offset, Offset}]) ->
    bank:execute(LServer, list_users_limit, [Limit, Offset]);
list_users(LServer, [{prefix, Prefix},
                     {limit, Limit},
                     {offset, Offset}]) ->
    bank:execute(LServer, list_users_prefix, [<<Prefix/binary, "%">>, Limit, Offset]).

users_number(LServer) ->
    bank:execute(LServer, users_number, []).

users_number(LServer, [{prefix, Prefix}]) ->
    bank:execute(LServer, users_number_prefix, [<<Prefix/binary, "%">>]);
users_number(LServer, []) ->
    users_number(LServer).

get_password(Server, Username) ->
    bank:execute(Server, get_password, [Username]).

set_password(Server, Username, Password) ->
    T = fun(Module, State) ->
            NewState = case Module:execute(update_password, [Password, Username], State) of
                {ok, 1, 0, State2} ->
                    State2;
                {ok, 0, 0, State2} ->
                    {ok, 1, 0, State3} = Module:execute(add_user,
                                                        [Username, Password], State2),
                    State3
            end,
            {ok, ok, NewState}
    end,
    bank:batch(Server, transaction(T)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Host) -> 
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Host], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Host]) ->
    {Driver, Args, WorkerCount} = ejabberd_config:get_local_option({bank_server, Host}),
    InitFun = fun() ->
            {ok, State} = apply(Driver, connect, Args),
            {ok, 0, 0, State2} = Driver:sql_query(<<"set names 'utf8'">>, State),
            {ok, 0, 0, State3} = Driver:sql_query(<<"set session query_cache_type=1">>, State2),
            {ok, Driver, State3}
    end,
    case bank:start_pool(Host, WorkerCount, [{init_fun, InitFun}]) of
        ok ->
            _ = [bank:prepare(Host, Name, Query) ||
                    {Name, Query} <- prepared_statements()],
            {ok, #state{host = Host,
                driver = Driver}};
        Error ->
            {stop, Error}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info,  State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

transaction(TransactionFun) ->
    fun(Module, State) ->
            {ok, 0, 0, State2} = Module:sql_query(<<"begin">>, State),
            try
                {ok, Result, State3} = TransactionFun(Module, State2),
                {ok, 0, 0, State4} = Module:sql_query(<<"commit">>, State3),
                {ok, {ok, Result}, State4}
            catch
                _:_ ->
                    {ok, 0, 0, State5} = Module:sql_query(<<"rollback">>, State2),
                    {ok, aborted, State5}
            end
    end.

prepared_statements() ->
    [{add_user, <<"insert into users(username, password) values (?, ?)">>},
     {del_user, <<"delete from users where username = ?">>},
     {del_user_password, <<"delete from users where username = ? and password = ?">>},
     {list_users, <<"select username from users">>},
     {list_users_limit,
      <<"select username from users order by username limit ? offset ?">>},
     {list_users_prefix,
      <<"select username from users where username like ? "
        "order by username limit ? offset ?">>},
     {users_number, <<"select count(*) as users_number from users">>},
     {users_number_prefix,
      <<"select count(*) as users_number from users where username like ?">>},
     {get_password, <<"select password from users where username = ?">>},
     {update_password, <<"update users set password = ? where username = ?">>}].

%%%===================================================================
%%% Worker starter
%%%===================================================================

start() ->
    lists:foreach(
        fun(Host) ->
                case needs_bank(Host) of
                    true -> start_bank(Host);
                    _ -> ok
                end
        end, ?MYHOSTS).

start_bank(Host) ->
    WorkerName = gen_mod:get_module_proc(Host, ?MODULE),
    ChildSpec = {WorkerName,
                 {?MODULE, start_link, [Host]},
                 transient,
                 5000,
                 worker,
                 [?MODULE]},
    case supervisor:start_child(ejabberd_sup, ChildSpec) of
        {ok, _PID} -> ok;
        Error ->
            ?ERROR_MSG("Start of supervisor ~p failed:~n~p~nRetrying...~n", [WorkerName, Error]),
            start_bank(Host)
    end.

needs_bank(Host) ->
    LHost = jlib:nameprep(Host),
    case ejabberd_config:get_local_option({bank_server, LHost}) of
        undefined -> false;
        _ -> true
    end.

