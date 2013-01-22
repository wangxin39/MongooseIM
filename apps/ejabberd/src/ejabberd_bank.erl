%%%===================================================================
%%% @copyright (C) 2013, Erlang Solutions Ltd.
%%% @doc Module for executing SQL queries on bank
%%% @end
%%%===================================================================

-module(ejabberd_bank).
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
         get_last/2,
         set_last/4,
         del_last/2,
         get_default_privacy_list/2,
         set_default_privacy_list/3,
         del_default_privacy_list/2,
         get_privacy_list_data/3,
         get_privacy_list_data_by_id/2,
         get_privacy_list_names/2,
         get_privacy_list_id/3,
         del_privacy_list/3,
         del_privacy_lists/2,
         set_privacy_list/4,
         set_private_data/3,
         get_private_data/3,
         del_private_data/2,
         to_bool/1,
         transaction/1,
         update_fun/6]).

%% Bank pool starter
-export([start/0]).

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
    T = update_fun(get_password, [Username],
                   update_password, [Password, Username],
                   add_user, [Username, Password]),
    bank:batch(Server, transaction(T)).

get_last(Server, Username) ->
    bank:execute(Server, get_last, [Username]).

set_last(Server, Username, Seconds, State) ->
    T = update_fun(get_last, [Username],
                   update_last, [Seconds, State, Username],
                   add_last, [Username, Seconds, State]),
    bank:batch(Server, transaction(T)).

del_last(Server, Username) ->
    bank:execute(Server, del_last, [Username]).

get_default_privacy_list(Server, Username) ->
    bank:execute(Server, get_default_privacy_list, [Username]).

set_default_privacy_list(Server, Username, Name) ->
    T = fun(Module, State) ->
            {result_set, _, State2} = Module:execute(get_privacy_list_by_name,
                                                     [Username, Name], State),
            case Module:fetch_all(State2) of
                {rows, [], State3} ->
                    {ok, {error, not_found}, State3};
                {rows, _, State3} ->
                    F = update_fun(get_default_privacy_list, [Username],
                                   update_default_privacy_list, [Name, Username],
                                   add_default_privacy_list, [Username, Name]),
                    F(Module, State3)
            end
    end,
    bank:batch(Server, transaction(T)).

del_default_privacy_list(Server, Username) ->
    bank:execute(Server, del_default_privacy_list, [Username]).

get_privacy_list_id(Server, Username, SName) ->
    bank:execute(Server, get_privacy_list_id, [Username, SName]).

get_privacy_list_data(Server, Username, SName) ->
    bank:execute(Server, get_privacy_list_data, [Username, SName]).

get_privacy_list_data_by_id(Server, ID) ->
    bank:execute(Server, get_privacy_list_data_by_id, [ID]).

get_privacy_list_names(Server, Username) ->
    bank:execute(Server, get_privacy_list_names, [Username]).

del_privacy_list(Server, Username, Name) ->
    T = fun(Module, State) ->
            {result_set, _, State2} = Module:execute(get_default_privacy_list, [Username], State),
            case Module:fetch_all(State2) of
                {rows, [], State3} ->
                    {ok, _, _, State4} = Module:execute(del_privacy_list, [Username, Name], State3),
                    {ok, {result, []}, State4};
                {rows, [[{<<"name">>, Default}]], State3} ->
                    case Name == Default of
                        true ->
                            {error, conflict};
                        _ ->
                            {ok, _, _, State4} = Module:execute(del_privacy_list, [Username, Name], State3),
                            {ok, {result, []}, State4}
                    end
            end
    end,
    bank:batch(Server, transaction(T)).

del_privacy_lists(Server, Username) ->
    Value = <<Username/binary, "@", Server/binary>>,
    T = fun(Module, State) ->
            {ok, _, _, State2} = Module:execute(del_privacy_lists, [Username], State),
            {ok, _, _, State3} = Module:execute(del_privacy_lists_data, [Value], State2),
            {ok, _, _, State4} = Module:execute(del_default_privacy_list, [Username], State3),
           {ok, ok, State4}
    end,
    bank:batch(Server, transaction(T)). 

set_privacy_list(Server, Username, Name, Items) ->
    T = fun(Module, State) ->
            {result_set, _, State2} = Module:execute(get_privacy_list_id, [Username, Name], State),
            {ID, NewState} = case Module:fetch_all(State2) of
                {rows, [], State3} ->
                    {ok, _, _, State4} = Module:execute(add_privacy_list, [Username, Name], State3),
                    {result_set, _, State5} = Module:execute(get_privacy_list_id, [Username, Name], State4),
                    {rows, [[I]], State6} = Module:fetch_all(State5),
                    {I, State6};
                {rows, [[I]], State3} ->
                    {I, State3}
            end,
            {ok, _, _, NewState1} = Module:execute(del_privacy_list_data, [ID], NewState),
            NewState2 = lists:foldl(fun(Item, AccState) ->
                        {ok, _, _, CurrState} = Module:execute(add_privacy_list_data, [ID | Item], AccState),
                        CurrState
                    end, NewState1, Items),
            {ok, {result, []}, NewState2}
    end,
    bank:batch(Server, transaction(T)).

set_private_data(Server, Username, Elements) ->
    T = fun(Module, State) ->
            NewState = lists:foldl(fun(Element, AccState) ->
                            {xmlelement, _, Attrs, _} = Element,
                            XMLNS = xml:get_attr_s(<<"xmlns">>, Attrs),
                            SElement = xml:element_to_binary(Element),
                            Update = update_fun(get_private_data,
                                                [Username, XMLNS],
                                                update_private_data,
                                                [SElement, Username, XMLNS],
                                                add_private_data,
                                                [Username, XMLNS, SElement]),
                            {ok, ok, CurrState} = Update(Module, AccState),
                            CurrState
                    end, State, Elements),
           {ok, ok, NewState}
    end,
    bank:batch(Server, transaction(T)). 

get_private_data(Server, Username, XMLNS) ->
    bank:execute(Server, get_private_data, [Username, XMLNS]).

del_private_data(Server, Username) ->
    bank:execute(Server, del_private_data, [Username]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

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

prepared_statements() ->
    [{add_user,
      <<"insert into users(username, password) values (?, ?)">>},
     {del_user,
      <<"delete from users where username = ?">>},
     {del_user_password,
      <<"delete from users where username = ? and password = ?">>},
     {list_users,
      <<"select username from users">>},
     {list_users_limit,
      <<"select username from users order by username limit ? offset ?">>},
     {list_users_prefix,
      <<"select username from users where username like ? order by username limit ? offset ?">>},
     {users_number,
      <<"select count(*) as users_number from users">>},
     {users_number_prefix,
      <<"select count(*) as users_number from users where username like ?">>},
     {get_password,
      <<"select password from users where username = ?">>},
     {update_password,
      <<"update users set password = ? where username = ?">>},
     {get_last,
      <<"select seconds, state from last where username = ?">>},
     {update_last,
      <<"update last set seconds = ?, state = ? where username = ?">>},
     {add_last,
      <<"insert into last(username, seconds, state) values (?, ?, ?)">>},
     {del_last,
      <<"delete from last where username = ?">>},
     {get_default_privacy_list,
      <<"select name from privacy_default_list where username = ?">>},
     {update_default_privacy_list,
      <<"update privacy_default_list set name = ? where username = ?">>},
     {add_default_privacy_list,
      <<"insert into privacy_default_list(username, name) values (?, ?)">>},
     {del_default_privacy_list,
      <<"delete from privacy_default_list where username = ?">>},
     {get_privacy_list_by_name,
      <<"select name from privacy_list where username = ? and name = ?">>},
     {get_privacy_list_id,
      <<"select id from privacy_list where username = ? and name = ?">>},
     {get_privacy_list_data,
      <<"select * from privacy_list_data where "
        "id = (select id from privacy_list where username = ? and name = ?) "
        "order by ord">>},
     {get_privacy_list_data_by_id,
      <<"select * from privacy_list_data where id = ? order by ord">>},
     {get_privacy_list_names,
      <<"select name from privacy_list where username = ?">>},
     {del_privacy_list,
      <<"delete from privacy_list where username = ? and name = ?">>},
     {del_privacy_lists,
      <<"delete from privacy_list where username = ?">>},
     {del_privacy_list_data,
      <<"delete from privacy_list_data where id = ?">>},
     {del_privacy_lists_data,
      <<"delete from privacy_list_data where value = ?">>},
     {add_privacy_list,
      <<"insert into privacy_list(username, name) values (?, ?)">>},
     {add_privacy_list_data,
      <<"insert into privacy_list_data(id, t, value, action, ord, match_all, "
        "match_iq, match_message, match_presence_in, match_presence_out) values "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)">>},
     {get_private_data,
      <<"select data from private_storage where "
        "username = ? and namespace = ?">>},
     {update_private_data,
      <<"update private_storage set data = ? where username = ? and namespace = ?">>},
     {add_private_data,
      <<"insert into private_storage(username, namespace, data) values (?, ?, ?)">>},
     {del_private_data,
      <<"delete from private_storage where username = ?">>}].

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
            NewState = lists:foldl(
                    fun({Name, Query}, AccState) ->
                            {ok, CurrState} = Driver:prepare(Name, Query, AccState),
                            CurrState
                    end, State3, prepared_statements()),
            {ok, Driver, NewState}
    end,
    bank:start_pool(Host, WorkerCount, [{init_fun, InitFun}]).

%%%===================================================================
%%% Helpers
%%%===================================================================

to_bool(1) -> true;
to_bool(0) -> false.
