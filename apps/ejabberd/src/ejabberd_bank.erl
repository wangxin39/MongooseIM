%%%===================================================================
%%% @copyright (C) 2013, Erlang Solutions Ltd.
%%% @doc Module for executing SQL queries on bank
%%% @end
%%%===================================================================

-module(ejabberd_bank).
-include("ejabberd.hrl").

%% Queries
-export([%% auth
         add_user/3,
         del_user/2,
         del_user_return_password/3,
         list_users/1,
         list_users/2,
         users_number/1,
         users_number/2,
         get_password/2,
         set_password/3,
         %% mod_last
         get_last/2,
         set_last/4,
         del_last/2,
         %% mod_privacy
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
         %% mod_private
         set_private_data/3,
         get_private_data/3,
         del_private_data/2,
         %% ejabberd_snmp_backend
         count_privacy_lists/1,
         average_roster_size/1,
         average_rostergroup_size/1,
         %% mod_roster
         get_roster_version/2,
         set_roster_version_fun/2,
         set_roster_version/3,
         del_roster_fun/2,
         del_roster_fun/1,
         update_roster_fun/4,
         roster_subscribe_fun/3,
         get_rostergroups/2,
         get_roster/2,
         get_subscription/3,
         get_rostergroup/3,
         %% helpers
         to_bool/1,
         transaction/1,
         transaction/2,
         update_fun/6]).

%% Bank pool starter
-export([start/0]).

%%%===================================================================
%%% Queries
%%%===================================================================

%%%===================================================================
%%% auth
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

%%%===================================================================
%%% mod_last
%%%===================================================================
get_last(Server, Username) ->
    bank:execute(Server, get_last, [Username]).

set_last(Server, Username, Seconds, State) ->
    T = update_fun(get_last, [Username],
                   update_last, [Seconds, State, Username],
                   add_last, [Username, Seconds, State]),
    bank:batch(Server, transaction(T)).

del_last(Server, Username) ->
    bank:execute(Server, del_last, [Username]).

%%%===================================================================
%%% mod_privacy
%%%===================================================================
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

%%%===================================================================
%%% mod_private
%%%===================================================================
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
%%% ejabberd_snmp_backend
%%%===================================================================
count_privacy_lists(Server) ->
    bank:sql_query(Server, <<"select count(*) as lists from privacy_list">>).

average_roster_size(Server) ->
    bank:sql_query(Server, <<"select avg(items) as avg from (select count(*) as "
                             "items from rosterusers group by username) as items">>).

average_rostergroup_size(Server) ->
    bank:sql_query(Server, <<"select avg(roster) as avg from (select count(*) as "
                             "roster from rostergroups group by username) as roster">>).

%%%===================================================================
%%% mod_roster
%%%===================================================================
get_roster_version(Server, Username) ->
    bank:execute(Server, get_roster_version, [Server, Username]).

set_roster_version_fun(Username, Version) ->
    update_fun(get_roster_version,
               [Username],
               update_roster_version,
               [Version, Username],
               add_roster_version,
               [Username, Version]).

set_roster_version(Server, Username, Version) ->
    T = set_roster_version_fun(Username, Version),
    bank:batch(Server, transaction(T)).

del_roster_fun(Username, SJID) ->
    fun(Module, State) ->
            {ok, _, _, State2} = Module:execute(del_roster, [Username, SJID], State),
            {ok, _, _, State3} = Module:execute(del_rostergroup, [Username, SJID], State2),
            {ok, ok, State3}
    end.

del_roster_fun(Username) ->
    fun(Module, State) ->
            {ok, _, _, State2} = Module:execute(del_roster_username, [Username], State),
            {ok, _, _, State3} = Module:execute(del_rostergroup_username, [Username], State2),
            {ok, ok, State3}
    end.


update_roster_fun(Username, SJID, [Username, SJID | ItemRest] = ItemVals, ItemGroups) ->
    fun(Module, State) ->
            Update = update_fun(get_roster_by_jid,
                                [Username, SJID],
                                update_roster,
                                ItemRest ++ [Username, SJID],
                                add_roster,
                                ItemVals),
            {ok, ok, State2} = Update(Module, State),
            {ok, _, _, State3} = Module:execute(del_rostergroup, [Username, SJID], State2),
            NewState = lists:foldl(fun(ItemGroup, AccState) ->
                            {ok, _, _, CurrState} = Module:execute(add_rostergroup, ItemGroup, AccState),
                            CurrState
                    end, State3, ItemGroups),
            {ok, ok, NewState}
    end.

roster_subscribe_fun(Username, SJID, [Username, SJID | ItemRest] = ItemVals) ->
    update_fun(get_roster_by_jid,
               [Username, SJID],
               update_roster,
               ItemRest ++ [Username, SJID],
               add_roster,
               ItemVals).

get_rostergroups(Server, Username) ->
    bank:execute(Server, get_rostergroups, [Username]).

get_roster(Server, Username) ->
    bank:execute(Server, get_roster, [Username]).

get_subscription(Server, Username, SJID) ->
    bank:execute(Server, get_subscription, [Username, SJID]).

get_rostergroup(Server, Username, SJID) ->
    bank:execute(Server, get_rostergroup, [Username, SJID]).

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
      <<"delete from private_storage where username = ?">>},
     {get_roster_version,
      <<"select version from roster_version where username = ?">>},
     {update_roster_version,
      <<"update roster_version set version = ? where username = ?">>},
     {add_roster_version,
      <<"insert into roster_version(username, version) values (?, ?)">>},
     {get_roster_by_jid,
      <<"select username, jid, nick, subscription, ask, askmessage, server, subscribe, "
        "type from rosterusers where username = ? and jid = ?">>},
     {get_roster,
      <<"select username, jid, nick, subscription, ask, askmessage, server, subscribe, "
        "type from rosterusers where username = ?">>},
     {update_roster,
      <<"update rosterusers set nick = ?, subscription = ?, "
        "ask = ?, askmessage = ?, server = ?, subscribe = ?, type = ? "
        "where username = ? and jid = ?">>},
     {add_roster,
      <<"insert into rosterusers(username, jid, nick, subscription, ask, "
        "askmessage, server, subscribe, type) values (?, ?, ?, ?, ?, ?, ?, ?, ?)">>},
     {del_roster,
      <<"delete from rosterusers where username = ? and jid = ?">>},
     {del_rostergroup,
      <<"delete from rostergroups where username = ? and jid = ?">>},
     {del_roster_username,
      <<"delete from rosterusers where username = ?">>},
     {del_rostergroup_username,
      <<"delete from rostergroups where username = ?">>},
     {add_rostergroup,
      <<"insert into rostergroups(username, jid, grp) values (?, ?, ?)">>},
     {get_rostergroups,
      <<"select jid, grp from rostergroups where username = ?">>},
     {get_subscription,
      <<"select subscription from rosterusers where username = ? and jid = ? ">>},
     {get_rostergroup,
      <<"select grp from rostergroups where username = ? and jid = ?">>}].

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

transaction(Server, Fun) ->
    case bank:batch(Server, transaction(Fun)) of
        {ok, Result} ->
            Result;
        _ ->
            aborted
    end.

to_bool(1) -> true;
to_bool(0) -> false.
