%%%===================================================================
%%% @copyright (C) 2013, Erlang Solutions Ltd.
%%% @doc Implementation of ejabberd_auth interface using bank
%%% @end
%%%===================================================================

-module(ejabberd_auth_bank).

%% External exports
-export([start/1,
	 set_password/3,
	 check_password/3,
	 check_password/5,
	 try_register/3,
	 dirty_get_registered_users/0,
	 get_vh_registered_users/1,
	 get_vh_registered_users/2,
	 get_vh_registered_users_number/1,
	 get_vh_registered_users_number/2,
	 get_password/2,
	 get_password_s/2,
	 is_user_exists/2,
	 remove_user/2,
	 remove_user/3,
	 plain_password_required/0
	]).

-behaviour(ejabberd_bank).
-export([prepared_statements/0]).

-include("ejabberd.hrl").

%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------
start(_Host) ->
    ok.

plain_password_required() ->
    false.

%% @spec (User, Server, Password) -> true | false | {error, Error}
check_password(User, Server, Password) ->
    case jlib:nodeprep(User) of
	error ->
	    false;
	LUser ->
            LServer = jlib:nameprep(Server),
            case get_password_q(LServer, LUser) of
                {rows, [[{<<"password">>, Password}]]} ->
                    Password =/= <<"">>;
                _ ->
                    false
            end
    end.

%% @spec (User, Server, Password, Digest, DigestGen) -> true | false | {error, Error}
check_password(User, Server, Password, Digest, DigestGen) ->
    case jlib:nodeprep(User) of
	error ->
	    false;
	LUser ->
	    LServer = jlib:nameprep(Server),
            case get_password_q(LServer, LUser) of
                {rows, [[{<<"password">>, Passwd}]]} ->
                    ((Digest =/= <<"">>) and (Digest =:= DigestGen(Passwd))) or
                    ((Password =/= <<"">>) and (Passwd =:= Password));
                _ -> false
            end
    end.

%% @spec (User::string(), Server::string(), Password::string()) ->
%%       ok | {error, invalid_jid}
set_password(User, Server, Password) ->
    case jlib:nodeprep(User) of
	error ->
	    {error, invalid_jid};
	_LUser ->
	    LServer = jlib:nameprep(Server),
            case set_password_q(LServer, User, Password) of
                {ok, {ok, _Res}} -> ok;
                {ok, Other} -> {error, Other}
	    end
    end.


%% @spec (User, Server, Password) -> {atomic, ok} | {atomic, exists} | {error, invalid_jid}
try_register(User, Server, Password) ->
    case jlib:nodeprep(User) of
	error ->
	    {error, invalid_jid};
	LUser ->
	    LServer = jlib:nameprep(Server),
            case add_user_q(LServer, LUser, Password) of
                {ok, 1, 0} -> {atomic, ok};
                _ -> {atomic, exists}
            end
    end.

dirty_get_registered_users() ->
    Servers = ejabberd_config:get_vh_by_auth_method(bank),
    lists:flatmap(
      fun(Server) ->
	      get_vh_registered_users(Server)
      end, Servers).

get_vh_registered_users(Server) ->
    LServer = jlib:nameprep(Server),
    case list_users_q(LServer) of
        {rows, Result} ->
            [{Username, LServer} || [{<<"username">>, Username}] <- Result];
        _ ->
            []
    end.

get_vh_registered_users(Server, Opts) ->
    LServer = jlib:nameprep(Server),
    case list_users_q(LServer, Opts) of
        {rows, Result} ->
            [{Username, LServer} || [{<<"username">>, Username}] <- Result];
        _ ->
            []
    end.

get_vh_registered_users_number(Server) ->
    LServer = jlib:nameprep(Server),
    case users_number_q(LServer) of
        {rows, [[{<<"users_number">>, Number}]]} ->
            Number;
	_ ->
	    0
    end. 

get_vh_registered_users_number(Server, Opts) ->
    LServer = jlib:nameprep(Server),
    case users_number_q(LServer, Opts) of
        {rows, [[{<<"users_number">>, Number}]]} ->
            Number;
	_ ->
	    0
    end.

get_password(User, Server) ->
    case jlib:nodeprep(User) of
	error ->
	    false;
	_LUser ->
	    LServer = jlib:nameprep(Server),
            case get_password_q(LServer, User) of
                {rows, [[{<<"password">>, Password}]]} ->
                    Password;
                _ ->
                    false
            end
    end.

get_password_s(User, Server) ->
    case jlib:nodeprep(User) of
	error ->
            <<"">>;
	_LUser ->
	    LServer = jlib:nameprep(Server),
            case get_password_q(LServer, User) of
                {rows, [[{<<"password">>, Password}]]} ->
                    Password;
                _ ->
                    <<"">>
            end
    end.

%% @spec (User, Server) -> true | false | {error, Error}
is_user_exists(User, Server) ->
    case jlib:nodeprep(User) of
	error ->
	    false;
	_LUser ->
	    LServer = jlib:nameprep(Server),
            case get_password_q(LServer, User) of
                {rows, [[{<<"password">>, _Password}]]} ->
                    true;
                {rows, []} ->
                    false
            end
    end.

%% @spec (User, Server) -> ok | error
%% @doc Remove user.
%% Note: it may return ok even if there was some problem removing the user.
remove_user(User, Server) ->
    case jlib:nodeprep(User) of
	error ->
	    error;
	_LUser ->
	    LServer = jlib:nameprep(Server),
            {ok, _, _} = del_user_q(LServer, User),
            ok
    end.

%% @spec (User, Server, Password) -> ok | error | not_exists | not_allowed
%% @doc Remove user if the provided password is correct.
remove_user(User, Server, Password) ->
    case jlib:nodeprep(User) of
	error ->
	    error;
	_LUser ->
	    LServer = jlib:nameprep(Server),
            case del_user_return_password_q(LServer, User, Password) of
                {ok, {ok, [[Password]]}} ->
                    ok;
                {ok, {ok, []}} ->
                    not_exists;
                _ ->
                    not_allowed
            end
    end.

%%%===================================================================
%%% Queries
%%%===================================================================
add_user_q(Server, Username, Password) ->
    bank:execute(Server, add_user, [Username, Password]).

del_user_q(Server, Username) ->
    bank:execute(Server, del_user, [Username]).

del_user_return_password_q(Server, Username, Password) ->
    T = fun(Module, State) ->
            {result_set, _, State2} = Module:execute(get_password, [Username], State),
            {rows, Result, State3} = Module:fetch_all(State2),
            {ok, _, _, State4} = Module:execute(del_user_password, [Username, Password], State3),
            {ok, Result, State4}
    end,
    bank:batch(Server, ejabberd_bank:transaction(T)).

list_users_q(Server) ->
    bank:execute(Server, list_users, []).

list_users_q(LServer, [{from, Start}, {to, End}]) ->
    list_users_q(LServer, [{limit, End-Start+1}, {offset, Start-1}]);
list_users_q(LServer, [{prefix, Prefix}, {from, Start}, {to, End}]) ->
    list_users_q(LServer, [{prefix, Prefix}, {limit, End-Start+1}, {offset, Start-1}]);
list_users_q(LServer, [{limit, Limit}, {offset, Offset}]) ->
    bank:execute(LServer, list_users_limit, [Limit, Offset]);
list_users_q(LServer, [{prefix, Prefix},
                     {limit, Limit},
                     {offset, Offset}]) ->
    bank:execute(LServer, list_users_prefix, [<<Prefix/binary, "%">>, Limit, Offset]).

users_number_q(LServer) ->
    bank:execute(LServer, users_number, []).

users_number_q(LServer, [{prefix, Prefix}]) ->
    bank:execute(LServer, users_number_prefix, [<<Prefix/binary, "%">>]);
users_number_q(LServer, []) ->
    users_number_q(LServer).

get_password_q(Server, Username) ->
    bank:execute(Server, get_password, [Username]).

set_password_q(Server, Username, Password) ->
    T = ejabberd_bank:update_fun(get_password, [Username],
                   update_password, [Password, Username],
                   add_user, [Username, Password]),
    bank:batch(Server, ejabberd_bank:transaction(T)).


%%%===================================================================
%%% Behaviour
%%%===================================================================

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
      <<"update users set password = ? where username = ?">>}].
