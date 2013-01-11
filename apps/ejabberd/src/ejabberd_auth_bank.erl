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
            case ejabberd_bank:get_password(LServer, LUser) of
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
            case ejabberd_bank:get_password(LServer, LUser) of
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
            case ejabberd_bank:set_password(LServer, User, Password) of
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
            case ejabberd_bank:add_user(LServer, LUser, Password) of
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
    case ejabberd_bank:list_users(LServer) of
        {rows, Result} ->
            [{Username, LServer} || [{<<"username">>, Username}] <- Result];
        _ ->
            []
    end.

get_vh_registered_users(Server, Opts) ->
    LServer = jlib:nameprep(Server),
    case ejabberd_bank:list_users(LServer, Opts) of
        {rows, Result} ->
            [{Username, LServer} || [{<<"username">>, Username}] <- Result];
        _ ->
            []
    end.

get_vh_registered_users_number(Server) ->
    LServer = jlib:nameprep(Server),
    case ejabberd_bank:users_number(LServer) of
        {rows, [[{<<"users_number">>, Number}]]} ->
            Number;
	_ ->
	    0
    end. 

get_vh_registered_users_number(Server, Opts) ->
    LServer = jlib:nameprep(Server),
    case ejabberd_bank:users_number(LServer, Opts) of
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
            case ejabberd_bank:get_password(LServer, User) of
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
            case ejabberd_bank:get_password(LServer, User) of
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
            case ejabberd_bank:get_password(LServer, User) of
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
            {ok, _, _} = ejabberd_bank:del_user(LServer, User),
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
            case ejabberd_bank:del_user_return_password(LServer, User, Password) of
                {ok, {ok, [[Password]]}} ->
                    ok;
                {ok, {ok, []}} ->
                    not_exists;
                _ ->
                    not_allowed
            end
    end.
