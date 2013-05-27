%%%----------------------------------------------------------------------
%%% File    : ejabberd_auth_wordpress.erl
%%% Author  : Piotr Nosek <piotr.nosek@erlang-solutions.com>
%%% Purpose : Authentification via Wordpress
%%% Created : 19 Mar 2013 by Piotr Nosek <piotr.nosek@erlang-solutions.com>
%%%----------------------------------------------------------------------

-module(ejabberd_auth_wordpress).
-author('alexey@process-one.net').

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

-export([wp_check_password/3]).

-include("ejabberd.hrl").

%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------
start(_Host) ->
    ok.

plain_password_required() ->
    true.

check_password(User, Server, Password) ->
    case jlib:nodeprep(User) of
        error ->
            false;
        LUser ->
            LServer = jlib:nameprep(Server),
            PasswordSplit = binary:split(Password, <<"|">>, [global]),
            wp_check_password(LUser, LServer, PasswordSplit)
    end.

check_password(_User, _Server, _Password, _Digest, _DigestGen) ->
   false.
    
set_password(_User, _Server, _Password) ->
    {error, not_allowed}.

try_register(_User, _Server, _Password) ->
    {error, not_allowed}.

dirty_get_registered_users() ->
    [].

get_vh_registered_users(_Server) ->
    [].

get_vh_registered_users(_Server, _Opts) ->
    [].

get_vh_registered_users_number(_Server) ->
    0.

get_vh_registered_users_number(_Server, _Opts) ->
    0.

get_password(_User, _Server) ->
    false.

get_password_s(_User, _Server) ->
    {error, not_allowed}.

is_user_exists(_User, _Server) ->
    false.

remove_user(_User, _Server) ->
    {error, not_allowed}.

remove_user(_User, _Server, _Password) ->
    {error, not_allowed}.

wp_check_password(User, Server, [Password]) ->
    StoredHash = wp_get_password(User, Server),

    phpass:check_password(Password, StoredHash, phpass:hash_init(8, true, none));
wp_check_password(User, Server, [User, Expiration, HashFromCookie | Additional]) -> % it's a cookie!
    PassSkip = case Additional of
        [<<"global">>] -> 4;
        _ -> 8
    end,
    <<_:PassSkip/binary, PassFrag:4/binary, _/binary>> = wp_get_password(User, Server),

    ExpirationInt = list_to_integer(binary_to_list(Expiration)),
    {Mega, Secs, _} = erlang:now(),
    UnixTime = Mega*1000000+Secs,

    if
        UnixTime < ExpirationInt ->
            Key = wp_hash(<<User/binary, PassFrag/binary, $|, Expiration/binary>>, Server, logged_in),
            ProperHash = hmac(md5, Key, <<User/binary, $|, Expiration/binary>>),

            Format = lists:flatten(["~2.16.0b" || _N <- lists:seq(1, byte_size(ProperHash))]),

            HashFromCookie =:= list_to_binary(io_lib:format(Format, binary_to_list(ProperHash)));
        true ->
            false
    end;
wp_check_password(_,_,_) ->
    false.

wp_get_password(LUser, LServer) ->
    Username = ejabberd_odbc:escape(LUser),
    case catch odbc_wp_password(LServer, Username) of
        {selected, ["user_pass"], [{Password}]} ->
            Password;
        _ ->
            false
    end.

odbc_wp_password(LServer, Username) ->
    ejabberd_odbc:sql_query(LServer,
        ["select user_pass from wp_users where user_login='", Username, "';"]).

wp_hash(Data, LServer, Scheme) ->
    Hmac = hmac(md5, wp_salt(LServer, Scheme), Data),
    Format = lists:flatten(["~2.16.0b" || _N <- lists:seq(1, byte_size(Hmac))]),
    list_to_binary(io_lib:format(Format, binary_to_list(Hmac))).


wp_salt(LServer, logged_in) ->
    Key = ejabberd_config:get_local_option({wp_logged_in_key, LServer}),
    Salt = ejabberd_config:get_local_option({wp_logged_in_salt, LServer}),
    <<Key/binary, Salt/binary>>.

hmac(md5, Key, Data) ->
    Hmac0 = crypto:hmac_init(md5, Key),
    Hmac1 = crypto:hmac_update(Hmac0, Data),
    crypto:hmac_final(Hmac1).
