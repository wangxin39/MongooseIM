%%%===================================================================
%%% @copyright (C) 2013, Erlang Solutions Ltd.
%%% @doc Module for querying for last activities via bank
%%% @end
%%%===================================================================
-module(mod_last_bank).

-behaviour(ejabberd_bank).
-export([prepared_statements/0]).

-behaviour(gen_mod).
-export([start/2,
         stop/1,
         process_local_iq/3,
         process_sm_iq/3,
         on_presence_update/4,
         store_last_info/4,
         get_last_info/2,
         remove_user/2]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("mod_privacy.hrl").

start(Host, Opts) ->
    IQDisc = gen_mod:get_opt(iqdisc, Opts, one_queue),
    gen_iq_handler:add_iq_handler(ejabberd_local, Host, ?NS_LAST,
                                  ?MODULE, process_local_iq, IQDisc),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, ?NS_LAST,
                                  ?MODULE, process_sm_iq, IQDisc),
    ejabberd_hooks:add(remove_user, Host,
                       ?MODULE, remove_user, 50),
    ejabberd_hooks:add(unset_presence_hook, Host,
                       ?MODULE, on_presence_update, 50).

stop(Host) ->
    ejabberd_hooks:delete(remove_user, Host,
                          ?MODULE, remove_user, 50),
    ejabberd_hooks:delete(unset_presence_hook, Host,
                          ?MODULE, on_presence_update, 50),
    gen_iq_handler:remove_iq_handler(ejabberd_local, Host, ?NS_LAST),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_LAST).

%%%
%%% Uptime of ejabberd node
%%%
process_local_iq(_From, _To, #iq{type = Type, sub_el = SubEl} = IQ) ->
    case Type of
        set ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_NOT_ALLOWED]};
        get ->
            Sec = get_node_uptime(),
            IQ#iq{type = result,
                  sub_el =  [{xmlelement, <<"query">>,
                              [{<<"xmlns">>, ?NS_LAST},
                               {<<"seconds">>, list_to_binary(integer_to_list(Sec))}],
                              []}]}
    end.

%% @spec () -> integer()
%% @doc Get the uptime of the ejabberd node, expressed in seconds.
%% When ejabberd is starting, ejabberd_config:start/0 stores the datetime.
get_node_uptime() ->
    case ejabberd_config:get_local_option(node_start) of
        {_, _, _} = StartNow ->
            now_to_seconds(now()) - now_to_seconds(StartNow);
        _undefined ->
            trunc(element(1, erlang:statistics(wall_clock))/1000)
    end.

now_to_seconds({MegaSecs, Secs, _MicroSecs}) ->
    MegaSecs * 1000000 + Secs.


%%%
%%% Serve queries about user last online
%%%
process_sm_iq(From, To, #iq{type = Type, sub_el = SubEl} = IQ) ->
    case Type of
        set ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_NOT_ALLOWED]};
        get ->
            User = To#jid.luser,
            Server = To#jid.lserver,
            {Subscription, _Groups} =
                ejabberd_hooks:run_fold(
                  roster_get_jid_info, Server,
                  {none, []}, [User, Server, From]),
            if
                (Subscription == both) or (Subscription == from) ->
                    UserListRecord = ejabberd_hooks:run_fold(
                                       privacy_get_user_list, Server,
                                       #userlist{},
                                       [User, Server]),
                    case ejabberd_hooks:run_fold(
                           privacy_check_packet, Server,
                           allow,
                           [User, Server, UserListRecord,
                            {To, From,
                             {xmlelement, <<"presence">>, [], []}},
                            out]) of
                        allow ->
                            get_last_iq(IQ, SubEl, User, Server);
                        deny ->
                            IQ#iq{type = error,
                                  sub_el = [SubEl, ?ERR_FORBIDDEN]}
                    end;
                true ->
                    IQ#iq{type = error,
                          sub_el = [SubEl, ?ERR_FORBIDDEN]}
            end
    end.

%% @spec (LUser::string(), LServer::string()) ->
%%      {ok, TimeStamp::integer(), Status::string()} | not_found | {error, Reason}
get_last(LUser, LServer) ->
    case get_last_q(LServer, LUser) of
        {rows, []} ->
            not_found;
        {rows, [[{<<"seconds">>, TimeStamp}, {<<"state">>, Status}]]} ->
            TimeStampInt = list_to_integer(binary_to_list(TimeStamp)),
            {ok, TimeStampInt, Status};
        _ ->
            {error, invalid_result}
    end.

get_last_iq(IQ, SubEl, LUser, LServer) ->
    case ejabberd_sm:get_user_resources(LUser, LServer) of
        [] ->
            case get_last(LUser, LServer) of
                {error, _Reason} ->
                    IQ#iq{type = error, sub_el = [SubEl, ?ERR_INTERNAL_SERVER_ERROR]};
                not_found ->
                    IQ#iq{type = error, sub_el = [SubEl, ?ERR_SERVICE_UNAVAILABLE]};
                {ok, TimeStamp, Status} ->
                    TimeStamp2 = now_to_seconds(now()),
                    Sec = TimeStamp2 - TimeStamp,
                    IQ#iq{type = result,
                          sub_el = [{xmlelement, <<"query">>,
                                     [{<<"xmlns">>, ?NS_LAST},
                                      {<<"seconds">>, integer_to_list(Sec)}],
                                     [{xmlcdata, Status}]}]}
            end;
        _ ->
            IQ#iq{type = result,
                  sub_el = [{xmlelement, <<"query">>,
                             [{<<"xmlns">>, ?NS_LAST},
                              {<<"seconds">>, <<"0">>}],
                             []}]}
    end.

on_presence_update(User, Server, _Resource, Status) ->
    TimeStamp = now_to_seconds(now()),
    store_last_info(User, Server, TimeStamp, Status).

store_last_info(User, Server, TimeStamp, Status) ->
    LServer = jlib:nameprep(Server),
    {ok, {ok, _Res}} = set_last_q(LServer, User, TimeStamp, Status).

%% @spec (LUser::string(), LServer::string()) ->
%%      {ok, TimeStamp::integer(), Status::string()} | not_found
get_last_info(LUser, LServer) ->
    case get_last(LUser, LServer) of
        {error, _Reason} ->
            not_found;
        Res ->
            Res
    end.

remove_user(User, Server) ->
    LServer = jlib:nameprep(Server),
    {ok, _, _} = del_last_q(LServer, User).

%%%===================================================================
%%% Queries
%%%===================================================================

get_last_q(Server, Username) ->
        bank:execute(Server, get_last, [Username]).

set_last_q(Server, Username, Seconds, State) ->
        T = ejabberd_bank:update_fun(get_last, [Username],
                                          update_last, [Seconds, State, Username],
                                          add_last, [Username, Seconds, State]),
        bank:batch(Server, ejabberd_bank:transaction(T)).

del_last_q(Server, Username) ->
        bank:execute(Server, del_last, [Username]).

%%%===================================================================
%%% Behaviour
%%%===================================================================

prepared_statements() ->
    [{get_last,
      <<"select seconds, state from last where username = ?">>},
     {update_last,
      <<"update last set seconds = ?, state = ? where username = ?">>},
     {add_last,
      <<"insert into last(username, seconds, state) values (?, ?, ?)">>},
     {del_last,
      <<"delete from last where username = ?">>}].
