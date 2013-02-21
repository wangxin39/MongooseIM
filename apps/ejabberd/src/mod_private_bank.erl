%%%===================================================================
%%% @copyright (C) 2013, Erlang Solutions Ltd.
%%% @doc Module for querying for private storage activities via bank
%%% @end
%%%===================================================================
-module(mod_private_bank).

-behaviour(ejabberd_bank).
-export([prepared_statements/0]).

-behaviour(gen_mod).
-export([start/2,
         stop/1,
         process_sm_iq/3,
         remove_user/2]).

-include("ejabberd.hrl").
-include("jlib.hrl").

start(Host, Opts) ->
    IQDisc = gen_mod:get_opt(iqdisc, Opts, one_queue),
    ejabberd_hooks:add(remove_user, Host,
                       ?MODULE, remove_user, 50),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, ?NS_PRIVATE,
                                  ?MODULE, process_sm_iq, IQDisc).

stop(Host) ->
    ejabberd_hooks:delete(remove_user, Host,
                          ?MODULE, remove_user, 50),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_PRIVATE).

process_sm_iq(From, To, #iq{type = Type, sub_el = SubEl} = IQ) ->
    #jid{luser = LUser, lserver = LServer} = From,
    case lists:member(LServer, ?MYHOSTS) of
        true ->
            if
                From#jid.luser == To#jid.luser ->
                    {xmlelement, Name, Attrs, Els} = SubEl,
                    case Type of
                        set ->
                            {ok, {ok, ok}} = set_private_data_q(LServer, LUser, Els), 
                            IQ#iq{type = result,
                                  sub_el = [{xmlelement, Name, Attrs, []}]};
                        get ->
                            try
                                Res = get_data(LUser, LServer, Els),
                                IQ#iq{type = result,
                                      sub_el = [{xmlelement, Name, Attrs, Res}]}
                            catch _:_ ->
                                    IQ#iq{type = error,
                                          sub_el = [SubEl,
                                                    ?ERR_INTERNAL_SERVER_ERROR]}
                            end
                    end;
                true ->
                    IQ#iq{type = error,
                          sub_el = [SubEl,
                                    ?ERR_FORBIDDEN]}
            end;
        false ->
            IQ#iq{type = error, sub_el = [SubEl, ?ERR_NOT_ALLOWED]}
    end.

get_data(LUser, LServer, Els) ->
    get_data(LUser, LServer, Els, []).

get_data(_LUser, _LServer, [], Res) ->
    lists:reverse(Res);
get_data(LUser, LServer, [El | Els], Res) ->
    case El of
        {xmlelement, _Name, Attrs, _} ->
            XMLNS = xml:get_attr_s(<<"xmlns">>, Attrs),
            case get_private_data_q(LServer, LUser, XMLNS) of
                {rows, [[{<<"data">>, SData}]]} ->
                    case xml_stream:parse_element(SData) of
                        {xmlelement, _, _, _} = Data ->
                            get_data(LUser, LServer, Els, [Data | Res])
                    end;
                _ ->
                    get_data(LUser, LServer, Els, [El | Res])
            end;
        _ ->
            get_data(LUser, LServer, Els, Res)
    end.

remove_user(User, Server) ->
    {ok, _, _} = del_private_data_q(Server, User).

%%%===================================================================
%%% Queries
%%%===================================================================

set_private_data_q(Server, Username, Elements) ->
    T = fun(Module, State) ->
            NewState = lists:foldl(fun(Element, AccState) ->
                            {xmlelement, _, Attrs, _} = Element,
                            XMLNS = xml:get_attr_s(<<"xmlns">>, Attrs),
                            SElement = xml:element_to_binary(Element),
                            Update = ejabberd_bank:update_fun(get_private_data,
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
    bank:batch(Server, ejabberd_bank:transaction(T)). 

get_private_data_q(Server, Username, XMLNS) ->
    bank:execute(Server, get_private_data, [Username, XMLNS]).

del_private_data_q(Server, Username) ->
    bank:execute(Server, del_private_data, [Username]).

%%%===================================================================
%%% Behaviour
%%%===================================================================

prepared_statements() ->
    [{get_private_data,
      <<"select data from private_storage where "
        "username = ? and namespace = ?">>},
     {update_private_data,
      <<"update private_storage set data = ? where username = ? and namespace = ?">>},
     {add_private_data,
      <<"insert into private_storage(username, namespace, data) values (?, ?, ?)">>},
     {del_private_data,
      <<"delete from private_storage where username = ?">>}].
