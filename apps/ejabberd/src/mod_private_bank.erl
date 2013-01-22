%%%===================================================================
%%% @copyright (C) 2013, Erlang Solutions Ltd.
%%% @doc Module for querying for private storage activities via bank
%%% @end
%%%===================================================================
-module(mod_private_bank).
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
                            {ok, {ok, ok}} = ejabberd_bank:set_private_data(LServer, LUser, Els), 
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
            case ejabberd_bank:get_private_data(LServer, LUser, XMLNS) of
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
    {ok, _, _} = ejabberd_bank:del_private_data(Server, User).
