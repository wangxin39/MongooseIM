%%%===================================================================
%%% @copyright (C) 2013, Erlang Solutions Ltd.
%%% @doc Module for querying for privacy activities via bank
%%% @end
%%%===================================================================
-module(mod_privacy_bank).

-behaviour(ejabberd_bank).
-export([prepared_statements/0]).

-behaviour(gen_mod).

-export([start/2, stop/1,
	 process_iq/3,
	 process_iq_set/4,
	 process_iq_get/5,
	 get_user_list/3,
	 check_packet/6,
	 remove_user/2,
	 updated_list/3]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("mod_privacy.hrl").

start(Host, Opts) ->
    IQDisc = gen_mod:get_opt(iqdisc, Opts, one_queue),
    ejabberd_hooks:add(privacy_iq_get, Host,
		       ?MODULE, process_iq_get, 50),
    ejabberd_hooks:add(privacy_iq_set, Host,
		       ?MODULE, process_iq_set, 50),
    ejabberd_hooks:add(privacy_get_user_list, Host,
		       ?MODULE, get_user_list, 50),
    ejabberd_hooks:add(privacy_check_packet, Host,
		       ?MODULE, check_packet, 50),
    ejabberd_hooks:add(privacy_updated_list, Host,
		       ?MODULE, updated_list, 50),
    ejabberd_hooks:add(remove_user, Host,
		       ?MODULE, remove_user, 50),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, ?NS_PRIVACY,
				  ?MODULE, process_iq, IQDisc).

stop(Host) ->
    ejabberd_hooks:delete(privacy_iq_get, Host,
			  ?MODULE, process_iq_get, 50),
    ejabberd_hooks:delete(privacy_iq_set, Host,
			  ?MODULE, process_iq_set, 50),
    ejabberd_hooks:delete(privacy_get_user_list, Host,
			  ?MODULE, get_user_list, 50),
    ejabberd_hooks:delete(privacy_check_packet, Host,
			  ?MODULE, check_packet, 50),
    ejabberd_hooks:delete(privacy_updated_list, Host,
			  ?MODULE, updated_list, 50),
    ejabberd_hooks:delete(remove_user, Host,
			  ?MODULE, remove_user, 50),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, ?NS_PRIVACY).

process_iq(_From, _To, IQ) ->
    SubEl = IQ#iq.sub_el,
    IQ#iq{type = error, sub_el = [SubEl, ?ERR_NOT_ALLOWED]}.

process_iq_get(_, From, _To, #iq{sub_el = SubEl},
	       #userlist{name = Active}) ->
    #jid{luser = LUser, lserver = LServer} = From,
    {xmlelement, _, _, Els} = SubEl,
    case xml:remove_cdata(Els) of
	[] ->
	    process_lists_get(LUser, LServer, Active);
	[{xmlelement, Name, Attrs, _SubEls}] ->
	    case Name of
		<<"list">> ->
		    ListName = xml:get_attr(<<"name">>, Attrs),
		    process_list_get(LUser, LServer, ListName);
		_ ->
		    {error, ?ERR_BAD_REQUEST}
	    end;
	_ ->
	    {error, ?ERR_BAD_REQUEST}
    end.

process_lists_get(LUser, LServer, Active) ->
    Default = case get_default_privacy_list_q(LServer, LUser) of
        {rows, [[{<<"name">>, DefName}]]} ->
            DefName;
        _ ->
            none
    end,
    case get_privacy_list_names_q(LServer, LUser) of
        {rows, []} ->
	    {result, [{xmlelement, <<"query">>, [{<<"xmlns">>, ?NS_PRIVACY}], []}]};
        {rows, Rows} ->
            LItems = [{xmlelement, <<"list">>, Row, []} || Row <- Rows],
            DItems = case Default of
                none ->
                    LItems;
                _ ->
                    [{xmlelement, <<"default">>,
                      [{<<"name">>, Default}], []} | LItems]
            end,
            ADItems =
                      case Active of
                none ->
                    DItems;
                _ ->
                    [{xmlelement, <<"active">>,
                      [{<<"name">>, Active}], []} | DItems]
            end,
            {result,
             [{xmlelement, <<"query">>, [{<<"xmlns">>, ?NS_PRIVACY}],
               ADItems}]};
        _ ->
            {error, ?ERR_INTERNAL_SERVER_ERROR}
    end.

process_list_get(LUser, LServer, {value, Name}) ->
    case get_privacy_list_id_q(LServer, LUser, Name) of
        {rows, []} ->
	    {error, ?ERR_ITEM_NOT_FOUND};
        {rows, [[{<<"id">>, ID}]]} ->
            case get_privacy_list_data_by_id_q(LServer, ID) of
                {rows, Rows} ->
                    Items = lists:map(fun proplist_to_item/1, Rows),
		    LItems = lists:map(fun item_to_xml/1, Items),
		    {result,
		     [{xmlelement, <<"query">>, [{<<"xmlns">>, ?NS_PRIVACY}],
		       [{xmlelement, <<"list">>,
			 [{<<"name">>, Name}], LItems}]}]};
		_ ->
		    {error, ?ERR_INTERNAL_SERVER_ERROR}
            end;
	_ ->
	    {error, ?ERR_INTERNAL_SERVER_ERROR}
    end;

process_list_get(_LUser, _LServer, false) ->
    {error, ?ERR_BAD_REQUEST}.

item_to_xml(Item) ->
    Attrs1 = [{<<"action">>, action_to_binary(Item#listitem.action)},
	      {<<"order">>, order_to_binary(Item#listitem.order)}],
    Attrs2 = case Item#listitem.type of
		 none ->
		     Attrs1;
		 Type ->
		     [{<<"type">>, type_to_binary(Item#listitem.type)},
		      {<<"value">>, value_to_binary(Type, Item#listitem.value)} |
		      Attrs1]
	     end,
    SubEls = case Item#listitem.match_all of
		 true ->
		     [];
		 false ->
		     SE1 = case Item#listitem.match_iq of
			       true ->
				   [{xmlelement, <<"iq">>, [], []}];
			       false ->
				   []
			   end,
		     SE2 = case Item#listitem.match_message of
			       true ->
				   [{xmlelement, <<"message">>, [], []} | SE1];
			       false ->
				   SE1
			   end,
		     SE3 = case Item#listitem.match_presence_in of
			       true ->
				   [{xmlelement, <<"presence-in">>, [], []} | SE2];
			       false ->
				   SE2
			   end,
		     SE4 = case Item#listitem.match_presence_out of
			       true ->
				   [{xmlelement, <<"presence-out">>, [], []} | SE3];
			       false ->
				   SE3
			   end,
		     SE4
	     end,
    {xmlelement, <<"item">>, Attrs2, SubEls}.

action_to_binary(Action) ->
    case Action of
	allow -> <<"allow">>;
	deny -> <<"deny">>
    end.

order_to_binary(Order) ->
    integer_to_binary(Order).

type_to_binary(Type) ->
    case Type of
	jid -> <<"jid">>;
	group -> <<"group">>;
	subscription -> <<"subscription">>
    end.

value_to_binary(Type, Val) ->
    case Type of
	jid -> jlib:jid_to_binary(Val);
	group -> Val;
	subscription ->
	    case Val of
		both -> <<"both">>;
		to -> <<"to">>;
		from -> <<"from">>;
		none -> <<"none">>
	    end
    end.

binary_to_action(B) ->
    case B of
	<<"allow">> -> allow;
	<<"deny">> -> deny
    end.

binary_to_integer(B) ->
    list_to_integer(binary_to_list(B)).

integer_to_binary(I) ->
    list_to_binary(integer_to_list(I)).

process_iq_set(_, From, _To, #iq{sub_el = SubEl}) ->
    #jid{luser = LUser, lserver = LServer} = From,
    {xmlelement, _, _, Els} = SubEl,
    case xml:remove_cdata(Els) of
	[{xmlelement, Name, Attrs, SubEls}] ->
	    ListName = xml:get_attr(<<"name">>, Attrs),
	    case Name of
		<<"list">> ->
		    process_list_set(LUser, LServer, ListName,
				     xml:remove_cdata(SubEls));
		<<"active">> ->
		    process_active_set(LUser, LServer, ListName);
		<<"default">> ->
		    process_default_set(LUser, LServer, ListName);
		_ ->
		    {error, ?ERR_BAD_REQUEST}
	    end;
	_ ->
	    {error, ?ERR_BAD_REQUEST}
    end.

process_default_set(LUser, LServer, {value, Name}) ->
    case set_default_privacy_list_q(LServer, LUser, Name) of
        {ok, {ok, ok}} ->
            {result, []};
        {ok, {ok, {error, not_found}}} ->
            {error, ?ERR_ITEM_NOT_FOUND};
        _ ->
            {error, ?ERR_INTERNAL_SERVER_ERROR}
    end;

process_default_set(LUser, LServer, false) ->
    case del_default_privacy_list_q(LServer, LUser) of
        {ok, _, _} ->
            {result, []};
        _ ->
            {error, ?ERR_INTERNAL_SERVER_ERROR}
    end.

process_active_set(LUser, LServer, {value, Name}) ->
    case get_privacy_list_id_q(LServer, LUser, Name) of
        {rows, []} ->
            {error, ?ERR_ITEM_NOT_FOUND};
        {rows, [[{<<"id">>, ID}]]} ->
            case get_privacy_list_data_by_id_q(LServer, ID) of
                {rows, Rows} ->
                    Items = lists:map(fun proplist_to_item/1, Rows),
		    NeedDb = is_list_needdb(Items),
		    {result, [], #userlist{name = Name, list = Items, needdb = NeedDb}};
                _ ->
                    {error, ?ERR_INTERNAL_SERVER_ERROR}
            end;
        _ ->
            {error, ?ERR_INTERNAL_SERVER_ERROR}
    end;

process_active_set(_LUser, _LServer, false) ->
    {result, [], #userlist{}}.


process_list_set(LUser, LServer, {value, Name}, Els) ->
    case parse_items(Els) of
	false ->
	    {error, ?ERR_BAD_REQUEST};
	remove ->
            case del_privacy_list_q(LServer, LUser, Name) of
                {ok, {ok, {error, conflict}}} ->
                    {error, ?ERR_CONFLICT};
                {ok, {ok, {result, _} = Res}} ->
                    ejabberd_router:route(
                        jlib:make_jid(LUser, LServer, <<"">>),
                        jlib:make_jid(LUser, LServer, <<"">>),
                        {xmlelement, <<"broadcast">>, [],
                         [{privacy_list,
                           #userlist{name = Name, list = []},
                           Name}]}),
                    Res;
                _ ->
                    {error, ?ERR_INTERNAL_SERVER_ERROR}
            end;
	List ->
	    RItems = lists:map(fun item_to_list/1, List),
            case set_privacy_list_q(LServer, LUser, Name, RItems) of
                {ok, {ok, {result, _} = Res}} ->
		    NeedDb = is_list_needdb(List),
		    ejabberd_router:route(
		      jlib:make_jid(LUser, LServer, <<"">>),
		      jlib:make_jid(LUser, LServer, <<"">>),
		      {xmlelement, <<"broadcast">>, [],
		       [{privacy_list,
			 #userlist{name = Name, list = List, needdb = NeedDb},
			 Name}]}),
		    Res;
		_ ->
		    {error, ?ERR_INTERNAL_SERVER_ERROR}
	    end
    end;

process_list_set(_LUser, _LServer, false, _Els) ->
    {error, ?ERR_BAD_REQUEST}.

parse_items([]) ->
    remove;
parse_items(Els) ->
    parse_items(Els, []).

parse_items([], Res) ->
    %% Sort the items by their 'order' attribute
    lists:keysort(#listitem.order, Res);
parse_items([{xmlelement, <<"item">>, Attrs, SubEls} | Els], Res) ->
    Type   = xml:get_attr(<<"type">>,   Attrs),
    Value  = xml:get_attr(<<"value">>,  Attrs),
    SAction = xml:get_attr(<<"action">>, Attrs),
    BOrder = xml:get_attr(<<"order">>,  Attrs),
    Action = case catch binary_to_action(element(2, SAction)) of
		 {'EXIT', _} -> false;
		 Val -> Val
	     end,
    Order = case catch binary_to_integer(element(2, BOrder)) of
		{'EXIT', _} ->
		    false;
		IntVal ->
		    if
			IntVal >= 0 ->
			    IntVal;
			true ->
			    false
		    end
	    end,
    if
	(Action /= false) and (Order /= false) ->
	    I1 = #listitem{action = Action, order = Order},
	    I2 = case {Type, Value} of
		     {{value, T}, {value, V}} ->
			 case T of
			     <<"jid">> ->
				 case jlib:binary_to_jid(V) of
				     error ->
					 false;
				     JID ->
					 I1#listitem{
					   type = jid,
					   value = jlib:jid_tolower(JID)}
				 end;
			     <<"group">> ->
				 I1#listitem{type = group,
					     value = V};
			     <<"subscription">> ->
				 case V of
				     <<"none">> ->
					 I1#listitem{type = subscription,
						     value = none};
				     <<"both">> ->
					 I1#listitem{type = subscription,
						     value = both};
				     <<"from">> ->
					 I1#listitem{type = subscription,
						     value = from};
				     <<"to">> ->
					 I1#listitem{type = subscription,
						     value = to};
				     _ ->
					 false
				 end
			 end;
		     {{value, _}, false} ->
			 false;
		     _ ->
			 I1
		 end,
	    case I2 of
		false ->
		    false;
		_ ->
		    case parse_matches(I2, xml:remove_cdata(SubEls)) of
			false ->
			    false;
			I3 ->
			    parse_items(Els, [I3 | Res])
		    end
	    end;
	true ->
	    false
    end;

parse_items(_, _Res) ->
    false.

parse_matches(Item, []) ->
    Item#listitem{match_all = true};
parse_matches(Item, Els) ->
    parse_matches1(Item, Els).

parse_matches1(Item, []) ->
    Item;
parse_matches1(Item, [{xmlelement, <<"message">>, _, _} | Els]) ->
    parse_matches1(Item#listitem{match_message = true}, Els);
parse_matches1(Item, [{xmlelement, <<"iq">>, _, _} | Els]) ->
    parse_matches1(Item#listitem{match_iq = true}, Els);
parse_matches1(Item, [{xmlelement, <<"presence-in">>, _, _} | Els]) ->
    parse_matches1(Item#listitem{match_presence_in = true}, Els);
parse_matches1(Item, [{xmlelement, <<"presence-out">>, _, _} | Els]) ->
    parse_matches1(Item#listitem{match_presence_out = true}, Els);
parse_matches1(_Item, [{xmlelement, _, _, _} | _Els]) ->
    false.

is_list_needdb(Items) ->
    lists:any(
      fun(X) ->
	      case X#listitem.type of
		  subscription -> true;
		  group -> true;
		  _ -> false
	      end
      end, Items).

get_user_list(_, User, Server) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    case get_default_privacy_list_q(LServer, LUser) of
        {rows, []} ->
            #userlist{};
        {rows, [[{<<"name">>, Default}]]} ->
            case get_privacy_list_data_q(LServer, LUser, Default) of
                {rows, Rows} ->
                    Items = lists:map(fun proplist_to_item/1, Rows),
		    NeedDb = is_list_needdb(Items),
		    #userlist{name = Default, list = Items, needdb = NeedDb};
		_ ->
		    #userlist{}
	    end;
	_ ->
	    #userlist{}
    end.

%% From is the sender, To is the destination.
%% If Dir = out, User@Server is the sender account (From).
%% If Dir = in, User@Server is the destination account (To).
check_packet(_, User, Server,
	     #userlist{list = List, needdb = NeedDb},
	     {From, To, {xmlelement, PName, Attrs, _}},
	     Dir) ->
    case List of
	[] ->
	    allow;
	_ ->
	    PType = case PName of
			<<"message">> -> message;
			<<"iq">> -> iq;
			<<"presence">> ->
			    case xml:get_attr_s(<<"type">>, Attrs) of
				%% notification
				<<"">> -> presence;
				<<"unavailable">> -> presence;
				%% subscribe, subscribed, unsubscribe,
				%% unsubscribed, error, probe, or other
				_ -> other
			    end
		    end,
	    PType2 = case {PType, Dir} of
			 {message, in} -> message;
			 {iq, in} -> iq;
			 {presence, in} -> presence_in;
			 {presence, out} -> presence_out;
			 {_, _} -> other
		     end,
	    LJID = case Dir of
		       in -> jlib:jid_tolower(From);
		       out -> jlib:jid_tolower(To)
		   end,
	    {Subscription, Groups} =
		case NeedDb of
		    true -> ejabberd_hooks:run_fold(roster_get_jid_info,
						    jlib:nameprep(Server),
						    {none, []},
						    [User, Server, LJID]);
		    false -> {[], []}
		end,
	    check_packet_aux(List, PType2, LJID, Subscription, Groups)
    end.

check_packet_aux([], _PType, _JID, _Subscription, _Groups) ->
    allow;
check_packet_aux([Item | List], PType, JID, Subscription, Groups) ->
    #listitem{type = Type, value = Value, action = Action} = Item,
    case is_ptype_match(Item, PType) of
	true ->
	    case Type of
		none ->
		    Action;
		_ ->
		    case is_type_match(Type, Value,
				       JID, Subscription, Groups) of
			true ->
			    Action;
			false ->
			    check_packet_aux(List, PType,
					     JID, Subscription, Groups)
		    end
	    end;
	false ->
	    check_packet_aux(List, PType, JID, Subscription, Groups)
    end.


is_ptype_match(Item, PType) ->
    case Item#listitem.match_all of
	true ->
	    true;
	false ->
	    case PType of
		message ->
		    Item#listitem.match_message;
		iq ->
		    Item#listitem.match_iq;
		presence_in ->
		    Item#listitem.match_presence_in;
		presence_out ->
		    Item#listitem.match_presence_out;
		other ->
		    false
	    end
    end.


is_type_match(Type, Value, JID, Subscription, Groups) ->
    case Type of
	jid ->
	    case Value of
		{<<"">>, Server, <<"">>} ->
		    case JID of
			{_, Server, _} ->
			    true;
			_ ->
			    false
		    end;
		{User, Server, <<"">>} ->
		    case JID of
			{User, Server, _} ->
			    true;
			_ ->
			    false
		    end;
		_ ->
		    Value == JID
	    end;
	subscription ->
	    Value == Subscription;
	group ->
	    lists:member(Value, Groups)
    end.


remove_user(User, Server) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    {ok, {ok, ok}} = del_privacy_lists_q(LServer, LUser). 

updated_list(_,
	     #userlist{name = OldName} = Old,
             #userlist{name = NewName} = New) ->
    case OldName == NewName of
        true -> New;
        _ -> Old
    end.

proplist_to_item(Proplist) ->
    {<<"t">>, BType} = lists:keyfind(<<"t">>, 1, Proplist),
    {<<"value">>, BValue} = lists:keyfind(<<"value">>, 1, Proplist),
    {<<"action">>, BAction} = lists:keyfind(<<"action">>, 1, Proplist),
    {<<"ord">>, Order} = lists:keyfind(<<"ord">>, 1, Proplist),
    {<<"match_all">>, IMatchAll} = lists:keyfind(<<"match_all">>, 1, Proplist),
    {<<"match_iq">>, IMatchIQ} = lists:keyfind(<<"match_iq">>, 1, Proplist),
    {<<"match_message">>, IMatchMessage} = lists:keyfind(<<"match_message">>, 1, Proplist),
    {<<"match_presence_in">>, IMatchPresenceIn} = lists:keyfind(<<"match_presence_in">>,
                                                                1, Proplist),
    {<<"match_presence_out">>, IMatchPresenceOut} = lists:keyfind(<<"match_presence_out">>,
                                                                  1, Proplist),
    MatchAll = ejabberd_bank:to_bool(IMatchAll),
    MatchIQ = ejabberd_bank:to_bool(IMatchIQ),
    MatchMessage = ejabberd_bank:to_bool(IMatchMessage),
    MatchPresenceIn = ejabberd_bank:to_bool(IMatchPresenceIn),
    MatchPresenceOut = ejabberd_bank:to_bool(IMatchPresenceOut),
    {Type, Value} =
	case BType of
	    <<"n">> ->
		{none, none};
	    <<"j">> ->
		case jlib:binary_to_jid(BValue) of
		    #jid{} = JID ->
			{jid, jlib:jid_tolower(JID)}
		end;
	    <<"g">> ->
		{group, BValue};
	    <<"s">> ->
		case BValue of
		    <<"none">> ->
			{subscription, none};
		    <<"both">> ->
			{subscription, both};
		    <<"from">> ->
			{subscription, from};
		    <<"to">> ->
			{subscription, to}
		end
	end,
    Action =
	case BAction of
	    <<"a">> -> allow;
	    <<"d">> -> deny
	end,
    #listitem{type = Type,
	      value = Value,
	      action = Action,
	      order = Order,
	      match_all = MatchAll,
	      match_iq = MatchIQ,
	      match_message = MatchMessage,
	      match_presence_in = MatchPresenceIn,
	      match_presence_out = MatchPresenceOut
	     }.

item_to_list(#listitem{type = Type,
		      value = Value,
		      action = Action,
		      order = Order,
		      match_all = MatchAll,
		      match_iq = MatchIQ,
		      match_message = MatchMessage,
		      match_presence_in = MatchPresenceIn,
		      match_presence_out = MatchPresenceOut
		     }) ->
    {BType, BValue} =
	case Type of
	    none ->
		{<<"n">>, <<"">>};
	    jid ->
		{<<"j">>, jlib:jid_to_binary(Value)};
	    group ->
		{<<"g">>, Value};
	    subscription ->
		case Value of
		    none ->
			{<<"s">>, <<"none">>};
		    both ->
			{<<"s">>, <<"both">>};
		    from ->
			{<<"s">>, <<"from">>};
		    to ->
			{<<"s">>, <<"to">>}
		end
	end,
    BAction =
	case Action of
	    allow -> <<"a">>;
	    deny -> <<"d">>
	end,
    [BType, BValue, BAction, Order, MatchAll, MatchIQ,
     MatchMessage, MatchPresenceIn, MatchPresenceOut].

%%%===================================================================
%%% Queries
%%%===================================================================

get_default_privacy_list_q(Server, Username) ->
    bank:execute(Server, get_default_privacy_list, [Username]).

set_default_privacy_list_q(Server, Username, Name) ->
    T = fun(Module, State) ->
            {result_set, _, State2} = Module:execute(get_privacy_list_by_name,
                                                     [Username, Name], State),
            case Module:fetch_all(State2) of
                {rows, [], State3} ->
                    {ok, {error, not_found}, State3};
                {rows, _, State3} ->
                    F = ejabberd_bank:update_fun(get_default_privacy_list, [Username],
                                   update_default_privacy_list, [Name, Username],
                                   add_default_privacy_list, [Username, Name]),
                    F(Module, State3)
            end
    end,
    bank:batch(Server, ejabberd_bank:transaction(T)).

del_default_privacy_list_q(Server, Username) ->
    bank:execute(Server, del_default_privacy_list, [Username]).

get_privacy_list_id_q(Server, Username, SName) ->
    bank:execute(Server, get_privacy_list_id, [Username, SName]).

get_privacy_list_data_q(Server, Username, SName) ->
    bank:execute(Server, get_privacy_list_data, [Username, SName]).

get_privacy_list_data_by_id_q(Server, ID) ->
    bank:execute(Server, get_privacy_list_data_by_id, [ID]).

get_privacy_list_names_q(Server, Username) ->
    bank:execute(Server, get_privacy_list_names, [Username]).

del_privacy_list_q(Server, Username, Name) ->
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
    bank:batch(Server, ejabberd_bank:transaction(T)).

del_privacy_lists_q(Server, Username) ->
    Value = <<Username/binary, "@", Server/binary>>,
    T = fun(Module, State) ->
            {ok, _, _, State2} = Module:execute(del_privacy_lists, [Username], State),
            {ok, _, _, State3} = Module:execute(del_privacy_lists_data, [Value], State2),
            {ok, _, _, State4} = Module:execute(del_default_privacy_list, [Username], State3),
            {ok, ok, State4}
    end,
    bank:batch(Server, ejabberd_bank:transaction(T)).

set_privacy_list_q(Server, Username, Name, Items) ->
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
    bank:batch(Server, ejabberd_bank:transaction(T)).

%%%===================================================================
%%% Behaviour
%%%===================================================================

prepared_statements() ->
    [{get_default_privacy_list,
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
       "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)">>}].
