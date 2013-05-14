-module(erlkestrel_parser).
-author('Stanislav Sedov <stas@deglitch.com>').

-export([init/0, parse/3]).

-record(pstate, {state, expect, data}).

%%
%% API functions.
%%

init() ->
    #pstate{state = init, data = []}.

parse({value, {oneline, _}}, #pstate{state = init} = State, Data) ->
    {ok, Data, State};
parse({value, {get, _}}, #pstate{state = init} = State, Data) ->
    case Data of
	<<"END\r\n">> ->
	    {error, not_found, State};
	_ ->
	    ["VALUE", _Q, _Flags, SizeStr] = string:tokens(binary_to_list(Data),
							" \r\n"),
	    Size = list_to_integer(SizeStr),
	    {continue, State#pstate{state = get_data, data = [], expect = Size}}
    end;
parse({value, {get, _}},
      #pstate{state = get_data, expect = Expected, data = Data} = State,
      NewData) ->
    NewDataSize = size(NewData),
    case NewData of
	<<Value:Expected/binary, "\r\n">> ->
	    {continue, State#pstate{state = expect_end, data = [Value | Data]}};
	_ when NewDataSize =< Expected ->
	    {continue, State#pstate{expect = Expected - NewDataSize,
				   data = [NewData | Data]}};
	_ ->
	    {error, invalid_response}
    end;
parse({value, {get, _}}, #pstate{state = expect_end} = State, NewData) ->
    case NewData of
	<<"END\r\n">> ->
	    Value = list_to_binary(lists:reverse(State#pstate.data)),
	    {ok, Value, State#pstate{state = init, data = []}};
	_ ->
	    {error, invalid_response}
    end;
parse({value, {multiline, _}},
      #pstate{state = init, data = Data} = State, NewData) ->
    % Data length without trailing "\r\n".
    LineLength = size(NewData) - 2,
    case NewData of
	<<"END\r\n">> ->
	    Value = lists:reverse(Data),
	    {ok, Value, State#pstate{data = []}};
	<<Line:LineLength/binary, "\r\n">> ->
	    {continue, State#pstate{data = [Line | Data]}};
	_ ->
	    {error, invalid_response}
    end;
parse({value, {streaming, _}}, #pstate{state = init} = State, Data) ->
    case Data of
	<<"END\r\n">> ->
	    {ok, done, State};
	_ ->
	    ["VALUE", _Q, _Flags, SizeStr] = string:tokens(binary_to_list(Data),
							" \r\n"),
	    Size = list_to_integer(SizeStr),
	    {continue, State#pstate{state = get_data, data = [], expect = Size}}
    end;
parse({value, {streaming, _}},
      #pstate{state = get_data, expect = Expected, data = Data} = State,
      NewData) ->
    NewDataSize = size(NewData),
    case NewData of
	<<Value:Expected/binary, "\r\n">> ->
	    {continue, State#pstate{state = expect_end, data = [Value | Data]}};
	_ when NewDataSize =< Expected ->
	    {continue, State#pstate{expect = Expected - NewDataSize,
				   data = [NewData | Data]}};
	_ ->
	    {error, invalid_response}
    end;
parse({value, {streaming, _}}, #pstate{state = expect_end} = State, NewData) ->
    case NewData of
	<<"END\r\n">> ->
	    Value = list_to_binary(lists:reverse(State#pstate.data)),
	    {ok, Value, State#pstate{state = init, data = []}};
	_ ->
	    {error, invalid_response}
    end;
parse(empty, _State, Data) ->
    {error, {unexpected_reply, Data}}.
