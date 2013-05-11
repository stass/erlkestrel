-module(erlkestrel).
-author('Stanislav Sedov <stas@deglitch.com>').
-export([start/0, start/3, start_link/0,
	 start_link/3, stop/1, rawcmd/3, rawcmd/4]).
-export([version/1, flush_all/1, flush/2, reload/1, shutdown/1,
	 status/1, status/2, delete/2, set/3, set/4, get/2,
	 get_trans/3, peek/2]).

-define(TIMEOUT, 5000).

start() ->
    spawn(fun() -> start_link() end).

start(Host, Port, ReconnectInterval) ->
    spawn(fun() -> start_link(Host, Port, ReconnectInterval) end).

start_link() ->
    start_link("127.0.0.1", 22133, ?TIMEOUT).

start_link(Host, Port, ReconnectInterval)
  when is_list(Host),
       is_integer(Port),
       is_integer(ReconnectInterval) orelse ReconnectInterval =:= disable ->
    erlkestrel_client:start_link(Host, Port, ReconnectInterval).

stop(Client) ->
    erlkestrel_client:stop(Client).

rawcmd(Client, Type, Command) ->
    rawcmd(Client, Type, Command, ?TIMEOUT).

rawcmd(Client, Type, Command, Timeout) ->
    Data = [Command, <<"\r\n">>],
    call(Client, {command, Type, Data}, Timeout).

call(Client, Query, Timeout) ->
    gen_server:call(Client, Query, Timeout).

%%
%% Kestrel protocol commands.
%%
version(Client) ->
    case rawcmd(Client, single, <<"VERSION">>) of
	{ok, Reply} ->
	    case string:tokens(binary_to_list(Reply), " \r\n") of
		["VERSION", Ver] ->
		    {ok, Ver};
		_ ->
		    {error, Reply}
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

flush_all(Client) ->
    rawcmd(Client, single, <<"FLUSH_ALL">>).

flush(Client, Queue) ->
    expect_end(rawcmd(Client, single, [<<"FLUSH ">>, Queue])).

reload(Client) ->
    rawcmd(Client, single, <<"RELOAD">>).

shutdown(Client) ->
    rawcmd(Client, no_reply, <<"SHUTDOWN">>).

delete(Client, Queue) ->
    case rawcmd(Client, single, [<<"DELETE ">>, Queue]) of
	{ok, Reply} ->
	    case Reply of
		<<"DELETED\r\n">> ->
		    ok;
		_ ->
		    {error, Reply}
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

status(Client) ->
    case rawcmd(Client, single, <<"STATUS">>) of
	<<"UP\r\n">> ->
	    {ok, up};
	<<"READONLY\r\n">> ->
	    {ok, readonly};
	<<"QUIESCENT\r\n">> ->
	    {ok, quiescent};
	Msg ->
	    {error, Msg}
    end.

status(Client, up) ->
    expect_end(rawcmd(Client, single, [<<"STATUS UP">>]));
status(Client, readonly) ->
    expect_end(rawcmd(Client, single, [<<"STATUS READONLY">>]));
status(Client, quiescent) ->
    expect_end(rawcmd(Client, single, [<<"STATUS QUIESCENT">>])).

expect_end(Reply) ->
    case Reply of
	{ok, Msg} ->
	    case Msg of
		<<"END\r\n">> ->
		    ok;
		_ ->
		    {error, Reply}
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

set(Client, Queue, Data) ->
    set(Client, Queue, Data, 0).

set(Client, Queue, Data, Expiration) ->
    % SET <queue> <flags (ignored)> <exp> <size>
    Size = iolist_size(Data),
    Cmd = [<<"SET ">>, Queue, <<" 0 ">>,
	   integer_to_list(Expiration), <<" ">>,
	   integer_to_list(Size), <<"\r\n">>,
	   Data],
    case rawcmd(Client, single, Cmd) of
	{ok, Reply} ->
	    case Reply of
		<<"STORED\r\n">> ->
		    ok;
		<<"NOT_STORED\r\n">> ->
		    {error, not_stored};
		_ ->
		    {error, Reply}
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

get(Client, Queue) ->
    Cmd = [<<"GET ">>, Queue],
    rawcmd(Client, get, Cmd).

get_trans(Client, Queue, TransFlags) ->
    FlagsStr = case TransFlags of
		   open ->
		       <<"/open">>;
		   close ->
		       <<"/close">>;
		   close_open ->
		       <<"/close/open">>;
		   abort ->
		       <<"/abort">>;
		   _ ->
		       {error, invalid_flags}
	       end,
    Cmd = [<<"GET ">>, Queue, FlagsStr],
    case rawcmd(Client, get, Cmd) of
	{error, not_found} when TransFlags =:= close orelse
				TransFlags =:= abort ->
	    ok;
	Result ->
	    Result
    end.

peek(Client, Queue) ->
    Cmd = [<<"GET ">>, Queue, <<"/peek">>],
    rawcmd(Client, get, Cmd).
