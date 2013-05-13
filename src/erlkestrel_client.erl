-module(erlkestrel_client).
-author('Stanislav Sedov <stas@deglitch.com>').
-behaviour(gen_server).

%% API
-export([start_link/3, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
	  host,
	  port,
	  reconnect_interval,
	  socket,
	  pstate,
	  queue
}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Host, Port, ReconnectInterval) ->
    gen_server:start_link(?MODULE, [Host, Port, ReconnectInterval], []).

stop(Client) ->
    gen_server:call(Client, stop).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([Host, Port, ReconnectInterval]) ->
    State = #state{host = Host,
		   port = Port,
		   reconnect_interval = ReconnectInterval,
		   pstate = erlkestrel_parser:init(),
		   queue = queue:new()},
    case connect(State) of
	{ok, NewState} ->
	    {ok, NewState};
	{error, Reason} ->
	    {stop, Reason}
    end.

handle_call({command, Type, Req}, From, State) ->
    do_request(Type, Req, From, State);
handle_call({streaming, Pid, Req}, _From, State) ->
    do_request(streaming, Req, Pid, State);
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Sock, Data}, State) ->
    inet:setopts(Sock, [{active, once}]),
    {noreply, handle_data(Data, State)};
handle_info({tcp_error, _Sock, _Reason}, State) ->
    {noreply, State};	% will tcp_closed be always send afterwards?
handle_info({tcp_closed, Sock}, State) ->
    error_logger:info_msg("Got tcp_closed for ~p~n", [Sock]),
    reply_all_waiters(State#state.queue, error, connection_lost),
    case State#state.reconnect_interval of
	disable ->
	    {stop, normal, State#state{socket = undefined}};
	_ ->
	    Self = self(),
	    spawn(fun() -> reconnect(Self, State) end),
	    {noreply, State#state{socket = undefined, queue = queue:new()}}
    end;
handle_info({connected, NewSock}, #state{socket = undefined} = State) ->
    error_logger:info_msg("Got reconnected socket ~p~n", [NewSock]),
    {noreply, State#state{socket = NewSock}};
handle_info(Msg, State) ->
    {stop, {unhandled_message, Msg}, State}.

terminate(_Reason, #state{socket = undefined}) ->
    ok;
terminate(Reason, State) ->
    error_logger:info_msg("Terminated: ~p~n", [Reason]),
    gen_tcp:close(State#state.socket),
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% private functions
%% ===================================================================
connect(State) ->
    Host = State#state.host,
    Port = State#state.port,
    case gen_tcp:connect(Host, Port, [{active, once}, {packet, line},
				      {reuseaddr, true}, binary]) of
	{ok, Socket} ->
	    error_logger:info_msg("Got socket~p~n", [Socket]),
	    {ok, State#state{socket = Socket}};
	{error, Reason} ->
	    {error, {connect_error, Reason}}
    end.

reconnect(Client, State) ->
    case connect(State) of
	{ok, NewState} ->
	    error_logger:info_msg("Successfully reconnected~n"),
	    Socket = NewState#state.socket,
	    gen_tcp:controlling_process(Socket, Client),
	    Client ! {connected, Socket};
	{error, _Reason} ->
	    error_logger:info_msg("Unable to reconnect, sleeping~n"),
	    timer:sleep(State#state.reconnect_interval),
	    reconnect(Client, State)
    end.

do_request(_Type, _Req, _From, #state{socket = undefined} = State) ->
    {reply, {error, disconnected}, State};
do_request(Type, Req, From, #state{socket = Sock, queue = Queue} = State) ->
    error_logger:info_msg("Sending via ~p: ~p~n", [Sock, Req]),
    case gen_tcp:send(Sock, Req) of
	ok ->
	    case Type of
		no_reply ->
		    {reply, ok, State};
		streaming ->
		    % return ok and remember the Pid to send data to.
		    NewQueue = queue:in({Type, From}, Queue),
		    {reply, ok, State#state{queue = NewQueue}};
		_ ->
		    % store the request data in the queue.
		    NewQueue = queue:in({Type, From}, Queue),
		    {noreply, State#state{queue = NewQueue}}
	    end;
	{error, Reason} ->
	    error_logger:info_msg("tcp:send returned~p~n", [Reason]),
	    {reply, {error, Reason}, State}
    end.

%
% Handles incoming tcp data.  Returns new state.
%
handle_data(Data,
	    #state{queue = Queue, pstate = Pstate, socket = Sock} = State) ->
    Head = queue:peek(Queue),
    case erlkestrel_parser:parse(Head, Pstate, Data) of
	{error, Reason} ->
	    error_logger:error_msg("Got error from parser: ~p~n", [Reason]),
	    gen_tcp:close(Sock),
	    self() ! {tcp_closed, Sock},
	    State#state{socket = undefined};
	% more data needed.
	{continue, NewPState} ->
	    State#state{pstate = NewPState};
	% all data has been processed
	{Result, Value, NewPState} ->
	    NewQueue = handle_reply(Result, Value, Queue),
	    State#state{pstate = NewPState, queue = NewQueue};
	% some unprocessed data left
	{Result, Value, Rest, NewPState} ->
	    NewQueue = handle_reply(Result, Value, State),
	    NewState = State#state{pstate = NewPState, queue = NewQueue},
	    handle_data(Rest, NewState)
    end.

%
% Passes reply to the consumer.  Returns new requests queue.
%
handle_reply(Result, Value, Queue) ->
    {Head, NewQueue} = queue:out(Queue),
    case Head of
	empty ->
	    error_logger:error_msg("Got stray reply from kestrel: ~p~n",
				   [Value]),
	    throw(stray_reply);
	{value, {streaming, Pid}} ->
	    Pid ! {kestrel, Value},
	    case {Result, Value} of
		{ok, done} ->
		    NewQueue;
		{error, _} ->
		    NewQueue;
		_ ->
		    Queue
	    end;
	{value, {_Type, From}} ->
	    gen_server:reply(From, {Result, Value}),
	    NewQueue
    end.

reply_all_waiters(Queue, Result, Value) ->
    case queue:is_empty(Queue) of
	true ->
	    ok;
	false ->
	    NewQueue = handle_reply(Result, Value, Queue),
	    reply_all_waiters(NewQueue, Result, Value)
    end.
