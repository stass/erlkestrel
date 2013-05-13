-module(erlkestrel_sub).
-author('Stanislav Sedov <stas@deglitch.com>').
-behaviour(gen_server).

%% API
-export([start_link/3, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
	  client,
	  pid,
	  inflight,
	  queuename,
	  queue,
	  size,
	  fetch_in_progress
}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Client, Pid, QueueName) ->
    gen_server:start_link(?MODULE, [Client, Pid, QueueName], []).

stop(Client) ->
    gen_server:call(Client, stop).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

-define(HIGH_WATERMARK, 100).
-define(LOW_WATERMARK, 50).
-define(BATCH_SIZE, 10).

init([Client, Pid, QueueName]) ->
    State = #state{client = Client,
		   queuename = QueueName,
		   pid = Pid,
		   inflight = 0,
		   fetch_in_progress = false,
		   queue = queue:new(),
		   size = 0},
    {ok, State, 0}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call({ack, N}, _From, #state{inflight = Inflight} = State)
  when is_integer(N) ->
    if
	N =:= 0 ->
	    {reply, ok, State};
	N > Inflight ->
	    {reply, {error, invalid_ack}, State};
	true ->
	    send_confirm(State#state.client, State#state.queuename, N),
	    case fetch_messages(State#state{inflight = Inflight - N}) of
		{ok, NewState} ->
		    {reply, ok, send_batch(NewState)};
		{error, NewState} ->
		    {reply, ok, send_batch(NewState), 10000}
	    end
    end;
handle_call(Msg, _From, State) ->
    error_logger:info_msg("SUB got ~p~n", Msg),
    {reply, unknown_request, State}.

handle_cast(Msg, State) ->
    error_logger:info_msg("SUB got ~p~n", Msg),
    {noreply, State}.

fetch_messages(State) ->
    if State#state.size < ?LOW_WATERMARK ->
	    send_fetch_command(State);
       true ->
	    {ok, State}
    end.

send_fetch_command(#state{fetch_in_progress = true} = State) ->
    {ok, State};
send_fetch_command(#state{fetch_in_progress = false,
			  client = Client, queuename = QueueName} = State) ->
    case erlkestrel:monitor(Client, self(), QueueName, 5, ?HIGH_WATERMARK) of
	ok ->
	    {ok, State#state{fetch_in_progress = true}};
	_ ->
	    {error, State}
    end.

send_confirm(Client, Queue, N) ->
    erlkestrel:confirm(Client, Queue, N).

handle_info({kestrel, done}, State) ->
    error_logger:info_msg("SUB got done~n"),
    case fetch_messages(State#state{fetch_in_progress = false}) of
	{ok, NewState} ->
	    {noreply, NewState};
	{error, NewState} ->
	    {noreply, NewState, 10000}
    end;
handle_info({kestrel, connection_lost}, State) ->
    error_logger:info_msg("SUB got connlost~n"),
    case fetch_messages(State#state{fetch_in_progress = false}) of
	{ok, NewState} ->
	    {noreply, NewState};
	{error, NewState} ->
	    {noreply, NewState, 10000}
    end;
handle_info({kestrel, Data}, State) ->
    error_logger:info_msg("SUB got kestrel data~n"),
    NewState = handle_incoming_data(Data, State),
    {noreply, NewState};
handle_info(timeout, State) ->
    error_logger:info_msg("Got timeout~n"),
    case fetch_messages(State) of
	{ok, NewState} ->
	    {noreply, NewState};
	{error, NewState} ->
	    {noreply, NewState, 10000}
    end;
handle_info(Msg, State) ->
    error_logger:info_msg("SUB got ~p~n", [Msg]),
    {stop, {unhandled_message, Msg}, State}.

terminate(Reason, _State) ->
    error_logger:info_msg("Terminated: ~p~n", [Reason]),
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% private functions
%% ===================================================================
handle_incoming_data(Data, #state{queue = Queue, size = Size} = State) ->
    NewQueue = queue:in(Data, Queue),
    NewSize = Size + 1,
    send_batch(State#state{queue = NewQueue, size = NewSize}).

send_batch(#state{inflight = Inflight, size = Size} = State) ->
    N = min(?BATCH_SIZE - Inflight, Size),
    send_batch_iter(State, N).

send_batch_iter(#state{pid = Pid, queue = Queue, size = Size} = State, N) ->
    case N of
	0 ->
	    State;
	_ ->
	    {{value, Item}, NewQueue} = queue:out(Queue),
	    Pid ! {kestrel, State#state.queuename, Item},
	    Inflight = State#state.inflight,
	    send_batch_iter(State#state{queue = NewQueue, size = Size - 1,
					inflight = Inflight + 1}, N - 1)
    end.
