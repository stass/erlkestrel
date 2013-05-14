-module(fetch_messages).
-author('Stanislav Sedov <stas@deglitch.com>').
-export([start/1, start/3]).

-define(BATCH_SIZE, 50).

start(Queue) ->
    start("127.0.0.1", 22133, Queue).
start(Host, Port, Queue) ->
    {ok, Client} = erlkestrel:start_link(Host, Port, 1000),
    {ok, SClient} = erlkestrel:subscribe(Client, Queue, self(), ?BATCH_SIZE),
    error_logger:info_msg("Subscribed to queue ~p~n", [Queue]),
    receive_msgs(SClient, 0, 0).

receive_msgs(SClient, N, Inflight) ->
    NewInf = case Inflight of
		 ?BATCH_SIZE div 2 ->
		     erlkestrel:ack(SClient, Inflight),
		     0;
		 _ ->
		     Inflight
	     end,
    print_status(N),
    receive
	_ ->
	    erlkestrel:ack(SClient, 1),
	    receive_msgs(SClient, N + 1, NewInf + 1)
    end.

print_status(N) when N rem 100 == 0 ->
    error_logger:info_msg("Processed ~p messages~n", [N]);
print_status(_) ->
    ok.
