-module(fetch_messages).
-author('Stanislav Sedov <stas@deglitch.com>').
-export([start/1, start/3]).

start(Queue) ->
    start("127.0.0.1", 22133, Queue).
start(Host, Port, Queue) ->
    {ok, Client} = erlkestrel:start_link(Host, Port, 1000),
    {ok, SClient} = erlkestrel:subscribe(Client, Queue),
    error_logger:info_msg("Subscribed to queue ~p~n", [Queue]),
    receive_msgs(SClient, 0).

receive_msgs(SClient, N) ->
    print_status(N),
    receive
	_ ->
	    erlkestrel:ack(SClient, 1),
	    receive_msgs(SClient, N + 1)
    end.

print_status(N) when N rem 1000 == 1 ->
    error_logger:info_msg("Processed ~p messages~n", [N]);
print_status(_) ->
    ok.
