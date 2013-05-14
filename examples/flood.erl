-module(flood).
-author('Stanislav Sedov <stas@deglitch.com>').
-export([start/1, start/3]).

start(Queue) ->
    start("127.0.0.1", 22133, Queue).
start(Host, Port, Queue) ->
    {ok, Client} = erlkestrel:start_link(Host, Port, 1000),
    error_logger:info_msg("Sending to queue ~p~n", [Queue]),
    flood(Client, Queue, 0).

flood(Client, Queue, N) ->
    print_status(N),
    erlkestrel:set(Client, Queue, "testtest"),
    timer:sleep(10),
    flood(Client, Queue, N + 1).

print_status(N) when N rem 1000 == 0 ->
    error_logger:info_msg("Sent ~p messages~n", [N]);
print_status(_) ->
    ok.
