erlkestrel
==========

Erlang client for kestrel

# Usage
```erlang
{ok, Client} = erlkestrel:start_link(Host, Port, ReconnectInterval),
erlkestrel:version(Client),
erlkestrel:set(Client, "myqueue", "test").
```
