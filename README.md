erlkestrel
==========

Erlang client for kestrel

# Usage
```erlang
{ok, Client} = erlkestrel:start_link(Host, Port, ReconnectInterval),
erlkestrel:version(Client),
erlkestrel:set(Client, "myqueue", "test").
```
# Supported commands

The following kestrel command are available in the erlkestrel module:

* confirm/3
* delete/2
* flush/2
* flush_all/1
* get/2
* get_trans/3
* monitor/5
* peek/2
* reload/1
* set/3 and set/4
* shutdown/1
* stats/1
* status/1
* status/2
* version/1

All commands take erlkestrel_client pid as a first argument.
