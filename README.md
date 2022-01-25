# educkdb

DuckDB NIF for Erlang

# API


### Startup

#### `open(filename, options)` -> `{ok, DB}` | `{error, msg}` 

Note: (**dirty**) 

#### `connect(db)` -> `{ok, Conn}` | `{error, msg}`

Note, creates a nif thread with a command queue, all calls which uses the 
connection as parameter will be send to the thread via the queue.

#### `disconnect(connection)` -> `ok` | `{error, msg}`

Explicitly disconnect the connection. This also explicitly finalizes the thread.
It is not required to call this. The garbage collector will disconnection 
automatically.

#### `close(db)` -> `ok` | `{error, msg}`

Explicitly close the db. It is not required to call this. The garbage collector 
will disconnection automatically.

### Querying



