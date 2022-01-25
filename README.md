# educkdb

DuckDB NIF for Erlang

# API


### Startup

#### `open(filename, options)` -> `{ok, DB}` | `{error, msg}` 

Note: (**dirty scheduler**) 

#### `connect(db)` -> `{ok, Conn}` | `{error, msg}`

Note, creates a nif thread with a command queue, all calls which uses the 
connection as parameter will be send to the thread via the queue.

#### `disconnect(connection)` -> `ok` | `{error, msg}`

Note: **via command thread**.

Explicitly disconnect the connection. This also explicitly finalizes the thread.
It is not required to call this. The garbage collector will disconnection 
automatically.

#### `close(db)` -> `ok` | `{error, msg}`

Note: (**dirty scheduler**)

Explicitly close the db. It is not required to call this. The garbage collector 
will disconnection automatically.

### Querying

#### `query(Conn, Query)` -> `{ok, QueryResult}` | `{error, msg}`

Note: **via command thread**

Run a query, return the result.

### Prepared Statements

#### `prepare(Conn, Query)` -> `{ok, Stmt}` | `{error, _}`

Note: **via command thread**

#### `bind(Stmt, Values)` -> `ok` | `{error, _}`

Note: **direct c**

#### `bind_boolean(Stmt, Index, Value)`
Note: **direct c**

#### `bind_int8(Stmt, Index, Value)`
Note: **direct c**

#### `bind_int16(Stmt, Index, Value)`
Note: **direct c**

#### `bind_int32(Stmt, Index, Value)`
Note: **direct c**

#### `bind_int64(Stmt, Index, Value)`
Note: **direct c**

#### `bind_hugeint(Stmt, Index, Value)`
Note: **direct c**

#### `bind_uint8(Stmt, Index, Value)`
Note: **direct c**

#### `bind_uint16(Stmt, Index, Value)`
Note: **direct c**

#### `bind_uint32(Stmt, Index, Value)`
Note: **direct c**

#### `bind_uint64(Stmt, Index, Value)`
Note: **direct c**

#### `bind_float(Stmt, Index, Value)`
Note: **direct c**

#### `bind_double(Stmt, Index, Value)`
Note: **direct c**

#### `bind_date(Stmt, Index, Value)`
Note: **direct c**

#### `bind_time(Stmt, Index, Value)`
Note: **direct c**

#### `bind_timestamp(Stmt, Index, Value)`
Note: **direct c**

#### `....more`

#### `execute(Stmt)`

Note: **via command thread**

### `appender`

#### 


