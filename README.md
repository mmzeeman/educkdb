# educkdb

[![Hex.pm version](https://img.shields.io/hexpm/v/educkdb.svg)](https://hex.pm/packages/educkdb)
[![Hex.pm downloads](https://img.shields.io/hexpm/dt/educkdb.svg)](https://hex.pm/packages/educkdb)
[![Test](https://github.com/mmzeeman/educkdb/actions/workflows/test.yml/badge.svg)](https://github.com/mmzeeman/educkdb/actions/workflows/test.yml)

DuckDB NIF for Erlang. 

DuckDB (https://duckdb.org/) is an in-process SQL OLAP database management system. This library makes it possible to 
use DuckDB in Erlang, Elixir or other languages supported by the beam.

# Example

Connect to a database

```erlang

{ok, Db} = educkdb:open("database.db"),
{ok, Conn} = educkdb:connect(Db)
...
```

Create a table by reading a CSV or parquet files.

```erlang

{ok, _} = educkdb:query(Conn, "CREATE TABLE ontime AS SELECT * FROM 'test.csv'"),
{ok, ParquetData} = educkdb:query(Conn, "SELECT * FROM 'test.parquet'"),
...
```


# API

*Note: This is a work in progress. At the moment it is possible to open, connect, query and return integer and
varchar results. The nif function returning the query result will become a yielding nif. This is important for
large query results.*

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

#### `bind_null(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_boolean(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_int8(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_int16(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_int32(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_int64(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_hugeint(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_uint8(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_uint16(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_uint32(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_uint64(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_float(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_double(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_date(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_time(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `bind_timestamp(Stmt, Index, Value)` -> `ok` | `error`
Note: **direct c**

#### `....more`

#### `execute_statement(Stmt)` -> `{ok, result()}` | `{error, _}`

Note: **via command thread**

### `appender`

#### 

## Implementation notes

 * https://potatosalad.io/2017/08/05/latency-of-native-functions-for-erlang-and-elixir
 * https://github.com/vinoski/bitwise

