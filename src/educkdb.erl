%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2022-2024 Maas-Maarten Zeeman
%%
%% @doc Low level erlang API for duckdb databases.
%% @end

%% Copyright 2022-2024 Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(educkdb).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").


%% low-level exports
-export([
    open/1, open/2,
    close/1,
    config_flag_info/0,

    connect/1,
    disconnect/1,

    query/2,

    prepare/2,
    execute_prepared/1,
    bind_boolean/3,
    bind_int8/3,
    bind_int16/3,
    bind_int32/3,
    bind_int64/3,
    bind_uint8/3,
    bind_uint16/3,
    bind_uint32/3,
    bind_uint64/3,
    bind_float/3,
    bind_double/3,
    bind_date/3,
    bind_time/3,
    bind_timestamp/3,
    bind_varchar/3,
    bind_null/2,

    %% Results
    result_extract/1,
    fetch_chunk/1,
    get_chunks/1,
    chunk_count/1,
    column_names/1,

    %% Chunks
    chunk_column_count/1,
    chunk_column_types/1,
    chunk_columns/1,
    chunk_size/1,

    appender_create/3,
    append_boolean/2,
    append_int8/2,
    append_int16/2,
    append_int32/2,
    append_int64/2,
    append_uint8/2,
    append_uint16/2,
    append_uint32/2,
    append_uint64/2,
    append_float/2,
    append_double/2,
    append_time/2,
    append_date/2,
    append_timestamp/2,
    append_varchar/2,
    append_null/1,
    appender_flush/1,
    appender_end_row/1
]).

%% High Level Api
-export([
    squery/2,
    execute/1
]).


%% Utilities
-export([
    uuid_binary_to_uuid_string/1,
    uuid_string_to_uuid_binary/1,

    hugeint_to_integer/1,
    integer_to_hugeint/1
]).

-include("educkdb.hrl").

-type database() :: reference(). 
-type connection() :: reference().
-type prepared_statement() :: reference().
-type result() :: reference().
-type appender() :: reference().
-type data_chunk() :: reference().


-type sql() :: iodata(). 

-type idx() :: uint64(). % DuckDB index type

-type int8() :: -16#7F..16#7F. % DuckDB tinyint
-type uint8() :: 0..16#FF. % DuckDB duckdb utinyint

-type int16() :: -16#7FFF..16#7FFF. % DuckDB  shortint
-type uint16() :: 0..16#FFFF. % DuckDB ushortint

-type int32() :: -16#7FFFFFFF..16#7FFFFFFF. % DuckDB integer
-type uint32() :: 0..16#FFFFFFFF. % DuckDB uinteger

-type int64() :: -16#7FFFFFFFFFFFFFFF..16#7FFFFFFFFFFFFFFF. % DuckDB bigint
-type uint64() :: 0..16#FFFFFFFFFFFFFFFF.  % DuckDB ubigint

-type hugeint() :: #hugeint{}. % struct, making up a 128 bit signed integer.

-type second() :: float(). %% In the range 0.0..60.0
-type time() :: {calendar:hour(), calendar:minute(), second()}.
-type datetime() :: {calendar:date(), time()}.

-type data() :: boolean()
              | int8() | int16() | int32() | int64() 
              | uint8() | uint16() | uint32() | uint64()
              | float() | hugeint() 
              | calendar:date() | time() | datetime()
              | binary()
              | list(data())
              | #{binary() => data()}
              | {map, list(data()), list(data())}.

-type type_name() :: boolean
                   | tinyint | smallint | integer | bigint 
                   | utinyint | usmallint | uinteger | ubigint
                   | float | double | hugeint
                   | timestamp | date | time
                   | interval 
                   | varchar | blob
                   | decimal
                   | timestamp_s | timestamp_ms | timestamp_ns
                   | enum
                   | list | struct | map
                   | uuid | json.  %% Note: decimal, timestamp_s, timestamp_ms, timestamp_ns and interval's are not supported yet.

-type named_column() :: #{ name := binary(), data := list(data()), type := type_name() }.

-type bind_response() :: ok | {error, _}.
-type append_response() :: ok | {error, _}.

-export_type([database/0, connection/0, prepared_statement/0, result/0, sql/0,
              int8/0, int16/0, int32/0, int64/0,
              uint8/0, uint16/0, uint32/0, uint64/0,
              idx/0, hugeint/0, second/0, time/0, datetime/0
             ]).

-define(SEC_TO_MICS(S), (S * 1000000)).
-define(MIC_TO_SECS(S), (S / 1000000.0)).
-define(MIN_TO_MICS(S), (S * 60000000)).
-define(HOUR_TO_MICS(S), (S * 3600000000)).
-define(EPOCH_OFFSET, 62167219200000000).

-on_load(init/0).

init() ->
    NifName = "educkdb_nif",
    NifFileName = case code:priv_dir(educkdb) of
                      {error, bad_name} -> filename:join("priv", NifName);
                      Dir -> filename:join(Dir, NifName)
                  end,
    ok = erlang:load_nif(NifFileName, 0).

%%
%% Startup & Configure
%%

%% @doc Open, or create a duckdb database with default options.
-spec open(Filename) -> Result
    when Filename :: string(),
         Result :: {ok, database()} | {error, _}.
open(Filename) ->
    open(Filename, #{}).

%% @doc Open, or create a duckdb file
%%
-spec open(Filename, Options) -> Result
    when Filename :: string(),
         Options :: map(),
         Result :: {ok, database()} | {error, _}.
open(_Filename, _Options) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Connect to the database. In the background a thread is started which 
%%      is used by long running commands. Note: It is adviced to use the
%%      connection in a single process.
%%
-spec connect(Database) -> Result
    when Database :: database(),
         Result :: {ok, connection()} | {error, _}.
connect(_Db) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Disconnect from the database. Stops the thread.
%%      The calling pid will receive:
%%      {disconnect, Ref, ok | {error, _}}.
-spec disconnect(Connection) -> Result
    when Connection :: connection(),
         Result :: ok | {error, _}.
disconnect(_Connection) ->
    erlang:nif_error(nif_library_not_loaded).
                                 
%% @doc Close the database. All open connections will become unusable.
-spec close(Database) -> Result
    when Database :: database(),
         Result :: ok | {error, _}.
close(_Db) ->
    erlang:nif_error(nif_library_not_loaded).
 

%% @doc Return a map with config flags, and explanations. The options
%%      can vary depending on the underlying DuckDB version used.
%%      For more information about DuckDB configuration options, see: 
%%      <a href="https://duckdb.org/docs/sql/configuration" target="_parent" rel="noopener">DuckDB Configuration</a>.
-spec config_flag_info() -> map().
config_flag_info() ->
    erlang:nif_error(nif_library_not_loaded).

%%
%% Query
%%

%% @doc Query the database. The answer the answer is returned immediately. 
-spec query(Connection, Sql) -> Result
    when Connection :: connection(),
         Sql :: sql(),
         Result :: {ok, result()} | {error, _}.
query(_Conn, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).

%%
%% Prepared Statements 
%%

%% @doc Compile, and prepare a statement for later execution. 
-spec prepare(connection(), sql()) -> {ok, prepared_statement()} | {error, _}.
prepare(_Conn, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Execute a prepared statement. The answer is returned.
-spec execute_prepared(PreparedStatement) -> Result
    when PreparedStatement :: prepared_statement(),
         Result :: {ok, result()} | {error, _}.
execute_prepared(_PreparedStatement) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind a boolean to the prepared statement at the specified index.
-spec bind_boolean(PreparedStatement, Index, Boolean) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(), 
         Boolean :: boolean(),
         BindResponse :: bind_response().
bind_boolean(Statement, Index, true) ->
    bind_boolean_intern(Statement, Index, 1);
bind_boolean(Statement, Index, false) ->
    bind_boolean_intern(Statement, Index, 0).

-spec bind_boolean_intern(prepared_statement(), idx(), 0..1) -> bind_response().
bind_boolean_intern(_Stmt, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Bind an int8 to the prepared statement at the specified index.
-spec bind_int8(PreparedStatement, Index, Int8) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         Int8 :: int8(),
         BindResponse :: bind_response().
bind_int8(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind an int16 to the prepared statement at the specified index.
-spec bind_int16(PreparedStatement, Index, Int16) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         Int16 :: int16(),
         BindResponse :: bind_response().
bind_int16(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind an int32 to the prepared statement at the specified index.
-spec bind_int32(PreparedStatement, Index, Int32) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         Int32 :: int32(),
         BindResponse :: bind_response().
bind_int32(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind an int64 to the prepared statement at the specified index.
-spec bind_int64(PreparedStatement, Index, Int64) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         Int64 :: int64(),
         BindResponse :: bind_response().
bind_int64(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind an uint8 to the prepared statement at the specified index.
-spec bind_uint8(PreparedStatement, Index, UInt8) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         UInt8 :: uint8(),
         BindResponse :: bind_response().
bind_uint8(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind an uint8 to the prepared statement at the specified index.
-spec bind_uint16(PreparedStatement, Index, UInt16) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         UInt16 :: uint16(),
         BindResponse :: bind_response().
bind_uint16(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind an uint32 to the prepared statement at the specified index.
-spec bind_uint32(PreparedStatement, Index, UInt32) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         UInt32 :: uint32(),
         BindResponse :: bind_response().
bind_uint32(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind an uint64 to the prepared statement at the specified index.
-spec bind_uint64(PreparedStatement, Index, UInt64) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         UInt64 :: uint64(),
         BindResponse :: bind_response().
bind_uint64(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind an float to the prepared statement at the specified index. Note: Erlang's
%%      float() datatype is actually a DuckDB double. When binding an Erlang float
%%      variable you will lose precision.
-spec bind_float(PreparedStatement, Index, Float) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         Float :: float(),
         BindResponse :: bind_response().
bind_float(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind an uint64 to the prepared statement at the specified index. Note: Erlang's
%%      float datatype is a DuckDB double. Using this function allows you to keep the
%%      precision.
-spec bind_double(PreparedStatement, Index, Double) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         Double :: float(),
         BindResponse :: bind_response().
bind_double(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind a date to the prepared statement at the specified index. The date can be 
%%      either given as a calendar:date() tuple, or an integer with the number of 
%%      days since the first of January in the year 0.
-spec bind_date(PreparedStatement, Index, Date) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         Date :: calendar:date() | integer(),
         BindResponse :: bind_response().
bind_date(Stmt, Index, {Y, M, D}=Date) when is_integer(Y) andalso is_integer(M) andalso is_integer(D) ->
    bind_date_intern(Stmt, Index, calendar:date_to_gregorian_days(Date));
bind_date(Stmt, Index, Days) when is_integer(Days) ->
    bind_date_intern(Stmt, Index, Days).

-spec bind_date_intern(prepared_statement(), idx(), integer()) -> bind_response().
bind_date_intern(_Stmt, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind a time to the prepared statement at the specified index. The time can be either
%%      given as an {hour, minute, second} tuple (similar to calendar:time()), or as an integer
%%      with the number of microseconds since midnight.
-spec bind_time(PreparedStatement, Index, Time) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         Time :: calendar:time() | time() | non_neg_integer(),
         BindResponse :: bind_response().
bind_time(Stmt, Index, {H, M, S}) -> 
    bind_time_intern(Stmt, Index, ?HOUR_TO_MICS(H) + ?MIN_TO_MICS(M) + floor(?SEC_TO_MICS(S)));
bind_time(Stmt, Index, Micros) when is_integer(Micros) ->
    bind_time_intern(Stmt, Index, Micros).

bind_time_intern(_Stmt, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Bind a timestamp to the prepared statement at the specified index. The timestamp
%%      can be either a datetime tuple, or an integer with the microseconds since 1-Jan in 
%%      the year 0.
-spec bind_timestamp(PreparedStatement, Index, TimeStamp) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         TimeStamp :: calendar:datetime() | datetime() | integer(),
         BindResponse :: bind_response().
bind_timestamp(Stmt, Index, {MegaSecs, Secs, MicroSecs}) ->
    bind_timestamp_intern(Stmt, Index, ?EPOCH_OFFSET + ?SEC_TO_MICS(MegaSecs * 1000000) + ?SEC_TO_MICS(Secs) + MicroSecs);
bind_timestamp(Stmt, Index, {{_, _, _}=Date, {Hour, Minute, Second}}) -> 
    Mics = ?SEC_TO_MICS(calendar:datetime_to_gregorian_seconds({Date, {Hour, Minute, 0}})),
    RemMics = floor(?SEC_TO_MICS(Second)),
    bind_timestamp_intern(Stmt, Index, Mics + RemMics);
bind_timestamp(Stmt, Index, Micros) when is_integer(Micros) ->
    bind_timestamp_intern(Stmt, Index, Micros).

bind_timestamp_intern(_Stmt, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

% @doc Bind a iolist as varchar to the prepared statement at the specified index. Note: This
%      function is meant to bind null terminated strings in the database. Not arbitrary
%      binary data.
-spec bind_varchar(PreparedStatement, Index, IOData) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         IOData :: iodata(),
         BindResponse :: bind_response().
bind_varchar(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

% @doc Bind a null value to the prepared statement at the specified index.
-spec bind_null(PreparedStatement, Index) -> BindResponse
    when PreparedStatement :: prepared_statement(),
         Index :: idx(),
         BindResponse :: bind_response().
bind_null(_Stmt, _Index) ->
    erlang:nif_error(nif_library_not_loaded).

%%
%% Results
%%

%% @doc Get all data chunks from a query result.
-spec get_chunks(QueryResult) -> DataChunks
    when QueryResult :: result(),
         DataChunks :: list(data_chunk()).
get_chunks(_Result) -> 
    erlang:nif_error(nif_library_not_loaded).
 
%% @doc Fetches a data chunk from a result. The function should be called
%% repeatedly until the result is exhausted.
-spec fetch_chunk(QueryResult) -> DataChunk | '$end'
    when QueryResult :: result(),
         DataChunk :: data_chunk().
fetch_chunk(_Result) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the number of data chunks in a query result. (deprecated)
-spec chunk_count(QueryResult) -> Count
    when QueryResult :: result(),
         Count :: uint64().
chunk_count(_Result) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the column names from the query result.
-spec column_names(QueryResult) -> Names 
    when QueryResult :: result(),
         Names :: list(binary()).
column_names(_Result) -> 
    erlang:nif_error(nif_library_not_loaded).

%%
%% Chunks
%%

%% @doc Return the number of columns in the data chunk.
-spec chunk_column_count(DataChunk) -> ColumnCount
    when DataChunk :: data_chunk(),
         ColumnCount :: uint64().
chunk_column_count(_Chunk) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the column types of the chunk
%% [todo] spec..
chunk_column_types(_Chunk) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Get the columns data of the chunk
%% [todo] spec
chunk_columns(_Chunk) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Return the number of tuples in th data chunk.
-spec chunk_size(DataChunk) -> TupleCount
    when DataChunk :: data_chunk(),
         TupleCount :: uint64().
chunk_size(_Chunk) ->
    erlang:nif_error(nif_library_not_loaded).


%%
%% Appender Interface
%%

%% @doc Create an appender. Appenders allow for high speed bulk insertions into the database. See
%%      <a href="https://duckdb.org/docs/data/appender">DuckDB Appender</a> for more information.
-spec appender_create(Connection, Schema, Table) -> Result 
    when Connection :: connection(),
         Schema :: undefined | binary(),
         Table :: binary,
         Result :: {ok, appender()} | {error, _}.
appender_create(_Connection, _Schema, _Table) -> 
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a boolean value to the current location in the row.
-spec append_boolean(Appender, Boolean) -> AppendResponse
    when Appender :: appender(),
         Boolean :: boolean(),
         AppendResponse :: append_response().
append_boolean(Appender, true) ->
    append_boolean_intern(Appender, 1);
append_boolean(Appender, false) ->
    append_boolean_intern(Appender, 0).

-spec append_boolean_intern(appender(), int32()) -> append_response().
append_boolean_intern(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a tinyint to the current location in the row.
-spec append_int8(Appender, TinyInt) -> AppendResponse
    when Appender :: appender(),
         TinyInt :: int8(),
         AppendResponse :: append_response().
append_int8(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a smallint to the current location in the row.
-spec append_int16(Appender, SmallInt) -> AppendResponse
    when Appender :: appender(),
         SmallInt :: int16(),
         AppendResponse :: append_response().
append_int16(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append an integer (32 bit) to the current location in the row.
-spec append_int32(Appender, Integer) -> AppendResponse
    when Appender :: appender(),
         Integer :: int32(),
         AppendResponse :: append_response().
append_int32(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a bigint to the current location in the row.
-spec append_int64(Appender, BigInt) -> AppendResponse
    when Appender :: appender(),
         BigInt :: int64(),
         AppendResponse :: append_response().
append_int64(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a utinyint to the current location in the row.
-spec append_uint8(Appender, UTinyInt) -> AppendResponse
    when Appender :: appender(),
         UTinyInt :: uint8(),
         AppendResponse :: append_response().
append_uint8(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a usmallint to the current location in the row.
-spec append_uint16(Appender, USmallInt) -> AppendResponse
    when Appender :: appender(),
         USmallInt :: uint16(),
         AppendResponse :: append_response().
append_uint16(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append an unsigned integer (32 bit) to the current location in the row.
-spec append_uint32(Appender, UInteger) -> AppendResponse
    when Appender :: appender(),
         UInteger :: uint32(),
         AppendResponse :: append_response().
append_uint32(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a ubigint to the current location in the row.
-spec append_uint64(Appender, UBigInt) -> AppendResponse
    when Appender :: appender(),
         UBigInt :: uint64(),
         AppendResponse :: append_response().
append_uint64(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a float to the current location in the row. Note: duckdb floats
%%      are different than erlang floats. When you add an erlang float to a 
%%      duckdb float column, you will loose precision.
-spec append_float(Appender, Float) -> AppendResponse
    when Appender :: appender(),
         Float :: float(),
         AppendResponse :: append_response().
append_float(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a double to the current location in the row. Note: duckdb double's 
%%      are the same as erlang floats. 
-spec append_double(Appender, Double) -> AppendResponse
    when Appender :: appender(),
         Double :: float(),
         AppendResponse :: append_response().
append_double(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a time to the current location in the row.
-spec append_time(Appender, Time) -> AppendResponse
    when Appender :: appender(),
         Time :: calendar:time() | time() | non_neg_integer(),
         AppendResponse :: append_response().
append_time(Appender, {H, M, S}) -> 
    append_time_intern(Appender, ?HOUR_TO_MICS(H) + ?MIN_TO_MICS(M) + floor(?SEC_TO_MICS(S)));
append_time(Appender, Micros) when is_integer(Micros) ->
    append_time_intern(Appender, Micros).

append_time_intern(_Appender, _Time) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a data to the current location in the row.
-spec append_date(Appender, Date) -> AppendResponse
    when Appender :: appender(),
         Date :: calendar:date() | integer(),
         AppendResponse :: append_response().
append_date(Appender, {Y, M, D}=Date) when is_integer(Y) andalso is_integer(M) andalso is_integer(D) ->
    append_date_intern(Appender, calendar:date_to_gregorian_days(Date));
append_date(Appender, Days) when is_integer(Days) ->
    append_date_intern(Appender, Days).

append_date_intern(_Appender, _Date) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Append a timestamp to the current location in the row.
-spec append_timestamp(Appender, Timestamp) -> AppendResponse
    when Appender :: appender(),
         Timestamp :: calendar:datetime() | erlang:timestamp() | datetime() | integer(),
         AppendResponse :: append_response().
append_timestamp(Appender, {MegaSecs, Secs, MicroSecs}) ->
    append_timestamp_intern(Appender, ?EPOCH_OFFSET + ?SEC_TO_MICS(MegaSecs * 1000000) + ?SEC_TO_MICS(Secs) + MicroSecs);
append_timestamp(Appender, {{_, _, _}=Date, {Hour, Minute, Second}}) -> 
    Millies = ?SEC_TO_MICS(calendar:datetime_to_gregorian_seconds({Date, {Hour, Minute, 0}})),
    RemMillies = floor(?SEC_TO_MICS(Second)),
    append_timestamp_intern(Appender, Millies + RemMillies);
append_timestamp(Appender, Micros) when is_integer(Micros) ->
    append_timestamp_intern(Appender, Micros).

append_timestamp_intern(_Appender, _Timestamp) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a varchar to the current location in the row.
-spec append_varchar(Appender, IOData) -> AppendResponse
    when Appender :: appender(),
         IOData :: iodata(),
         AppendResponse :: append_response().
append_varchar(_Appender, _IOData) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Append a null value to the current location in the row.
-spec append_null(Appender) -> AppendResponse
    when Appender :: appender(),
         AppendResponse :: append_response().
append_null(_Appender) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Finalize the current row, and start a new one.
-spec appender_end_row(Appender) -> AppendResponse
    when Appender :: appender(),
         AppendResponse :: append_response().
appender_end_row(_Appender) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Flush the appender to the table, forcing the cache of the appender to be cleared
%%      and the data to be appended to the base table.
-spec appender_flush(Appender) -> AppendResult
    when Appender :: appender(),
         AppendResult :: append_response().
appender_flush(_Appender) ->
    erlang:nif_error(nif_library_not_loaded).


%%
%% Higher Level API
%%

result_extract(Result) ->
    case fetch_chunk(Result) of
        '$end' ->
            {ok, [], []};
        Chunk ->
            Names = column_names(Result),
            Types = chunk_column_types(Chunk),
            Info = column_info(Names, Types),
            Rows = chunk_rows(Chunk, Result, []),
            {ok, Info, Rows}
    end.

column_info(Names, Types) ->
    column_info(Names, Types, []).

column_info([], _, Acc) ->
    lists:reverse(Acc);
column_info([N|T], undefined, Acc) ->
    column_info(T, undefined, [#column{name=N}|Acc]);
column_info([Name|RestNames], [Type|RestTypes], Acc) ->
    column_info(RestNames, RestTypes, [#column{name=Name, type=Type}|Acc]).

chunk_rows('$end', _Result, Rows) ->
    lists:reverse(lists:flatten(Rows));
chunk_rows(Chunk, Result, Rows) ->
    R = lists:reverse(
          lists:map(fun list_to_tuple/1,
                    transpose(
                      chunk_columns(Chunk)))),

    chunk_rows(fetch_chunk(Result), Result, [R | Rows]).


%% @doc Do a simple sql query without parameters, and retrieve the result from the
%%      first data chunk.
-spec squery(Connection, Sql) -> Result
    when Connection :: connection(),
         Sql :: sql(),
         Result :: {ok, [ named_column() ]} | {error, _}.
squery(Connection, Sql) ->
    case query(Connection, Sql) of
        {ok, Result} ->
            result_extract(Result);
        {error, _}=E ->
            E
    end.

%% @doc Execute a prepared statement, and retrieve the first result from the first
%%      data chunk.
-spec execute(PreparedStatement) -> Result
    when PreparedStatement :: prepared_statement(),
         Result :: {ok, [ named_column() ]} | {error, _}.
execute(Stmt) ->
    case execute_prepared(Stmt) of
        {ok, Result} ->
            result_extract(Result);
        {error, _}=E ->
            E
    end.

%%
%% Utilities
%% 

transpose([[]|_]) -> [];
transpose(M) ->
    [ lists:map(fun hd/1, M) | transpose(lists:map(fun tl/1, M))].

%% @doc Convert a duckdb hugeint record to erlang integer. 
-spec hugeint_to_integer(Hugeint) -> Integer
    when Hugeint :: hugeint(),
         Integer :: integer().
hugeint_to_integer(#hugeint{upper=Upper, lower=Lower}) ->
    (Upper bsl 64) bor Lower.

%% @doc Convert an erlang integer to a DuckDB hugeint.
%%
%% For more information on DuckDB numeric types: See <a href="https://duckdb.org/docs/sql/data_types/numeric" target="_parent" rel="noopener">DuckDB Numeric Data Types</a>.
-spec integer_to_hugeint(Integer) -> Hugeint
    when Integer :: integer(),
         Hugeint :: hugeint().
integer_to_hugeint(Int) ->
    #hugeint{upper=(Int bsr 64), lower=(Int band 16#FFFFFFFFFFFFFFFF)}.

%% @doc Convert a binary represented UUID to a printable hex representation.
uuid_binary_to_uuid_string(Bin) ->
    Formatted = io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~2.16.0b~2.16.0b-~12.16.0b", uuid_unpack(Bin)),
    erlang:iolist_to_binary(Formatted).

%% @doc Convert a printable UUID to a binary representation.
uuid_string_to_uuid_binary(U)->
    uuid_string_to_uuid_binary1(U, <<>>).

uuid_string_to_uuid_binary1(<<>>, Acc) -> Acc;
uuid_string_to_uuid_binary1(<<$-, Rest/binary>>, Acc) ->
    uuid_string_to_uuid_binary1(Rest, Acc); 
uuid_string_to_uuid_binary1(<<A, B, Rest/binary>>, Acc) ->
    Int = list_to_integer([A,B], 16),
    uuid_string_to_uuid_binary1(Rest, <<Acc/binary, Int>>).

%uuid_pack(TL, TM, THV, CSHR, CSL, N) ->
%  <<UUID:128>> = <<TL:32, TM:16, THV:16, CSHR:8, CSL:8, N:48>>,
%  UUID.

uuid_unpack(<<TL:32, TM:16, THV:16, CSHR:8, CSL:8, N:48>>) ->
  [TL, TM, THV, CSHR, CSL, N].
