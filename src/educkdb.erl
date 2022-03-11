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
%%
%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2022 Maas-Maarten Zeeman
%%
%% @doc Low level erlang API for duckdb databases.

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

    extract_result/1,

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
    append_varchar/2,
    append_null/1,
    appender_flush/1,
    appender_end_row/1
]).

%% High Level Api
-export([
    squery/2
]).

%% low-level api

-export([
    query_cmd/2,
    execute_prepared_cmd/1
]).

-type database() :: reference().
-type connection() :: reference().
-type prepared_statement() :: reference().
-type result() :: reference().
-type appender() :: reference().

-type sql() :: iodata(). 

-type idx() :: 0..16#FFFFFFFFFFFFFFFF. 

-type int8() :: -16#7F..16#7F.
-type uint8() :: 0..16#FF.

-type int16() :: -16#7FFF..16#7FFF.
-type uint16() :: 0..16#FFFF.

-type int32() :: -16#7FFFFFFF..16#7FFFFFFF.
-type uint32() :: 0..16#FFFFFFFF.

-type int64() :: -16#7FFFFFFFFFFFFFFF..16#7FFFFFFFFFFFFFFF.
-type uint64() :: 0..16#FFFFFFFFFFFFFFFF. 

-type bind_response() :: ok | {error, _}.
-type append_response() :: ok | {error, _}.

-export_type([database/0, connection/0, prepared_statement/0, result/0, sql/0,
              int8/0, int16/0, int32/0, int64/0,
              uint8/0, uint16/0, uint32/0, uint64/0,
              idx/0
             ]).

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
%%
open(Filename) ->
    open(Filename, #{}).

%% @doc Open, or create a duckdb file
%%
% -spec open(, map()) -> {ok, database()} | {error, _}.
open(_Filename, _Options) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Connect to the database. In the background a thread is started which 
%%      is used by long running commands. Note: It is adviced to use the
%%      connection in a single process.
%%
-spec connect(database()) -> {ok, connection()} | {error, _}.
connect(_Db) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Disconnect from the database. Stops the thread.
%%      The calling pid will receive:
%%      {disconnect, Ref, ok | {error, _}}.
-spec disconnect(connection()) -> ok | {error, _}.
disconnect(_Connection) ->
    erlang:nif_error(nif_library_not_loaded).
                                 
%% @doc Close the database. All open connections will become unusable.
-spec close(database()) -> ok | {error, _}.
close(_Db) ->
    erlang:nif_error(nif_library_not_loaded).
 

%% @doc Return a list with config flags, and explanation
-spec config_flag_info() -> map().
config_flag_info() ->
    erlang:nif_error(nif_library_not_loaded).
 
%%
%% Query
%%

%% @doc Query the database. The answer the answer is returned immediately. 
%% Special care has been taken to prevent blocking the scheduler. A reference
%% to a result data structure will be returned. 
-spec query(connection(), sql()) -> {ok, result()} | {error, _}.
query(Conn, Sql) ->
    case query_cmd(Conn, Sql) of
        {ok, Ref} ->
            receive 
                {educkdb, Ref, Answer} ->
                    Answer
            end;
        {error, _}=E ->
            E
    end.

%% @doc Query the database. The answer is send back as a result to 
%% the calling process.
-spec query_cmd(connection(), sql()) -> {ok, reference()} | {error, _}.
query_cmd(_Conn, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).

%%
%% Prepared Statements 
%%

%% @doc Compile, and prepare a statement for later execution. 
-spec prepare(connection(), sql()) -> {ok, prepared_statement()} | {error, _}.
prepare(_Conn, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).

execute_prepared(PreparedStatement) ->
    case execute_prepared_cmd(PreparedStatement) of
        {ok, Ref} ->
            receive 
                {educkdb, Ref, Answer} ->
                    Answer
            end;
        {error, _}=E ->
            E
    end.

-spec execute_prepared_cmd(prepared_statement()) -> bind_response().
execute_prepared_cmd(_Stmt) ->
    erlang:nif_error(nif_library_not_loaded).

-spec bind_boolean(prepared_statement(), idx(), boolean()) -> bind_response().
bind_boolean(Statement, Index, true) ->
    bind_boolean_intern(Statement, Index, 1);
bind_boolean(Statement, Index, false) ->
    bind_boolean_intern(Statement, Index, 0).

-spec bind_boolean_intern(prepared_statement(), idx(), 0..1) -> bind_response().
bind_boolean_intern(_Stmt, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

-spec bind_int8(prepared_statement(), idx(), int8()) -> bind_response().
bind_int8(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

-spec bind_int16(prepared_statement(), idx(), int16()) -> bind_response().
bind_int16(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

-spec bind_int32(prepared_statement(), idx(), int32()) -> bind_response().
bind_int32(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

-spec bind_int64(prepared_statement(), idx(), int64()) -> bind_response().
bind_int64(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

-spec bind_uint8(prepared_statement(), idx(), uint8()) -> bind_response().
bind_uint8(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

-spec bind_uint16(prepared_statement(), idx(), uint16()) -> bind_response().
bind_uint16(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

-spec bind_uint32(prepared_statement(), idx(), uint32()) -> bind_response().
bind_uint32(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

-spec bind_uint64(prepared_statement(), idx(), uint64()) -> bind_response().
bind_uint64(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

-spec bind_float(prepared_statement(), idx(), float()) -> bind_response().
bind_float(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

-spec bind_double(prepared_statement(), idx(), float()) -> bind_response().
bind_double(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

bind_date(Stmt, Index, {Y, M, D}=Date) when is_integer(Y) andalso is_integer(M) andalso is_integer(D) ->
    bind_date_intern(Stmt, Index, calendar:date_to_gregorian_days(Date));
bind_date(Stmt, Index, Days) when is_integer(Days) ->
    bind_date_intern(Stmt, Index, Days).

bind_date_intern(_Stmt, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc 
bind_time(Stmt, Index, Micros) when is_integer(Micros) ->
    bind_time_intern(Stmt, Index, Micros).

bind_time_intern(_Stmt, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc 
bind_timestamp(Stmt, Index, Micros) when is_integer(Micros) ->
    bind_timestamp_intern(Stmt, Index, Micros).

bind_timestamp_intern(_Stmt, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

% @doc Bind a iolist as varchar. 
% Note: be really carefull, value must be valid utf8 data.
-spec bind_varchar(prepared_statement(), idx(), iodata()) -> bind_response().
bind_varchar(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

-spec bind_null(prepared_statement(), idx()) -> bind_response().
bind_null(_Stmt, _Index) ->
    erlang:nif_error(nif_library_not_loaded).


%%
%% Results
%%

%% @doc Extract the values from the result.
-spec extract_result(result()) -> {ok, [], []}.
extract_result(_Result) -> 
    erlang:nif_error(nif_library_not_loaded).
    

%%
%% Appender Interface
%%

-spec appender_create(connection(), string(), string()) -> {ok, appender()} | {error, _}. 
appender_create(_Connection, _Schema, _Table) -> 
    erlang:nif_error(nif_library_not_loaded).

-spec append_boolean(appender(), boolean()) -> append_response().
append_boolean(Appender, true) ->
    append_boolean_intern(Appender, 1);
append_boolean(Appender, false) ->
    append_boolean_intern(Appender, 0).


-spec append_boolean_intern(appender(), int32()) -> append_response().
append_boolean_intern(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

-spec append_int8(appender(), int8()) -> append_response().
append_int8(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

-spec append_int16(appender(), int16()) -> append_response().
append_int16(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

-spec append_int32(appender(), int32()) -> append_response().
append_int32(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

-spec append_int64(appender(), integer()) -> append_response().
append_int64(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

-spec append_uint8(appender(), uint8()) -> append_response().
append_uint8(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

-spec append_uint16(appender(), uint16()) -> append_response().
append_uint16(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

-spec append_uint32(appender(), uint32()) -> append_response().
append_uint32(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

-spec append_uint64(appender(), uint64()) -> append_response().
append_uint64(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

-spec append_float(appender(), float()) -> append_response().
append_float(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

-spec append_double(appender(), float()) -> append_response().
append_double(_Appender, _Integer) ->
    erlang:nif_error(nif_library_not_loaded).

-spec append_varchar(appender(), iodata()) -> append_response().
append_varchar(_Appender, _IOData) ->
    erlang:nif_error(nif_library_not_loaded).


-spec append_null(appender()) -> append_response().
append_null(_Appender) ->
    erlang:nif_error(nif_library_not_loaded).

-spec appender_flush(appender()) -> append_response().
appender_flush(_Appender) ->
    erlang:nif_error(nif_library_not_loaded).

-spec appender_end_row(appender()) -> append_response().
appender_end_row(_Appender) ->
    erlang:nif_error(nif_library_not_loaded).

%%
%% Higher Level API
%%

%% @doc Do a simple sql query without parameters.
squery(Connection, Sql) ->
    case query(Connection, Sql) of
        {ok, Result} ->
            extract_result(Result);
        {error, _}=E ->
            E
    end.


