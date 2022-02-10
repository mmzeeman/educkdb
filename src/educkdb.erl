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
    bind_varchar/3,
    bind_null/2,

    extract_result/1,

    appender_create/3
]).

%% low-level api

-export([
    query_cmd/2
]).

-type database() :: reference().
-type connection() :: reference().
-type prepared_statement() :: reference().
-type result() :: reference().
-type appender() :: reference().
-type sql() :: iodata(). 

-export_type([database/0, connection/0, prepared_statement/0, result/0, sql/0]).

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

execute_prepared_cmd(_Stmt) ->
    erlang:nif_error(nif_library_not_loaded).

bind_boolean(Statement, Index, true) ->
    bind_boolean_intern(Statement, Index, 1);
bind_boolean(Statement, Index, false) ->
    bind_boolean_intern(Statement, Index, 0).

bind_boolean_intern(_Stmt, _Index, _Value) ->
    erlang:nif_error(nif_library_not_loaded).

bind_int8(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

bind_int16(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

bind_int32(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

bind_int64(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

bind_uint8(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

bind_uint16(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

bind_uint32(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

bind_uint64(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

bind_float(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

bind_double(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

% @doc Bind a iolist as varchar. 
% Note: be really carefull, value must be valid utf8 data.
bind_varchar(_Stmt, _Index, _Value) -> 
    erlang:nif_error(nif_library_not_loaded).

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
