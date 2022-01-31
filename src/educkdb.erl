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

    extract_result/1
]).

%% low-level api

-export([
    query_cmd/2
]).

-type raw_database() :: reference().
-type raw_connection() :: reference().
-type raw_statement() :: reference().
-type raw_result() :: reference().
-type sql() :: iodata(). 

-export_type([raw_connection/0, raw_statement/0, sql/0]).

-on_load(init/0).

init() ->
    NifName = "educkdb_nif",
    NifFileName = case code:priv_dir(esqlite) of
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
% -spec open(, map()) -> {ok, raw_database()} | {error, _}.
open(_Filename, _Options) ->
    erlang:nif_error(nif_library_not_loaded).

%% @doc Connect to the database. In the background a thread is started which 
%%      is used by long running commands. Note: It is adviced to use the
%%      connection in a single process.
%%
-spec connect(raw_database()) -> {ok, raw_connection()} | {error, _}.
connect(_Db) ->
    erlang:nif_error(nif_library_not_loaded).


%% @doc Disconnect from the database. Stops the thread.
%%      The calling pid will receive:
%%      {disconnect, Ref, ok | {error, _}}.
-spec disconnect(raw_connection()) -> ok | {error, _}.
disconnect(_Connection) ->
    erlang:nif_error(nif_library_not_loaded).
                                 
%% @doc Close the database. All open connections will become unusable.
-spec close(raw_database()) -> ok | {error, _}.
close(_Db) ->
    erlang:nif_error(nif_library_not_loaded).
 

%%
%% Query
%%

%% @doc Query the database. The answer the answer is returned immediately. 
%% Special care has been taken to prevent blocking the scheduler. A reference
%% to a result data structure will be returned. 
-spec query(raw_connection(), sql()) -> {ok, raw_result()} | {error, _}.
query(Conn, Sql) ->
    case query_cmd(Conn, Sql) of
        {ok, Ref} ->
            receive 
                {educkdb, Ref, Answer} -> Answer
            end;
        {error, _}=E ->
            E
    end.

%% @doc Query the database. The answer is send back as a result to 
%% the calling process.
-spec query_cmd(raw_connection(), sql()) -> {ok, reference()} | {error, _}.
query_cmd(_Conn, _Sql) ->
    erlang:nif_error(nif_library_not_loaded).

%%
%% Results
%%

%% @doc Extract the values from the result.
extract_result(_Result) -> 
    erlang:nif_error(nif_library_not_loaded).
    

