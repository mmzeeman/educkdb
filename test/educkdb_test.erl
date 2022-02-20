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
%% @doc Test suite for educkdb
%%

-module(educkdb_test).

-include_lib("eunit/include/eunit.hrl").

-define(DB1, "./test/dbs/temp_db1.db").
-define(DB2, "./test/dbs/temp_db2.db").


-define(INT8_MIN, -127).
-define(INT8_MAX,  127).
-define(UINT8_MAX, 255).

-define(INT16_MIN, -32767).
-define(INT16_MAX,  32767).
-define(UINT16_MAX, 65535).

-define(INT32_MIN, -2147483647).
-define(INT32_MAX,  2147483647).
-define(UINT32_MAX, 4294967295).

-define(INT64_MIN,  -9223372036854775807).
-define(INT64_MAX,   9223372036854775807).
-define(UINT64_MAX, 18446744073709551615).

educk_db_version_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    ?assertEqual({ok,
                  [{column, <<"library_version">>, varchar}, {column, <<"source_id">>, varchar}],
                  [[<<"0.3.1">>, <<"88aa81c6b">>]]},
                 q(Conn, <<"PRAGMA version;">>)),

    ok = educkdb:disconnect(Conn),
    ok = educkdb:close(Db),
    ok.

open_single_database_test() ->
    {ok, Db1} = educkdb:open(":memory:"),
    ok = educkdb:close(Db1),

    {ok, Db2} = educkdb:open(":memory:", #{threads => "2"}),
    ok = educkdb:close(Db2),

    ok.

open_and_connect_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, _Conn} = educkdb:connect(Db),
    ok = educkdb:close(Db),
    ok.

open_connect_and_disconnect_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    ok = educkdb:disconnect(Conn),
    ok = educkdb:close(Db),
    ok.

open_options_test() ->
    %% [note] newer duckdb versions can have new config flags.
    InfoMap = #{access_mode => 
                    <<"Access mode of the database ([AUTOMATIC], READ_ONLY or READ_WRITE)">>,
                default_null_order =>
                    <<"Null ordering used when none is specified ([NULLS_FIRST] or NULLS_LAST)">>,
                default_order =>
                    <<"The order type used when none is specified ([ASC] or DESC)">>,
                enable_external_access =>
                    <<"Allow the database to access external state (through e.g. COPY TO/FROM, CSV readers, pandas replacement scans, etc)">>,
                enable_object_cache =>
                    <<"Whether or not object cache is used to cache e.g. Parquet metadata">>,
                max_memory =>
                    <<"The maximum memory of the system (e.g. 1GB)">>,
                threads =>
                    <<"The number of total threads used by the system">>},
    ?assertEqual(InfoMap, educkdb:config_flag_info()),

    {ok, Db1} = educkdb:open(":memory:", #{threads => "2"}),
    ok = educkdb:close(Db1),

    %% Check if error reporting works
    {error, {open, "IO Error: The file is not a valid DuckDB database file!"}}
        = educkdb:open("README.md", #{ access_mode => "READ_ONLY" }),

    {error,{open,"IO Error: Cannot open file \".\": Is a directory"}}
        = educkdb:open(".", #{ access_mode => "READ_ONLY" }),

    ok.

query_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {error, no_iodata} = educkdb:query(Conn, 1000),
    {error, {result, "Parser Error: Table must have at least one column!"}} = educkdb:query(Conn, <<"create table test()"/utf8>>),

    {ok, Res1} = educkdb:query(Conn, "create table test(id integer, x varchar(20));"),
    {ok, Res2} = educkdb:query(Conn, "insert into test values(10, '10');"),
    {ok, Res3} = educkdb:query(Conn, "insert into test values(20, '20');"),
    {ok, _Res4} = educkdb:query(Conn, "insert into test values(null, 'null');"),
    {ok, Res5} = educkdb:query(Conn, "select * from test order by id;"),

    ?assertEqual({ok, [], []}, educkdb:extract_result(Res1)),
    ?assertEqual({ok, [{column, <<"Count">>, bigint}], [[1]]}, educkdb:extract_result(Res2)),
    ?assertEqual({ok, [{column, <<"Count">>, bigint}], [[1]]}, educkdb:extract_result(Res3)),
    ?assertEqual({ok,
                  [{column, <<"id">>, integer}, {column, <<"x">>, varchar}],
                  [[null, <<"null">>], [10, <<"10">>], [20, <<"20">>]]}, educkdb:extract_result(Res5)),

    ok = educkdb:disconnect(Conn),
    ok = educkdb:close(Db),
    ok.

prepare_error_test() ->
    Query = "this is a syntax error;",
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {error, {prepare, _}} = educkdb:prepare(Conn, Query),
    ok.

prepare_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = q(Conn, "create table test(id integer, value varchar(20));"),
    Query = "select * from test;",
    {ok, P} = educkdb:prepare(Conn, Query),

    {ok, [], []} = x(P),

    educkdb:disconnect(Conn),
    educkdb:close(Db),

    ok.

bind_int_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, [], []} = q(Conn, "create table test(a TINYINT, b SMALLINT, c INTEGER, d BIGINT);"),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2, $3, $4);"),
    {ok, [], []} = q(Conn, "select * from test;"),

    ok = educkdb:bind_int8(Insert, 1, 0),
    ok = educkdb:bind_int16(Insert, 2, 0),
    ok = educkdb:bind_int32(Insert, 3, 0),
    ok = educkdb:bind_int64(Insert, 4, 0),

    {ok, _, [[1]]} = x(Insert),

    ok = educkdb:bind_int8(Insert, 1, 3),
    ok = educkdb:bind_int16(Insert, 2, 3),
    ok = educkdb:bind_int32(Insert, 3, 3),
    ok = educkdb:bind_int64(Insert, 4, 3),

    {ok, _, [[1]]} = x(Insert),

    ok = educkdb:bind_int8(Insert, 1, -3),
    ok = educkdb:bind_int16(Insert, 2, -3),
    ok = educkdb:bind_int32(Insert, 3, -3),
    ok = educkdb:bind_int64(Insert, 4, -3),

    {ok, _, [[1]]} = x(Insert),

    ok = educkdb:bind_int8(Insert, 1, ?INT8_MAX),
    ok = educkdb:bind_int16(Insert, 2, ?INT16_MAX),
    ok = educkdb:bind_int32(Insert, 3, ?INT32_MAX),
    ok = educkdb:bind_int64(Insert, 4, ?INT64_MAX),

    {ok, _, [[1]]} = x(Insert),

    ok = educkdb:bind_int8(Insert, 1, ?INT8_MIN),
    ok = educkdb:bind_int16(Insert, 2, ?INT16_MIN),
    ok = educkdb:bind_int32(Insert, 3, ?INT32_MIN),
    ok = educkdb:bind_int64(Insert, 4, ?INT64_MIN),

    {ok, _, [[1]]} = x(Insert),

    {ok, _, [
             [?INT8_MIN, ?INT16_MIN, ?INT32_MIN, ?INT64_MIN],
             [-3,-3,-3,-3],
             [0,0,0,0],
             [3,3,3,3],
             [?INT8_MAX, ?INT16_MAX, ?INT32_MAX, ?INT64_MAX]
            ]} = q(Conn, "select * from test order by a"),

    ok.

bind_unsigned_int_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, [], []} = q(Conn, "create table test(a UTINYINT, b USMALLINT, c UINTEGER, d UBIGINT);"),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2, $3, $4);"),

    ok = educkdb:bind_uint8(Insert,  1, 0),
    ok = educkdb:bind_uint16(Insert, 2, 0),
    ok = educkdb:bind_uint32(Insert, 3, 0),
    ok = educkdb:bind_uint64(Insert, 4, 0),

    {ok, _, [[1]]} = x(Insert),

    ok = educkdb:bind_uint8(Insert,  1,  ?INT8_MAX),
    ok = educkdb:bind_uint16(Insert, 2, ?INT16_MAX),
    ok = educkdb:bind_uint32(Insert, 3, ?INT32_MAX),
    ok = educkdb:bind_uint64(Insert, 4, ?INT64_MAX),
    
    {ok, _, [[1]]} = x(Insert),

    ok = educkdb:bind_uint8(Insert,  1,  ?UINT8_MAX),
    ok = educkdb:bind_uint16(Insert, 2, ?UINT16_MAX),
    ok = educkdb:bind_uint32(Insert, 3, ?UINT32_MAX),
    ok = educkdb:bind_uint64(Insert, 4, ?UINT64_MAX),

    {ok, _, [[1]]} = x(Insert),

    {ok, _, [
             [0,0,0,0],
             [ ?INT8_MAX,  ?INT16_MAX,  ?INT32_MAX,  ?INT64_MAX],
             [?UINT8_MAX, ?UINT16_MAX, ?UINT32_MAX, ?UINT64_MAX]
            ]} = q(Conn, "select * from test order by a"),
    ok.

bind_float_and_double_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, [], []} = q(Conn, "create table test(a REAL, b DOUBLE);"),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2);"),

    ok = educkdb:bind_float(Insert, 1, 0.0),
    ok = educkdb:bind_double(Insert, 2, 0.0),

    {ok, _, [[1]]} = x(Insert),

    ok = educkdb:bind_float(Insert, 1,  200000000000000000000000.0),
    ok = educkdb:bind_double(Insert, 2, 200000000000000000000000.0),

    {ok, _, [[1]]} = x(Insert),

    {ok, _, [
             [0.0, 0.0],
             [1.9999999556392617e23, 2.0e23]
            ]} = q(Conn, "select * from test order by a"),

    ok.

bind_null_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, [], []} = q(Conn, "create table test(a REAL, b DOUBLE);"),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2);"),

    ok = educkdb:bind_null(Insert,  1),
    ok = educkdb:bind_null(Insert,  2),

    {ok, _, [[1]]} = x(Insert),

    {ok, _, [
             [null, null]
            ]} = q(Conn, "select * from test order by a"),

    ok.

bind_boolean_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, [], []} = q(Conn, "create table test(a boolean, b boolean);"),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2);"),

    ok = educkdb:bind_boolean(Insert, 1, true),
    ok = educkdb:bind_boolean(Insert, 2, false),

    {ok, _, [[1]]} = x(Insert),

    {ok, _, [
             [true, false]
            ]} = q(Conn, "select * from test order by a"),

    ok.

    
bind_varchar_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, [], []} = q(Conn, "create table test(a varchar(10), b varchar(200));"),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2);"),

    ok = educkdb:bind_varchar(Insert, 1, "hello"),
    ok = educkdb:bind_varchar(Insert, 2, "world"),

    {ok, _, [[1]]} = x(Insert),

    ok = educkdb:bind_varchar(Insert, 1, <<"😀"/utf8>>),
    ok = educkdb:bind_varchar(Insert, 2, <<"1234567890">>),

     {ok, _, [[1]]} = x(Insert),

    {ok, [ {column,<<"a">>,varchar},
           {column,<<"b">>,varchar}
         ],
     [ [<<"hello">>, <<"world">>],
       [<<"😀"/utf8>>, <<"1234567890">>]
     ]
    } = q(Conn, "select * from test order by a"),
    ok.

appender_create_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {error, {appender, "Catalog Error: Table \"main.test\" could not be found"}} = educkdb:appender_create(Conn, undefined, <<"test">>),
    {error, {appender, "Catalog Error: Table \"x.test\" could not be found"}} = educkdb:appender_create(Conn, <<"x">>, <<"test">>),

    {ok, [], []} = q(Conn, "create table test(a varchar(10), b integer);"),

    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    Appender,

    ok.

appender_end_row_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = q(Conn, "create table test(a varchar(10), b integer);"),

    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    {error, {appender, "Invalid Input Error: Call to EndRow before all rows have been appended to!"}}
        =  educkdb:appender_end_row(Appender),

    ok.

appender_append_int64_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = q(Conn, "create table test(a bigint, b bigint);"),

    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok = educkdb:append_int64(Appender, 1),
    ok = educkdb:append_int64(Appender, 2),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_int64(Appender, 3),
    ok = educkdb:append_int64(Appender, 4),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_int64(Appender, ?INT64_MIN),
    ok = educkdb:append_int64(Appender, ?INT64_MAX),
    ok = educkdb:appender_end_row(Appender),
    ok = educkdb:appender_flush(Appender),
 
    {ok,[{column, <<"a">>, bigint},{column, <<"b">>, bigint}],
     [[?INT64_MIN, ?INT64_MAX], [1,2],[3,4]]} = q(Conn, "select * from test order by a;"),

    ok.


appender_append_int32_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = q(Conn, "create table test(a integer, b integer);"),

    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok = educkdb:append_int32(Appender, 1),
    ok = educkdb:append_int32(Appender, 2),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_int32(Appender, 3),
    ok = educkdb:append_int32(Appender, 4),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_int32(Appender, ?INT32_MIN),
    ok = educkdb:append_int32(Appender, ?INT32_MAX),
    ok = educkdb:appender_end_row(Appender),
    ok = educkdb:appender_flush(Appender),
 
    {ok,[{column, <<"a">>, integer},{column, <<"b">>, integer}],
     [[?INT32_MIN, ?INT32_MAX], [1,2],[3,4]]} = q(Conn, "select * from test order by a;"),

    ok.

appender_append_int16_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = q(Conn, "create table test(a smallint, b smallint);"),

    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok = educkdb:append_int16(Appender, 1),
    ok = educkdb:append_int16(Appender, 2),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_int16(Appender, 3),
    ok = educkdb:append_int16(Appender, 4),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_int16(Appender, ?INT16_MIN),
    ok = educkdb:append_int16(Appender, ?INT16_MAX),
    ok = educkdb:appender_end_row(Appender),
    ok = educkdb:appender_flush(Appender),
 
    {ok,[{column, <<"a">>, smallint},{column, <<"b">>, smallint}],
     [[?INT16_MIN, ?INT16_MAX], [1,2], [3,4]]} = q(Conn, "select * from test order by a;"),

    ok.

appender_append_int8_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = q(Conn, "create table test(a tinyint, b tinyint);"),

    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok = educkdb:append_int8(Appender, 1),
    ok = educkdb:append_int8(Appender, 2),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_int8(Appender, 3),
    ok = educkdb:append_int8(Appender, 4),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_int8(Appender, ?INT8_MIN),
    ok = educkdb:append_int8(Appender, ?INT8_MAX),
    ok = educkdb:appender_end_row(Appender),
    ok = educkdb:appender_flush(Appender),
 
    {ok,[{column, <<"a">>, tinyint},{column, <<"b">>, tinyint}],
     [[?INT8_MIN, ?INT8_MAX], [1,2], [3,4]]} = q(Conn, "select * from test order by a;"),

    ok.

appender_append_uint64_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = q(Conn, "create table test(a ubigint, b ubigint);"),

    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok = educkdb:append_uint64(Appender, 1),
    ok = educkdb:append_uint64(Appender, 2),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_uint64(Appender, 3),
    ok = educkdb:append_uint64(Appender, 4),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_uint64(Appender, 0),
    ok = educkdb:append_uint64(Appender, ?UINT64_MAX),
    ok = educkdb:appender_end_row(Appender),
    ok = educkdb:appender_flush(Appender),
 
    {ok,[{column, <<"a">>, ubigint},{column, <<"b">>, ubigint}],
     [[0, ?UINT64_MAX], [1,2],[3,4]]} = q(Conn, "select * from test order by a;"),

    ok.

appender_append_uint32_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = q(Conn, "create table test(a uinteger, b uinteger);"),

    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok = educkdb:append_uint32(Appender, 1),
    ok = educkdb:append_uint32(Appender, 2),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_uint32(Appender, 3),
    ok = educkdb:append_uint32(Appender, 4),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_uint32(Appender, 0),
    ok = educkdb:append_uint32(Appender, ?UINT32_MAX),
    ok = educkdb:appender_end_row(Appender),
    ok = educkdb:appender_flush(Appender),
 
    {ok,[{column, <<"a">>, uinteger}, {column, <<"b">>, uinteger}],
     [[0, ?UINT32_MAX], [1,2],[3,4]]} = q(Conn, "select * from test order by a;"),

    ok.

appender_append_uint16_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = q(Conn, "create table test(a usmallint, b usmallint);"),

    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok = educkdb:append_uint16(Appender, 1),
    ok = educkdb:append_uint16(Appender, 2),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_uint16(Appender, 3),
    ok = educkdb:append_uint16(Appender, 4),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_uint16(Appender, 0),
    ok = educkdb:append_uint16(Appender, ?UINT16_MAX),
    ok = educkdb:appender_end_row(Appender),
    ok = educkdb:appender_flush(Appender),
 
    {ok,[{column, <<"a">>, usmallint}, {column, <<"b">>, usmallint}],
     [[0, ?UINT16_MAX], [1,2],[3,4]]} = q(Conn, "select * from test order by a;"),
    ok.

appender_append_uint8_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = q(Conn, "create table test(a utinyint, b utinyint);"),

    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok = educkdb:append_uint8(Appender, 1),
    ok = educkdb:append_uint8(Appender, 2),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_uint8(Appender, 3),
    ok = educkdb:append_uint8(Appender, 4),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_uint8(Appender, 0),
    ok = educkdb:append_uint8(Appender, ?UINT8_MAX),
    ok = educkdb:appender_end_row(Appender),
    ok = educkdb:appender_flush(Appender),
 
    {ok,[{column, <<"a">>, utinyint}, {column, <<"b">>, utinyint}],
     [[0, ?UINT8_MAX], [1,2],[3,4]]} = q(Conn, "select * from test order by a;"),
    ok.

appender_append_boolean_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = q(Conn, "create table test(a boolean, b boolean);"),

    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok = educkdb:append_boolean(Appender, true),
    ok = educkdb:append_boolean(Appender, false),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_boolean(Appender, false),
    ok = educkdb:append_boolean(Appender, false),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_boolean(Appender, true),
    ok = educkdb:append_boolean(Appender, true),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:appender_flush(Appender),
 
    {ok,[{column, <<"a">>, boolean},
         {column, <<"b">>, boolean}],
     [[false, false],
      [true,  false],
      [true,  true]]} = q(Conn, "select * from test order by a;"),

    ok.

yielding_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = q(Conn, "create table test(a integer, b varchar, c varchar);"),

    %% Insert a lot of records to ensure yielding takes place records 
    Values = lists:seq(1, 100000),
    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),
    {ok, [], []} = q(Conn, "begin;"),
    lists:foreach(fun(V) ->
                          ok = educkdb:append_int32(Appender, V),
                          ok = educkdb:append_varchar(Appender, "this is a test 123"),
                          ok = educkdb:append_varchar(Appender, "and this too"),
                          ok = educkdb:appender_end_row(Appender)
                  end,
                  Values),
    educkdb:appender_flush(Appender),
    {ok, [], []} = q(Conn, "commit;"),

    ?assertEqual({ok, [{column,<<"count">>,bigint}], [[length(Values)]]}, q(Conn, "select count(*) as count from test;")),

    {ok, _, Rows} = q(Conn, "select a from test order by a;"),
    ?assertEqual(length(Values), length(Rows)),
    RowValues = lists:flatten(Rows),
    Values = RowValues,

    {ok, _, _} = q(Conn, "select * from test order by a;"),

    ok.



garbage_collect_test() ->
    F = fun() ->
                {ok, Db} = educkdb:open(":memory:"),
                {ok, Conn} = educkdb:connect(Db),

                {ok, Res1} = educkdb:query(Conn, "create table test(id integer, x varchar(20));"),
                {ok, Res2} = educkdb:query(Conn, "insert into test values(10, '10');"),
                {ok, Res3} = educkdb:query(Conn, "insert into test values(20, '20');"),
                {ok, Res4} = educkdb:query(Conn, "select * from test;"),

                ?assertEqual({ok, [], []}, educkdb:extract_result(Res1)),
                ?assertEqual({ok, [{column, <<"Count">>, bigint}], [[1]]}, educkdb:extract_result(Res2)),
                ?assertEqual({ok, [{column, <<"Count">>, bigint}], [[1]]}, educkdb:extract_result(Res3)),
                ?assertEqual({ok, [{column, <<"id">>, integer}, {column, <<"x">>, varchar}],
                              [ [10, <<"10">>],
                                [20, <<"20">>]
                              ]}, educkdb:extract_result(Res4)),

                ok = educkdb:disconnect(Conn),
                ok = educkdb:close(Db)
        end,

    [spawn(F) || _X <- lists:seq(0,30)],
    receive after 500 -> ok end,
    erlang:garbage_collect(),

    [spawn(F) || _X <- lists:seq(0,30)],
    receive after 500 -> ok end,
    erlang:garbage_collect(),

    ok.


%%
%% Helpers
%%

%cleanup() ->
%    rm_rf(?DB1),
%    rm_rf(?DB2).
%
%rm_rf(Filename) ->
%    case file:delete(Filename) of
%        ok -> ok;
%        {error, _} -> ok
%    end.

q(Conn, Query) ->
    case educkdb:query(Conn, Query) of
        {ok, Result} -> educkdb:extract_result(Result);
        {error, _}=E -> E
    end.

x(Stmt) ->
    case educkdb:execute_prepared(Stmt) of
        {ok, Result} -> educkdb:extract_result(Result);
        {error, _}=E -> E
    end.

