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
%% @copyright 2022-2023 Maas-Maarten Zeeman
%%
%% @doc Test suite for educkdb
%%

-module(educkdb_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("educkdb/include/educkdb.hrl").

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

    %?assertEqual({ok,[#{data => [<<"v1.1.3">>],
    %                    name => <<"library_version">>,
    %                    type => varchar},
    %                  #{data => [<<"19864453f7">>],
    %                    name => <<"source_id">>,
    %                    type => varchar}]},
    %             educkdb:squery(Conn, <<"PRAGMA version;">>)),

    ?assertEqual({ok,[{column, <<"library_version">>, varchar},
                      {column, <<"source_id">>, varchar}
                     ], [{<<"v1.1.3">>, <<"19864453f7">>}]},
                 educkdb:squery(Conn, <<"PRAGMA version;">>)),

    ok = educkdb:disconnect(Conn),
    ok = educkdb:close(Db),
    ok.

open_single_database_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    ok = educkdb:close(Db),
    ok.

open_with_threads_option_test() ->
    {ok, Db} = educkdb:open(":memory:", #{
                                          max_memory => "2GB",
                                          threads => "4"
                                         }),
    ok = educkdb:close(Db),
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
    ?assert(is_map(educkdb:config_flag_info())),

    {ok, Db1} = educkdb:open(":memory:", #{threads => "2"}),
    ok = educkdb:close(Db1),

    %% Check if error reporting works
    {error, {open, "IO Error: The file \"README.md\" exists, but it is not a valid DuckDB database file!"}}
        = educkdb:open("README.md", #{ access_mode => "READ_ONLY" }),

    %% Check opening directory, this used to crash v0.7.0
    {error,{open,"IO Error: Could not read from file \".\": Is a directory"}}
        = educkdb:open(".", #{ access_mode => "READ_ONLY" }),

    ok.


query_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    ?assertError(badarg, educkdb:query(Conn, 1000)),
    {error, {result, "Parser Error: Table must have at least one column!"}} = educkdb:query(Conn, <<"create table test()"/utf8>>),

    {ok, Res1} = educkdb:query(Conn, "create table test(id integer, x varchar(20));"),
    {ok, Res2} = educkdb:query(Conn, "insert into test values(10, '10');"),
    {ok, Res3} = educkdb:query(Conn, "insert into test values(20, '20');"),
    {ok, _Res4} = educkdb:query(Conn, "insert into test values(null, 'null');"),
    {ok, Res5} = educkdb:query(Conn, "select * from test order by id;"),

    ?assertEqual([], educkdb:extract_result(Res1)),
    ?assertEqual([#{ name => <<"Count">>, type => bigint, data => [1]}],
                 educkdb:extract_result(Res2)),
    ?assertEqual([#{ name => <<"Count">>, type => bigint, data => [1]}],
                 educkdb:extract_result(Res3)),
    ?assertEqual([#{ name => <<"id">>, type => integer, data => [10, 20, null]},
                  #{ name => <<"x">>, type => varchar, data => [<<"10">>, <<"20">>, <<"null">>]}],
                 educkdb:extract_result(Res5)),

    ok = educkdb:disconnect(Conn),
    ok = educkdb:close(Db),
    ok.

column_names_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, Res1} = educkdb:query(Conn, "create table test(a integer, b varchar(20));"),
    ?assertEqual([<<"Count">>], educkdb:column_names(Res1)),

    {ok, Res2} = educkdb:query(Conn, "insert into test values (1, 'a'), (2, 'b') ;"),
    ?assertEqual([<<"Count">>], educkdb:column_names(Res2)),

    {ok, Res3} = educkdb:query(Conn, "select * from test;"),
    ?assertEqual([<<"a">>, <<"b">>], educkdb:column_names(Res3)),

    ok.

chunk_count_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, Res} = educkdb:query(Conn, "create table test(a integer);"),
    0 = educkdb:chunk_count(Res),

    {ok, Res1} = educkdb:query(Conn, "insert into test values (1), (2), (3);"),
    1 = educkdb:chunk_count(Res1),

    {ok, Res2} = educkdb:query(Conn, "select * from test;"),
    1 = educkdb:chunk_count(Res2),

    ok.

chunk_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, Res} = educkdb:query(Conn, "create table test(a integer);"),
    0 = educkdb:chunk_count(Res),

    {ok, Res1} = educkdb:query(Conn, "insert into test values (1), (2), (3);"),
    [Chunk1] = educkdb:get_chunks(Res1),
    ?assert(is_reference(Chunk1)),
    ?assertEqual(1, educkdb:chunk_column_count(Chunk1)),
    ?assertEqual(1, educkdb:chunk_size(Chunk1)),
    ?assertEqual([bigint], educkdb:chunk_column_types(Chunk1)),
    ?assertEqual([[3]], educkdb:chunk_columns(Chunk1)),


    {ok, Res2} = educkdb:query(Conn, "select * from test;"),
    [Chunk2] = educkdb:get_chunks(Res2),
    ?assert(is_reference(Chunk2)),
    ?assertEqual(1, educkdb:chunk_column_count(Chunk2)),
    ?assertEqual(3, educkdb:chunk_size(Chunk2)),
    ?assertEqual([integer], educkdb:chunk_column_types(Chunk2)),
    ?assertEqual([[1,2,3]], educkdb:chunk_columns(Chunk2)),

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
    {ok, _, _} = educkdb:squery(Conn, "create table test(id integer, value varchar(20));"),
    Query = "select * from test;",
    {ok, P} = educkdb:prepare(Conn, Query),

    {ok, []} = x(P),

    educkdb:disconnect(Conn),
    educkdb:close(Db),

    ok.

bind_int_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    ?assertEqual({ok, [], []}, educkdb:squery(Conn, "create table test(a TINYINT, b SMALLINT, c INTEGER, d BIGINT);")),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2, $3, $4);"),
    ?assertEqual({ok, [], []}, educkdb:squery(Conn, "select * from test;")),

    ok = educkdb:bind_int8(Insert, 1, 0),
    ok = educkdb:bind_int16(Insert, 2, 0),
    ok = educkdb:bind_int32(Insert, 3, 0),
    ok = educkdb:bind_int64(Insert, 4, 0),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ok = educkdb:bind_int8(Insert, 1, 3),
    ok = educkdb:bind_int16(Insert, 2, 3),
    ok = educkdb:bind_int32(Insert, 3, 3),
    ok = educkdb:bind_int64(Insert, 4, 3),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ok = educkdb:bind_int8(Insert, 1, -3),
    ok = educkdb:bind_int16(Insert, 2, -3),
    ok = educkdb:bind_int32(Insert, 3, -3),
    ok = educkdb:bind_int64(Insert, 4, -3),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ok = educkdb:bind_int8(Insert, 1, ?INT8_MAX),
    ok = educkdb:bind_int16(Insert, 2, ?INT16_MAX),
    ok = educkdb:bind_int32(Insert, 3, ?INT32_MAX),
    ok = educkdb:bind_int64(Insert, 4, ?INT64_MAX),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ok = educkdb:bind_int8(Insert, 1, ?INT8_MIN),
    ok = educkdb:bind_int16(Insert, 2, ?INT16_MIN),
    ok = educkdb:bind_int32(Insert, 3, ?INT32_MIN),
    ok = educkdb:bind_int64(Insert, 4, ?INT64_MIN),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ?assertEqual({ok,
                  [ #column{name = <<"a">>, type=tinyint},
                    #column{name = <<"b">>, type=smallint},
                    #column{name = <<"c">>, type=integer},
                    #column{name = <<"d">>, type=bigint} ],
                  [ {?INT8_MIN, ?INT16_MIN, ?INT32_MIN, ?INT64_MIN},
                    {-3,- 3, -3, -3},
                    { 0,  0,  0,  0},
                    { 3,  3,  3,  3},
                    {?INT8_MAX, ?INT16_MAX, ?INT32_MAX, ?INT64_MAX} ]},
                 educkdb:squery(Conn, "select * from test order by a")), 
    ok.

bind_unsigned_int_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, [], []} = educkdb:squery(Conn, "create table test(a UTINYINT, b USMALLINT, c UINTEGER, d UBIGINT);"),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2, $3, $4);"),

    ok = educkdb:bind_uint8(Insert,  1, 0),
    ok = educkdb:bind_uint16(Insert, 2, 0),
    ok = educkdb:bind_uint32(Insert, 3, 0),
    ok = educkdb:bind_uint64(Insert, 4, 0),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ok = educkdb:bind_uint8(Insert,  1,  ?INT8_MAX),
    ok = educkdb:bind_uint16(Insert, 2, ?INT16_MAX),
    ok = educkdb:bind_uint32(Insert, 3, ?INT32_MAX),
    ok = educkdb:bind_uint64(Insert, 4, ?INT64_MAX),
    
    {ok, [ #{ data := [1] } ]} = x(Insert),

    ok = educkdb:bind_uint8(Insert,  1,  ?UINT8_MAX),
    ok = educkdb:bind_uint16(Insert, 2, ?UINT16_MAX),
    ok = educkdb:bind_uint32(Insert, 3, ?UINT32_MAX),
    ok = educkdb:bind_uint64(Insert, 4, ?UINT64_MAX),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ?assertEqual({ok, [#column{ name = <<"a">>, type = utinyint},
                       #column{ name = <<"b">>, type = usmallint},
                       #column{ name = <<"c">>, type = uinteger},
                       #column{ name = <<"d">>, type = ubigint} ],
                  [{0, 0, 0, 0},
                   {?INT8_MAX, ?INT16_MAX, ?INT32_MAX, ?INT64_MAX},
                   {?UINT8_MAX, ?UINT16_MAX, ?UINT32_MAX , ?UINT64_MAX} ]},
                 educkdb:squery(Conn, "select * from test order by a")),

    ok.

bind_float_and_double_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, [], []} = educkdb:squery(Conn, "create table test(a REAL, b DOUBLE);"),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2);"),

    ok = educkdb:bind_float(Insert, 1, 0.0),
    ok = educkdb:bind_double(Insert, 2, 0.0),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ok = educkdb:bind_float(Insert, 1,  200000000000000000000000.0),
    ok = educkdb:bind_double(Insert, 2, 200000000000000000000000.0),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ?assertEqual(
       {ok, [#column{ name = <<"a">>, type = float},
             #column{ name = <<"b">>, type = double}],
        [{0.0, 0.0},
         {1.9999999556392617e23, 2.0e23}]},
       educkdb:squery(Conn, "select * from test order by a")),

    ok.

bind_date_and_time_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, [], []} = educkdb:squery(Conn, "create table test(a date, b time);"),

    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2);"),

    ok = educkdb:bind_date(Insert, 1,  {1970, 8, 11}),
    ok = educkdb:bind_time(Insert, 2,  1000000), %% raw, in microseconds

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ok = educkdb:bind_date(Insert, 1,  {1970, 1, 1}),
    ok = educkdb:bind_time(Insert, 2,  0),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ok = educkdb:bind_date(Insert, 1,  {2022, 12, 25}),
    ok = educkdb:bind_time(Insert, 2,  {8, 12, 10.1234}),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    Expected = {ok, [ #column{ name = <<"a">>, type = date},
                      #column{ name = <<"b">>, type = time}],
                [{{1970,1,1},{0,0,0.0}},
                 {{1970,8,11},{0,0,1.0}},
                 {{2022,12,25},{8,12,10.1234}}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a")),

    ok.

extract_timestamp_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, [], []} = educkdb:squery(Conn, "create table test(a timestamp);"),

    {ok, _, [{1}]} = educkdb:squery(Conn, "INSERT INTO test VALUES ('0-01-01');"),
    {ok, _,  [{1}]} = educkdb:squery(Conn, "INSERT INTO test VALUES ('1970-01-01');"),
    {ok, _, [{1}]} = educkdb:squery(Conn, "INSERT INTO test VALUES ('2003-12-25');"),
    {ok, _, [{1}]} = educkdb:squery(Conn, "INSERT INTO test VALUES ('2023-4-3 11:23:16.123456');"),

    ?assertEqual(
       {ok, [#column{name = <<"a">>, type = timestamp}],
        [ { {{   0,  1,  1}, {0, 0, 0.0}} },
          { {{1970,  1,  1}, {0, 0, 0.0}} },
          { {{2003, 12, 25}, {0, 0, 0.0}} },
          { {{2023,  4,  3}, {11, 23, 16.123456}} } ] },
       educkdb:squery(Conn, "select * from test order by a")),

    ok.

bind_timestamp_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, _, _} = educkdb:squery(Conn, "create table test(a timestamp);"),

    %%
    %% Test bind
    %%

    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1);"),

    %% Plain milliseconds since 0, 1, 1
    ok = educkdb:bind_timestamp(Insert, 1, 0), 
    {ok, [ #{ data := [1] } ]} = x(Insert),

    %% Plain erlang date-time tuple
    ok = educkdb:bind_timestamp(Insert, 1, {{1970, 8, 11}, {8,0,0}}),
    {ok, [ #{ data := [1] } ]} = x(Insert),

    %% Erlang timestamp
    ok = educkdb:bind_timestamp(Insert, 1, {1647, 729383, 983105}),
    {ok, [ #{ data := [1] } ]} = x(Insert),

    %% Results are returned in fp
    ?assertEqual(
       {ok, [#column{ name = <<"a">>, type = timestamp }],
            [{{{   0, 1,  1}, { 0,  0,  0.0}}},
             {{{1970, 8, 11}, { 8,  0,  0.0}}},
             {{{2022, 3, 19}, {22, 36, 23.983105}}} ]},
       educkdb:squery(Conn, "select * from test order by a")),

    ok.

bind_null_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, [], []} = educkdb:squery(Conn, "create table test(a REAL, b DOUBLE);"),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2);"),

    ok = educkdb:bind_null(Insert,  1),
    ok = educkdb:bind_null(Insert,  2),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ?assertEqual({ok, [#column{ name = <<"a">>, type = float},
                       #column{ name = <<"b">>, type = double }],
                  [{null, null}]},
                 educkdb:squery(Conn, "select * from test order by a")),

    ok.

bind_boolean_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, _, _} = educkdb:squery(Conn, "create table test(a boolean, b boolean);"),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2);"),

    ok = educkdb:bind_boolean(Insert, 1, true),
    ok = educkdb:bind_boolean(Insert, 2, false),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    Expected = {ok, [ #column{ name = <<"a">>, type = boolean},
                      #column{ name = <<"b">>, type = boolean}],
                [{true,false}]},

    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a")),

    ok.
    
bind_varchar_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, _, _} = educkdb:squery(Conn, "create table test(a varchar(10), b varchar(200));"),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2);"),

    ok = educkdb:bind_varchar(Insert, 1, "hello"),
    ok = educkdb:bind_varchar(Insert, 2, "world"),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    ok = educkdb:bind_varchar(Insert, 1, <<"😀"/utf8>>),
    ok = educkdb:bind_varchar(Insert, 2, <<"1234567890">>),

    {ok, [ #{ data := [1] } ]} = x(Insert),

    Expected = {ok,[ #column{ name = <<"a">>, type = varchar},#column{ name = <<"b">>, type = varchar}],
                [{<<"hello">>,<<"world">>}, {<<240,159,152,128>>,<<"1234567890">>}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a")),

    ok.

appender_create_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    ?assertEqual({error, {appender, "Table \"main.test\" could not be found"}},
                 educkdb:appender_create(Conn, undefined, <<"test">>)),
    ?assertEqual({error, {appender, "Table \"x.test\" could not be found"}},
                 educkdb:appender_create(Conn, <<"x">>, <<"test">>)),

    {ok, _, _} = educkdb:squery(Conn, "create table test(a varchar(10), b integer);"),

    {ok, _Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok.

appender_end_row_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, _, _} = educkdb:squery(Conn, "create table test(a varchar(10), b integer);"),

    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ?assertEqual(
       {error, {appender, "Call to EndRow before all columns have been appended to!"}},
       educkdb:appender_end_row(Appender)),

    ok.

appender_append_int64_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, _, _} = educkdb:squery(Conn, "create table test(a bigint, b bigint);"),

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
 
    Expected = {ok,[#column{ name = <<"a">>, type = bigint},
                    #column{ name = <<"b">>, type = bigint}],
                [{?INT64_MIN, ?INT64_MAX},{1,2},{3,4}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a;")),

    ok.

appender_append_int32_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, _, _} = educkdb:squery(Conn, "create table test(a integer, b integer);"),

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

    Expected = {ok,[#column{ name = <<"a">>, type = integer},
                    #column{ name = <<"b">>, type = integer}],
                [{?INT32_MIN, ?INT32_MAX},{1,2},{3,4}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a;")),

    ok.

appender_append_int16_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, _, _} = educkdb:squery(Conn, "create table test(a smallint, b smallint);"),

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
 
    Expected = {ok,[#column{ name = <<"a">>, type = smallint},
                    #column{ name = <<"b">>, type = smallint}],
                [{?INT16_MIN, ?INT16_MAX},{1,2},{3,4}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a;")),

    ok.

appender_append_int8_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = educkdb:squery(Conn, "create table test(a tinyint, b tinyint);"),

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

    Expected = {ok,[#column{ name = <<"a">>, type = tinyint},
                    #column{ name = <<"b">>, type = tinyint}],
                [{?INT8_MIN, ?INT8_MAX},{1,2},{3,4}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a;")),

    ok.

appender_append_uint64_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = educkdb:squery(Conn, "create table test(a ubigint, b ubigint);"),

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

    Expected = {ok,[#column{ name = <<"a">>, type = ubigint},
                    #column{ name = <<"b">>, type = ubigint}],
                [{0, ?UINT64_MAX},{1,2},{3,4}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a;")),

    ok.

appender_append_uint32_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = educkdb:squery(Conn, "create table test(a uinteger, b uinteger);"),

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
 
    Expected = {ok,[#column{ name = <<"a">>, type = uinteger},
                    #column{ name = <<"b">>, type = uinteger}],
                [{0, ?UINT32_MAX},{1,2},{3,4}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a;")),

    ok.

appender_append_uint16_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = educkdb:squery(Conn, "create table test(a usmallint, b usmallint);"),

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

    Expected = {ok,[#column{ name = <<"a">>, type = usmallint},
                    #column{ name = <<"b">>, type = usmallint}],
                [{0, ?UINT16_MAX},{1,2},{3,4}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a;")),

    ok.

appender_append_uint8_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, [], []} = educkdb:squery(Conn, "create table test(a utinyint, b utinyint);"),

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

    Expected = {ok,[#column{ name = <<"a">>, type = utinyint},
                    #column{ name = <<"b">>, type = utinyint}],
                [{0, ?UINT8_MAX},{1,2},{3,4}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a;")),

    ok.

appender_append_boolean_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, _, _} = educkdb:squery(Conn, "create table test(a boolean, b boolean);"),

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
    
    Expected = {ok,[#column{ name = <<"a">>, type = boolean},#column{ name = <<"b">>, type = boolean}],
                [{false,false},{true,false},{true,true}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a;")),

    ok.

appender_append_time_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, _, _} = educkdb:squery(Conn, "create table test(a time);"),
    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok = educkdb:append_time(Appender, 0),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_time(Appender, {10, 10, 10}),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_time(Appender, {23, 50, 55.123456}),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:appender_flush(Appender),

    Expected = {ok,[#column{ name = <<"a">>, type = time}],
                [{{0,0,0.0}},{{10,10,10.0}},{{23,50,55.123456}}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a;")),

    ok.

appender_append_date_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, _, _} = educkdb:squery(Conn, "create table test(a date);"),
    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok = educkdb:append_date(Appender, 0),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_date(Appender, {1901, 10, 10}),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_date(Appender, {2032, 4, 29}),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:appender_flush(Appender),

    Expected = {ok,[#column{ name = <<"a">>, type = date}],
                [{{0,1,1}},{{1901,10,10}},{{2032,4,29}}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a;")),

    ok.

appender_append_timestamp_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, _, _} = educkdb:squery(Conn, "create table test(a timestamp);"),
    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    ok = educkdb:append_timestamp(Appender, 0),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_timestamp(Appender, {{1901, 10, 10}, {10, 15, 0}}),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_timestamp(Appender, {{2032, 4, 29}, {23, 59, 59}}),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:append_timestamp(Appender, {1647, 729383, 983105}),
    ok = educkdb:appender_end_row(Appender),

    ok = educkdb:appender_flush(Appender),

    Expected = {ok,[#column{ name = <<"a">>, type = timestamp}],
                [{{{0,1,1},{0,0,0.0}}},
                 {{{1901,10,10},{10,15,0.0}}},
                 {{{2022,3,19},{22,36,23.983105}}},
                 {{{2032,4,29},{23,59,59.0}}}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select * from test order by a;")),

    ok.

yielding_test() ->
    %% This was done with a yielding nif before, now duckdb already chunks up
    %% big returns. 
    
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    {ok, _, _} = educkdb:squery(Conn, "create table test(a integer, b varchar, c varchar);"),

    %% Insert a lot of records to ensure yielding takes place records 
    Values = lists:seq(1, 100000),
    {ok, Appender} = educkdb:appender_create(Conn, undefined, <<"test">>),

    {ok, _, _} = educkdb:squery(Conn, "begin;"),

    lists:foreach(fun(V) ->
                          ok = educkdb:append_int32(Appender, V),
                          ok = educkdb:append_varchar(Appender, "this is a test 123"),
                          ok = educkdb:append_varchar(Appender, "and this too"),
                          ok = educkdb:appender_end_row(Appender)
                  end,
                  Values),
    educkdb:appender_flush(Appender),
    {ok, _, _} = educkdb:squery(Conn, "commit;"),

    Expected = {ok, [#column{ name = <<"count">>, type = bigint}], [ {length(Values)}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "select count(*) as count from test;")),

    {ok, Res} = educkdb:query(Conn, "select a from test order by a;"),
    Chunks = [ begin
                   [Col] = educkdb:extract_chunk(C),
                   maps:get(data, Col)
               end || C <- educkdb:get_chunks(Res) ],

    Col = lists:flatten(Chunks),
    ?assertEqual(length(Values), length(Col)),
    Values = Col,

    ok.

current_schema_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    %% FYI, the result must be unnested first.
    Expected = {ok,[#column{ name = <<"current_schemas">>, type = varchar}],
                [{<<"main">>}, {<<"main">>}, {<<"main">>}, {<<"pg_catalog">>}]},
    ?assertEqual(Expected, educkdb:squery(Conn, "SELECT UNNEST(current_schemas(true)) as current_schemas;")),
    ok.


garbage_collect_test() ->
    F = fun() ->
                {ok, Db} = educkdb:open(":memory:"),
                {ok, Conn} = educkdb:connect(Db),

                {ok, Res1} = educkdb:query(Conn, "create table test(id integer, x varchar(20));"),
                {ok, Res2} = educkdb:query(Conn, "insert into test values(10, '10');"),
                {ok, Res3} = educkdb:query(Conn, "insert into test values(20, '20');"),
                {ok, Res4} = educkdb:query(Conn, "select * from test;"),

                ?assertEqual([], educkdb:extract_result(Res1)),
                ?assertEqual([#{ name => <<"Count">>, type => bigint, data => [1]}],
                             educkdb:extract_result(Res2)),
                ?assertEqual([#{ name => <<"Count">>, type => bigint, data => [1]}],
                             educkdb:extract_result(Res3)),
                ?assertEqual([#{ name => <<"id">>, type => integer, data => [10, 20]},
                              #{ name => <<"x">>, type => varchar, data => [<<"10">>, <<"20">>]}],
                             educkdb:extract_result(Res4)),

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

%multi_chunk_test() ->
%    {ok, Db} = educkdb:open(":memory:"),
%    {ok, Conn} = educkdb:connect(Db),
%
%    {ok, R1} = educkdb:squery(Conn, "select range(0, 10000000, 1)"),
%
%
%    ?assertEqual(2, length(maps:get(rows, R1))),
%    
%
%    ok.


extract_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, R1} = educkdb:query(Conn, "create table test(a integer);"),
    0 = educkdb:chunk_count(R1),

    {ok, R2} = educkdb:query(Conn, "insert into test values (10), (11), (12);"),
    C2 = educkdb:get_chunk(R2, 0),
    ?assertEqual( [ #{ type => bigint, data => [3] } ], educkdb:extract_chunk(C2)),
    ?assertEqual( [ bigint ], educkdb:chunk_column_types(C2)),
    ?assertEqual( [ [3] ], educkdb:chunk_columns(C2)),

    {ok, R3} = educkdb:query(Conn, "select * from test order by a;"),
    C3 = educkdb:get_chunk(R3, 0),
    ?assertEqual( [ #{ type => integer, data => [10, 11, 12] } ],
       educkdb:extract_chunk(C3)),

    ok.

fetch_chunk_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, R1} = educkdb:query(Conn, "create table test(a integer);"),
    '$end' = educkdb:fetch_chunk(R1),

    {ok, R2} = educkdb:query(Conn, "insert into test values (10), (11), (12);"),
    C2 = educkdb:fetch_chunk(R2),
    ?assertEqual( [ #{ type => bigint, data => [3] } ], educkdb:extract_chunk(C2)),
    '$end' = educkdb:fetch_chunk(R2),

    {ok, R3} = educkdb:query(Conn, "select * from test order by a;"),
    C3 = educkdb:fetch_chunk(R3),
    ?assertEqual( [ #{ type => integer, data => [10, 11, 12] } ],
       educkdb:extract_chunk(C3)),
    '$end' = educkdb:fetch_chunk(R3),

    ok.

boolean_extract_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, R1} = educkdb:query(Conn, "create table test(a boolean, b boolean);"),
    0 = educkdb:chunk_count(R1),

    {ok, R2} = educkdb:query(Conn, "insert into test values (null, true), (false, null), (true, true), (false, false), (true, false);"),
    Expected2 = {ok,[{column,<<"Count">>,bigint}],[{5}]},
    ?assertEqual(Expected2, educkdb:result_extract(R2)),

    {ok, R3} = educkdb:query(Conn, "select * from test order by a;"),
    ?assertEqual(
       [ #{ name => <<"a">>, 
            type => boolean,
            data => [false, false, true, true, null] },
         #{ name => <<"b">>,
            type => boolean,
            data => [null, false, true, false, true] }
       ],
       educkdb:extract_result(R3)),

    ok.

signed_extract_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, R1} = educkdb:query(Conn, "create table test(a smallint, b tinyint, c integer, d bigint);"),
    0 = educkdb:chunk_count(R1),

    {ok, R2} = educkdb:query(Conn, "insert into test values (-10, -10, -10, -10), (11, 11, 11, 11), (12, 12, 12, 12);"),
    C2 = educkdb:get_chunk(R2, 0),
    ?assertEqual(
       [ #{ type => bigint, data => [3] } ],
       educkdb:extract_chunk(C2)),

    {ok, R3} = educkdb:query(Conn, "select * from test order by a;"),
    C3 = educkdb:get_chunk(R3, 0),
    ?assertEqual(
       [ #{ type => smallint,
            data => [-10, 11, 12] },

         #{ type => tinyint,
            data => [-10, 11, 12] },

         #{ type => integer,
            data => [-10, 11, 12] },

         #{ type => bigint,
            data => [-10, 11, 12] }

       ],
       educkdb:extract_chunk(C3)),

    ok.

unsigned_extract_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, R1} = educkdb:query(Conn, "create table test(a usmallint, b utinyint, c uinteger, d ubigint);"),
    0 = educkdb:chunk_count(R1),

    {ok, R2} = educkdb:query(Conn, "insert into test values (10, 10, 10, 10), (11, 11, 11, 11), (12, 12, 12, 12);"),
    C2 = educkdb:get_chunk(R2, 0),
    ?assertEqual(
       [ #{ type => bigint,
                 data => [3] }
       ],
       educkdb:extract_chunk(C2)),

    {ok, R3} = educkdb:query(Conn, "select * from test order by a;"),
    C3 = educkdb:get_chunk(R3, 0),
    ?assertEqual(
       [ #{ type => usmallint,
            data => [10, 11, 12] },

         #{ type => utinyint,
            data => [10, 11, 12] },

         #{ type => uinteger,
            data => [10, 11, 12] },

         #{ type => ubigint,
            data => [10, 11, 12] }

       ],
       educkdb:extract_chunk(C3)),

    ok.

float_and_double_extract2_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, R1} = educkdb:query(Conn, "create table test(a float, b double);"),
    0 = educkdb:chunk_count(R1),

    {ok, R2} = educkdb:query(Conn, "insert into test values (1.0, 10.1), (2.0, 11.1), (3.0, 12.2);"),
    C2 = educkdb:get_chunk(R2, 0),
    ?assertEqual( [ #{ type => bigint, data => [3] } ], educkdb:extract_chunk(C2)),

    {ok, R3} = educkdb:query(Conn, "select * from test order by a;"),
    C3 = educkdb:get_chunk(R3, 0),
    ?assertEqual(
       [ #{ type => float,
            data => [1.0, 2.0, 3.0] },

         #{ type => double,
            data => [10.1, 11.1, 12.2] }

       ],
       educkdb:extract_chunk(C3)),

    ok.

varchar_extract_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, R1} = educkdb:query(Conn, "create table test(a varchar, b varchar);"),
    0 = educkdb:chunk_count(R1),

    {ok, R2} = educkdb:query(Conn, "insert into test values ('1', '2'), ('3', '4'), ('', ''), ('012345678901', '012345678901234567890');"),
    C2 = educkdb:get_chunk(R2, 0),
    ?assertEqual( [ #{ type => bigint, data => [4] } ], educkdb:extract_chunk(C2)),

    {ok, R3} = educkdb:query(Conn, "select * from test order by a;"),
    C3 = educkdb:get_chunk(R3, 0),
    ?assertEqual(
       [ #{ type => varchar, data => [<<"">>, <<"012345678901">>, <<"1">>, <<"3">> ] },
              #{ type => varchar, data => [<<"">>, <<"012345678901234567890">>, <<"2">>, <<"4">> ] }
       ],
       educkdb:extract_chunk(C3)),

    ok.

hugeint_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, _, _} = educkdb:squery(Conn, "create table test(a hugeint);"),

    A = educkdb:integer_to_hugeint(-170141183460469231731687303715884105727),
    B = educkdb:integer_to_hugeint(-1111),
    C = educkdb:integer_to_hugeint(-1),
    D = educkdb:integer_to_hugeint(0),
    E = educkdb:integer_to_hugeint(1),
    F = educkdb:integer_to_hugeint(1111),

    ?assertEqual({hugeint, -9223372036854775808, 1}, A), 
    ?assertEqual({hugeint, -1, 18446744073709550505}, B), 
    ?assertEqual({hugeint, -1, 18446744073709551615}, C), 
    ?assertEqual({hugeint, 0, 0}, D), 
    ?assertEqual({hugeint, 0, 1}, E), 
    ?assertEqual({hugeint, 0, 1111}, F), 


    ?assertMatch({ok, [ #{data := [{hugeint, -9223372036854775808, 1}] } ]},
                 educkdb:squery(Conn, "SELECT -170141183460469231731687303715884105727::hugeint")),

    ?assertMatch({ok, [ #{data := [{hugeint, -9223372036854775808, 1}] } ]},
                 educkdb:squery(Conn, "SELECT * FROM (values (-170141183460469231731687303715884105727::hugeint))")),

    {ok, _, _} = educkdb:squery(Conn, "insert into test values(-170141183460469231731687303715884105727::hugeint)"),
    {ok, _, _} = educkdb:squery(Conn, "insert into test values(-1111::hugeint)"),
    {ok, _, _} = educkdb:squery(Conn, "insert into test values(-1::hugeint)"),
    {ok, _, _} = educkdb:squery(Conn, "insert into test values(0::hugeint)"),
    {ok, _, _} = educkdb:squery(Conn, "insert into test values(1::hugeint)"),
    {ok, _, _} = educkdb:squery(Conn, "insert into test values(1111::hugeint)"),

    {ok, [ #{ data := [ A, B, C, D, E, F ] }
         ]} = educkdb:squery(Conn, "select * from test order by a"),

    ok.

uuid_test() ->
    UUID1 = <<"00112233-4455-6677-8899-aabbccddeeff">>,
    UUID2 = <<"550e8400-e29b-41d4-a716-446655440000">>,
    UUID3 = <<"ffffffff-ffff-ffff-ffff-ffffffffffff">>,

    BinUUID = educkdb:uuid_string_to_uuid_binary(UUID1),
    ?assertEqual(UUID1, educkdb:uuid_binary_to_uuid_string(BinUUID)),

    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, _, _} = educkdb:squery(Conn, "create table test(a uuid);"),
    {ok, _, _} = educkdb:squery(Conn, "insert into test values('00112233-4455-6677-8899-aabbccddeeff')"),
    {ok, _, _} = educkdb:squery(Conn, "insert into test values('550e8400-e29b-41d4-a716-446655440000')"),
    {ok, _, _} = educkdb:squery(Conn, "insert into test values('ffffffff-ffff-ffff-ffff-ffffffffffff')"),

    {ok, [#{ data := DuckBinUUIDs }]} = educkdb:squery(Conn, "select * from test order by a"),

    ?assertEqual([ educkdb:uuid_string_to_uuid_binary(D) || D <- [UUID1, UUID2, UUID3]], DuckBinUUIDs),

    ok.

blob_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, _, _} = educkdb:squery(Conn, "create table test(a blob);"),

    {ok, [#{ data := [ <<>> ] }]} = educkdb:squery(Conn, "select ''::blob"),
    {ok, [#{ data := [ <<"1234">> ] }]} = educkdb:squery(Conn, "select '1234'::blob"),
    {ok, [#{ data := [ <<"abcdefghijklmnopqrstuvwxyz">> ] }]} = educkdb:squery(Conn, "select 'abcdefghijklmnopqrstuvwxyz'::blob"),

    {ok, _, _} = educkdb:squery(Conn, "insert into test values('00'::blob)"),
    {ok, _, _} = educkdb:squery(Conn, "insert into test values(''::blob)"),
    {ok, _, _} = educkdb:squery(Conn, "insert into test values('1234'::blob)"),

    ?assertMatch({ok, [#{ data := [0, 2, 4] }]}, educkdb:squery(Conn, "select octet_length(a) from test order by a;")),
    {ok, [#{ data := Blobs }]} = educkdb:squery(Conn, "select * from test order by a;"),

    ?assertEqual([<<>>, <<"00">>, <<"1234">>], Blobs),

    ok.

uhugeint_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),
    ?assertMatch({ok, [#{ data := [ { no_extract, uhugeint } ]}]}, educkdb:squery(Conn, "SELECT 1::uhugeint;")),
    ok.

enum_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, _, _} = educkdb:squery(Conn, "CREATE TYPE rainbow AS ENUM ('red', 'orange', 'yellow', 'green', 'blue', 'purple');"),

    {ok, [#{ data := [ <<"orange">> ], type := enum }]} = educkdb:squery(Conn, "select 'orange'::rainbow"),

    ?assertEqual({ok, [#{ data => [ <<"red">>, null, <<"orange">>, <<"yellow">>, <<"green">> ], name => <<"b">>, type => enum }]},
                 educkdb:squery(Conn, "select a::rainbow as b from (values ('red'::rainbow), (null), ('orange'::rainbow), ('yellow'::rainbow), ('green'::rainbow)) color(a) ")),

    ok.

%%parquet_install_test() ->
%%    {ok, Db} = educkdb:open(":memory:"),
%%    {ok, Conn} = educkdb:connect(Db),
%%    {ok, []}  = educkdb:squery(Conn, "INSTALL parquet"),
%%    ok.

%% https_install_test() ->
%%     {ok, Db} = educkdb:open(":memory:"),
%%    {ok, Conn} = educkdb:connect(Db),
%%     {ok, []} = educkdb:squery(Conn, "INSTALL sqlite"),
%%     {ok, []} = educkdb:squery(Conn, "LOAD sqlite"),
%%     ok.


list_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    %% Simple lists
    ?assertMatch({ok, [#{ data := [ [1,2,3,4] ]}]},
                 educkdb:squery(Conn, "SELECT [1, 2, 3, 4];")),
    ?assertMatch({ok, [#{ data := [ [1,2, null, 4] ]}]},
                 educkdb:squery(Conn, "SELECT [1, 2, null, 4];")),
    ?assertMatch({ok, [#{ data := [ [<<"one">>, <<"two">>, null, <<"three">>] ]}]},
                 educkdb:squery(Conn, "SELECT ['one', 'two', null, 'three'];")),

    %% Nested lists
    ?assertMatch({ok, [#{ data := [ [ [10, 20], [1,2,3, 4], [1] ] ]}]},
                 educkdb:squery(Conn, "SELECT [ [10, 20], [1,2,3,4], [1] ];")),

    %% With null values
    ?assertMatch({ok, [#{ data := [ [ [10, 20], null, [1] ] ]}]},
                 educkdb:squery(Conn, "SELECT [ [10, 20], null, [1] ];")),

    %% With null values
    ?assertMatch({ok, [#{ data := [ [ [[10, 20]], [null], [[1]] ] ]}]},
                 educkdb:squery(Conn, "SELECT [ [ [10, 20] ], [null], [[1]] ];")),

    ok.

struct_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    ?assertMatch({ok, [#{ data := [[ #{}, #{} ]]}] },
                 educkdb:squery(Conn, "SELECT [{'x': 1, 'y': 2, 'z': 3},
                                               {'x': 100, 'y': 200, 'z': 300} ];")),

    ?assertMatch({ok, [ #{ data :=  [ #{} ] }]},
                 educkdb:squery(Conn, "SELECT {'x': 1, 'y': 2, 'z': 3};")),

    ok.

struct_table_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, _, _} = educkdb:squery(Conn, "create table test(a row(i integer, j integer));"),

    ?assertMatch({ok, []}, educkdb:squery(Conn, "SELECT * from test;")),

    {ok, _, _} = educkdb:squery(Conn, "insert into test values (null);"),

    ?assertMatch({ok, [#{ data := [null],
                          name := <<"a">>,
                          type := struct }]},
                 educkdb:squery(Conn, "SELECT * from test;")),

    {ok, _, _} = educkdb:squery(Conn, "insert into test values ({i: 10, j: 20});"),
    {ok, _, _} = educkdb:squery(Conn, "insert into test values ({i: 123, j: 456});"),

    ?assertMatch({ok, [#{ data := [#{<<"i">> := 10, <<"j">> := 20}, #{ <<"i">> := 123, <<"j">> := 456}, null],
                          name := <<"a">>,
                          type := struct }]},
                 educkdb:squery(Conn, "SELECT * from test order by a.i;")),


    ok.

map_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    %%?assertMatch({ok, [#{ data := [ #map{ keys = [1, 5, 2, 12],
    %%                                      values = [<<"a">>, <<"e">>, <<"b">>, <<"c">>]
    %%                                    }
    %%                              ]
    %%                    }]
    %%             },
    %%             educkdb:squery(Conn, "SELECT map([1, 5, 2, 12], ['a', 'e', 'b', 'c']);")),

    ?assertMatch({ok, [#{ data := [ { no_extract, map }] }] },
                 educkdb:squery(Conn, "SELECT map([1, 5, 2, 12], ['a', 'e', 'b', 'c']);")),


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

x(Stmt) ->
    case educkdb:execute_prepared(Stmt) of
        {ok, Result} ->
            {ok, educkdb:extract_result(Result)};
        {error, _}=E ->
            E
    end.

