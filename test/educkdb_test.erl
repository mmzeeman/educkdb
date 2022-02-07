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

open_single_database_test() ->
    {ok, Db} = educkdb:open(":memory:"),
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

    ok = educkdb:bind_int8(Insert, 1, 127),
    ok = educkdb:bind_int16(Insert, 2, 32767),
    ok = educkdb:bind_int32(Insert, 3, 2147483647),
    ok = educkdb:bind_int64(Insert, 4, 9223372036854775807),

    {ok, _, [[1]]} = x(Insert),

    ok = educkdb:bind_int8(Insert, 1, -127),
    ok = educkdb:bind_int16(Insert, 2, -32767),
    ok = educkdb:bind_int32(Insert, 3, -2147483647),
    ok = educkdb:bind_int64(Insert, 4, -9223372036854775807),

    {ok, _, [[1]]} = x(Insert),

    {ok, _, [
             [-127, -32767, -2147483647, -9223372036854775807],
             [-3,-3,-3,-3],
             [0,0,0,0],
             [3,3,3,3],
             [127, 32767, 2147483647, 9223372036854775807]
            ]} = q(Conn, "select * from test order by a"),

    ok.

bind_unsigned_int_test() ->
    {ok, Db} = educkdb:open(":memory:"),
    {ok, Conn} = educkdb:connect(Db),

    {ok, [], []} = q(Conn, "create table test(a UTINYINT, b USMALLINT, c UINTEGER, d UBIGINT);"),
    {ok, Insert} = educkdb:prepare(Conn, "insert into test values($1, $2, $3, $4);"),

    ok = educkdb:bind_uint8(Insert, 1, 0),
    ok = educkdb:bind_uint16(Insert, 2, 0),
    ok = educkdb:bind_uint32(Insert, 3, 0),
    ok = educkdb:bind_uint64(Insert, 4, 0),

    {ok, _, [[1]]} = x(Insert),

    ok = educkdb:bind_uint8(Insert, 1, 128),
    ok = educkdb:bind_uint16(Insert, 2, 32768),
    ok = educkdb:bind_uint32(Insert, 3, 2147483648),
    ok = educkdb:bind_uint64(Insert, 4, 9223372036854775808),
    
    {ok, _, [[1]]} = x(Insert),

    ok = educkdb:bind_uint8(Insert, 1, 255),
    ok = educkdb:bind_uint16(Insert, 2, 65535),
    ok = educkdb:bind_uint32(Insert, 3, 4294967295),
    ok = educkdb:bind_uint64(Insert, 4, 18446744073709551615),

    {ok, _, [[1]]} = x(Insert),

    {ok, _, [
             [0,0,0,0],
             [128,32768,2147483648,9223372036854775808],
             [255, 65535, 4294967295, 18446744073709551615]
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

