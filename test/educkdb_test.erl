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
    cleanup(),
    {ok, Db} = educkdb:open(?DB1),
    ok = educkdb:close(Db),
    ok.

open_and_connect_test() ->
    cleanup(),
    {ok, Db} = educkdb:open(?DB1),
    {ok, _Conn} = educkdb:connect(Db),
    ok = educkdb:close(Db),
    ok.

open_connect_and_disconnect_test() ->
    cleanup(),
    {ok, Db} = educkdb:open(?DB1),
    {ok, Conn} = educkdb:connect(Db),
    ok = educkdb:disconnect(Conn),
    ok = educkdb:close(Db),
    ok.

query_test() ->
    cleanup(),
    {ok, Db} = educkdb:open(?DB1),
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
    cleanup(),
    Query = "this is a syntax error;",
    {ok, Db} = educkdb:open(?DB1),
    {ok, Conn} = educkdb:connect(Db),

    {error, {prepare, _}} = educkdb:prepare(Conn, Query),

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

cleanup() ->
    rm_rf(?DB1),
    rm_rf(?DB2).

rm_rf(Filename) ->
    case file:delete(Filename) of
        ok -> ok;
        {error, _} -> ok
    end.


