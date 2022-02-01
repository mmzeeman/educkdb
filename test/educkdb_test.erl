%%
%% Test suite for educkdb.
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
    {ok, Res3} = educkdb:query(Conn, "select * from test;"),

    false = Res1 =:= Res2,

    ?assertEqual({ok, [], []}, educkdb:extract_result(Res1)),
    ?assertEqual({ok, [{column, <<"Count">>, bigint}], [[todo]]}, educkdb:extract_result(Res2)),
    ?assertEqual({ok, [{column, <<"id">>, integer},
                       {column, <<"x">>, varchar}], [[todo, todo]]}, educkdb:extract_result(Res3)),

    ok = educkdb:disconnect(Conn),
    ok = educkdb:close(Db),
    ok.

garbage_collect_test() ->
    F = fun() ->
                {ok, Db} = educkdb:open(":memory:"),
                {ok, Conn} = educkdb:connect(Db),

                %%
                %% TODO... do some queries.
                %%

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


