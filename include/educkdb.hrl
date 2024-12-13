%%
%%
%%

-record(hugeint, {
    upper :: educkdb:int64(),
    lower :: educkdb:uint64()
}).

-record(map, {
    keys :: list(),
    values :: list()
}).

-record(column, {
    name :: binary(),
    type :: educkdb:type_name()
}).
