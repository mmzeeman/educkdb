/*
 * Copyright 2022-2024 Maas-Maarten Zeeman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

/*
 * duckdb_nif
*/

#define _DEFAULT_SOURCE

#include <erl_nif.h>
#include <sys/time.h>
#include <string.h>
#include <stdio.h>

#include <duckdb.h>

#define MAX_ATOM_LENGTH 255         /* from atom.h, not exposed in erlang include */
#define MAX_PATHNAME 512            /* unfortunately not in duckdb.h. */

#define DAY_EPOCH 719528            /* days since {0, 1, 1} -> {1970, 1, 1} */
#define MICS_EPOCH 62167219200000000    

#define NIF_NAME "educkdb_nif"

static ErlNifResourceType *educkdb_database_type = NULL;
static ErlNifResourceType *educkdb_connection_type = NULL;
static ErlNifResourceType *educkdb_result_type = NULL;
static ErlNifResourceType *educkdb_data_chunk_type = NULL;
static ErlNifResourceType *educkdb_prepared_statement_type = NULL;
static ErlNifResourceType *educkdb_appender_type = NULL;

/* Database reference */
typedef struct {
    duckdb_database database;
} educkdb_database;

/* Database connection */
typedef struct {
    duckdb_connection connection;
} educkdb_connection;

typedef struct {
    educkdb_connection *connection;
    duckdb_prepared_statement statement;
} educkdb_prepared_statement;

typedef struct {
    duckdb_result result;
} educkdb_result;

typedef struct {
    educkdb_result *result;
    duckdb_data_chunk data_chunk; 
} educkdb_data_chunk;

typedef struct {
    educkdb_connection *connection;
    duckdb_appender appender;
} educkdb_appender;

// Not exported for c api. Search for list_entry_t in header files.
typedef struct {
    uint64_t offset;
    uint64_t length;
} duckdb_list_entry_t;

static ERL_NIF_TERM atom_educkdb;
static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM atom_null;
static ERL_NIF_TERM atom_undefined;
static ERL_NIF_TERM atom_true;
static ERL_NIF_TERM atom_false;
static ERL_NIF_TERM atom_type;
static ERL_NIF_TERM atom_data;
static ERL_NIF_TERM atom_hugeint;
static ERL_NIF_TERM atom_uhugeint;
static ERL_NIF_TERM null_term;


static ERL_NIF_TERM
make_atom(ErlNifEnv *env, const char *atom_name)
{
    ERL_NIF_TERM atom;

    if(enif_make_existing_atom(env, atom_name, &atom, ERL_NIF_LATIN1)) {
        return atom;
    }

    return enif_make_atom(env, atom_name);
}

static ERL_NIF_TERM
make_ok_tuple(ErlNifEnv *env, ERL_NIF_TERM value)
{
    return enif_make_tuple2(env, atom_ok, value);
}

static ERL_NIF_TERM
make_error_tuple(ErlNifEnv *env, const char *reason)
{
    return enif_make_tuple2(env, make_atom(env, "error"), make_atom(env, reason));
}

static ERL_NIF_TERM
make_binary(ErlNifEnv *env, const void *bytes, unsigned int size)
{
    ERL_NIF_TERM result;
    memcpy(enif_make_new_binary(env, size, &result), bytes, size);
    return result;
}

/*
 *
 */
static void
destruct_educkdb_database(ErlNifEnv *env, void *arg)
{
    educkdb_database *database = (educkdb_database *) arg;
    duckdb_close(&database->database);
}
 
/*
 *
 */
static void
destruct_educkdb_connection(ErlNifEnv *env, void *arg) {
    educkdb_connection *connection = (educkdb_connection *) arg;
    duckdb_disconnect(&connection->connection);
}

/*
 * Destroy a materialized result
 */

static void
destruct_educkdb_result(ErlNifEnv *env, void *arg) {
    educkdb_result *res = (educkdb_result *) arg;
    duckdb_destroy_result(&res->result);
}

static void
destruct_educkdb_data_chunk(ErlNifEnv *env, void *arg) {
    educkdb_data_chunk *chunk = (educkdb_data_chunk *) arg;

    if(chunk->result) {
        enif_release_resource(chunk->result);
        chunk->result = NULL;
    }

    duckdb_destroy_data_chunk(&chunk->data_chunk);
}

static void
destruct_educkdb_prepared_statement(ErlNifEnv *env, void *arg) {
    educkdb_prepared_statement *stmt = (educkdb_prepared_statement *) arg;

    if(stmt->connection) {
        enif_release_resource(stmt->connection);
        stmt->connection = NULL;
    }

    duckdb_destroy_prepare(&stmt->statement);
}

static void
destruct_educkdb_appender(ErlNifEnv *env, void *arg) {
    educkdb_appender *appender = (educkdb_appender *) arg;

    if(appender->connection) {
        enif_release_resource(appender->connection);
        appender->connection = NULL;
    }

    /* Does a flush close and destroy */
    duckdb_appender_destroy(&appender->appender);
}

static const char*
duckdb_type_name(duckdb_type t) {
    switch(t) {
        case DUCKDB_TYPE_INVALID:      return "invalid";
        case DUCKDB_TYPE_ANY:          return "any";
        case DUCKDB_TYPE_BOOLEAN:      return "boolean";
        case DUCKDB_TYPE_TINYINT:      return "tinyint";
        case DUCKDB_TYPE_SMALLINT:     return "smallint";
        case DUCKDB_TYPE_INTEGER:      return "integer";
        case DUCKDB_TYPE_BIGINT:       return "bigint";
        case DUCKDB_TYPE_UTINYINT:     return "utinyint";
        case DUCKDB_TYPE_USMALLINT:    return "usmallint";
        case DUCKDB_TYPE_UINTEGER:     return "uinteger";
        case DUCKDB_TYPE_UBIGINT:      return "ubigint";
        case DUCKDB_TYPE_FLOAT:        return "float";
        case DUCKDB_TYPE_DOUBLE:       return "double";
        case DUCKDB_TYPE_TIMESTAMP:    return "timestamp";
        case DUCKDB_TYPE_DATE:         return "date";
        case DUCKDB_TYPE_TIME:         return "time";
        case DUCKDB_TYPE_INTERVAL:     return "interval";
        case DUCKDB_TYPE_HUGEINT:      return "hugeint";
        case DUCKDB_TYPE_UHUGEINT:     return "uhugeint";
        case DUCKDB_TYPE_VARCHAR:      return "varchar";
        case DUCKDB_TYPE_BLOB:         return "blob";
        case DUCKDB_TYPE_DECIMAL:      return "decimal";
        case DUCKDB_TYPE_TIMESTAMP_S:  return "timestamp_s";
        case DUCKDB_TYPE_TIMESTAMP_MS: return "timestamp_ms";
        case DUCKDB_TYPE_TIMESTAMP_NS: return "timestamp_ns";
        case DUCKDB_TYPE_ENUM:         return "enum";
        case DUCKDB_TYPE_LIST:         return "list";
        case DUCKDB_TYPE_STRUCT:       return "struct";
        case DUCKDB_TYPE_MAP:          return "map";
        case DUCKDB_TYPE_ARRAY:        return "array";
        case DUCKDB_TYPE_UUID:         return "uuid";
        case DUCKDB_TYPE_UNION:        return "union";
        case DUCKDB_TYPE_BIT:          return "bit";
        case DUCKDB_TYPE_TIME_TZ:      return "time_tz";
        case DUCKDB_TYPE_TIMESTAMP_TZ: return "timestamp_tz";
        case DUCKDB_TYPE_VARINT:       return "varint";
        case DUCKDB_TYPE_SQLNULL:      return "sqlnull";
        case DUCKDB_TYPE_STRING_LITERAL:      return "string_literal";
        case DUCKDB_TYPE_INTEGER_LITERAL:      return "integer_literal";
    }

    // When we are missing a DUCKDB_TYPE;
    return "unknown";
}

static const char*
duckdb_statement_type_name(duckdb_statement_type t) {
    switch(t) {
        case DUCKDB_STATEMENT_TYPE_INVALID: return "invalid";
        case DUCKDB_STATEMENT_TYPE_SELECT: return "select";
        case DUCKDB_STATEMENT_TYPE_INSERT: return "insert";
        case DUCKDB_STATEMENT_TYPE_UPDATE: return "update";
        case DUCKDB_STATEMENT_TYPE_EXPLAIN: return "explain";
        case DUCKDB_STATEMENT_TYPE_DELETE: return "delete";
        case DUCKDB_STATEMENT_TYPE_PREPARE: return "prepare";
        case DUCKDB_STATEMENT_TYPE_CREATE: return "create";
        case DUCKDB_STATEMENT_TYPE_EXECUTE: return "execute";
        case DUCKDB_STATEMENT_TYPE_ALTER: return "alter";
        case DUCKDB_STATEMENT_TYPE_TRANSACTION: return "transaction";
        case DUCKDB_STATEMENT_TYPE_COPY: return "copy";
        case DUCKDB_STATEMENT_TYPE_ANALYZE: return "analyze";
        case DUCKDB_STATEMENT_TYPE_VARIABLE_SET: return "variable_set";
        case DUCKDB_STATEMENT_TYPE_CREATE_FUNC: return "create_func";
        case DUCKDB_STATEMENT_TYPE_DROP: return "drop";
        case DUCKDB_STATEMENT_TYPE_EXPORT: return "export";
        case DUCKDB_STATEMENT_TYPE_PRAGMA: return "pragma";
        case DUCKDB_STATEMENT_TYPE_VACUUM: return "vacuum";
        case DUCKDB_STATEMENT_TYPE_CALL: return "call";
        case DUCKDB_STATEMENT_TYPE_SET: return "set";
        case DUCKDB_STATEMENT_TYPE_LOAD: return "load";
        case DUCKDB_STATEMENT_TYPE_RELATION: return "relation";
        case DUCKDB_STATEMENT_TYPE_EXTENSION: return "extension";
        case DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN: return "logical_plan";
        case DUCKDB_STATEMENT_TYPE_ATTACH: return "attach";
        case DUCKDB_STATEMENT_TYPE_DETACH: return "detach";
        case DUCKDB_STATEMENT_TYPE_MULTI: return "multi";
    }

    // When we are missing a DUCKDB_STATEMENT TYPE;
    return "unknown";
}

static ERL_NIF_TERM
handle_query_error(ErlNifEnv *env, educkdb_result *result) {
    /* Don't pass errors as a result data structure, but as an error tuple
     * with the error message in it.
     */
    const char *error_msg = duckdb_result_error(&(result->result)); 
    enif_release_resource(result);

    /* check if there is an error message, return {error, unknown} otherwise */
    if(error_msg == NULL) {
        return enif_make_tuple2(env, atom_error, make_atom(env, "unknown"));
    } 

    ERL_NIF_TERM erl_error_msg = enif_make_string(env, error_msg, ERL_NIF_LATIN1);
    return enif_make_tuple2(env, atom_error,
            enif_make_tuple2(env,
                make_atom(env, "result"), erl_error_msg));
} 

/*
 * Open the database. 
 *
 * Note: dirty nif call.
 */
static ERL_NIF_TERM
educkdb_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    char filename[MAX_PATHNAME];
    unsigned int size;
    educkdb_database *database;
    ERL_NIF_TERM educkdb_database;
    ERL_NIF_TERM key, value;
    ErlNifMapIterator opts_iter;
    duckdb_state rc;
    duckdb_config config;
    char *open_error;

    if(argc != 2)
        return enif_make_badarg(env);

    size = enif_get_string(env, argv[0], filename, MAX_PATHNAME, ERL_NIF_LATIN1);
    if(size <= 0) {
        return make_error_tuple(env, "filename");
    }

    // Loop through the map with options.
    if(!enif_map_iterator_create(env, argv[1], &opts_iter, ERL_NIF_MAP_ITERATOR_FIRST)) {
        return enif_make_badarg(env);
    }
    if (duckdb_create_config(&config) == DuckDBError) {
        return make_error_tuple(env, "create_config");
    }
    while(enif_map_iterator_get_pair(env, &opts_iter, &key, &value)) {
        char key_str[50];
        char value_str[50];

        if(!enif_get_atom(env, key, key_str, sizeof(key_str), ERL_NIF_LATIN1)) {
            enif_map_iterator_destroy(env, &opts_iter);
            duckdb_destroy_config(&config);
            return make_error_tuple(env, "option_key");
        }

        if(enif_get_string(env, value, value_str, sizeof(value_str), ERL_NIF_LATIN1) <= 0) {
            enif_map_iterator_destroy(env, &opts_iter);
            duckdb_destroy_config(&config);
            return make_error_tuple(env, "option_value");
        }

        if(duckdb_set_config(config, key_str, value_str) == DuckDBError) {
            enif_map_iterator_destroy(env, &opts_iter);
            duckdb_destroy_config(&config);
            return make_error_tuple(env, "set_config");
        }

        enif_map_iterator_next(env, &opts_iter);
    }
    enif_map_iterator_destroy(env, &opts_iter);

    // Open the database

    database = enif_alloc_resource(educkdb_database_type, sizeof(educkdb_database));
    if(!database) {
        return enif_raise_exception(env, make_atom(env, "no_memory"));
    }

    rc = duckdb_open_ext(filename, &(database->database), config, &open_error);
    duckdb_destroy_config(&config);
    if(rc == DuckDBError) {
        ERL_NIF_TERM erl_error_msg = enif_make_string(env, open_error, ERL_NIF_LATIN1);
        ERL_NIF_TERM error_tuple = enif_make_tuple2(env, atom_error,
                enif_make_tuple2(env,
                    make_atom(env, "open"), erl_error_msg));

        duckdb_free(open_error);
        return error_tuple;
    }

    educkdb_database = enif_make_resource(env, database);
    enif_release_resource(database);

    return make_ok_tuple(env, educkdb_database);
}

/*
 * Close the database. 
 *
 * Note: dirty nif call.
 */
static ERL_NIF_TERM
educkdb_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    educkdb_database *db;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_database_type, (void **) &db)) {
        return enif_make_badarg(env);
    }

    destruct_educkdb_database(env, (void *) db);

    return atom_ok;
}

/*
 * Get a list of config flags
 */
static ERL_NIF_TERM
educkdb_config_flag_info(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    size_t config_count = duckdb_config_count();
    const char *name;
    const char *description;
    unsigned int i;

    ERL_NIF_TERM info_map;

    if(argc != 0) {
        return enif_make_badarg(env);
    }

    info_map = enif_make_new_map(env);

    for(i = 0; i < config_count; i++) {
        duckdb_get_config_flag(i, &name, &description);
        enif_make_map_put(env, info_map,
                make_atom(env, name),
                make_binary(env, description, strlen(description)),
                &info_map);
    }

    return info_map;
}

/*
 * connect
 *
 */
static ERL_NIF_TERM
educkdb_connect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    educkdb_database *db;
    educkdb_connection *connection;
    duckdb_connection con;
    ERL_NIF_TERM db_conn;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_database_type, (void **) &db)) {
        return enif_make_badarg(env);
    }

    /* Connect to the database. Internally this can mean the new connections
     * has to wait on a lock from the database connection manager. So this 
     * call must be dirty
     */ 
    if(duckdb_connect(db->database, &con) == DuckDBError) {
        return make_error_tuple(env, "duckdb_connect");
    }

    /* Initialize the connection resource */
    connection = enif_alloc_resource(educkdb_connection_type, sizeof(educkdb_connection));
    if(!connection) {
        duckdb_disconnect(&con);
        return enif_raise_exception(env, make_atom(env, "no_memory"));
    }

    connection->connection = con;

    db_conn = enif_make_resource(env, connection);
    enif_release_resource(connection);

    return make_ok_tuple(env, db_conn);
}

/*
 * disconnect
 *
 */
static ERL_NIF_TERM
educkdb_disconnect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    educkdb_connection *conn;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_connection_type, (void **) &conn)) {
        return enif_make_badarg(env);
    }

    destruct_educkdb_connection(env, (void *)conn);

    return atom_ok;
}

/*
 * Query
 */

static ERL_NIF_TERM
educkdb_query(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_connection *conn;
    ErlNifBinary bin;
    ERL_NIF_TERM eos = enif_make_int(env, 0);

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_connection_type, (void **) &conn)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_iolist_as_binary(env, enif_make_list2(env, argv[1], eos), &bin)) {
        return enif_make_badarg(env);
    } 
    
    educkdb_result *result = enif_alloc_resource(educkdb_result_type, sizeof(educkdb_result));
    if(!result) {
        return enif_raise_exception(env, make_atom(env, "no_memory"));
    }

    if(duckdb_query(conn->connection, (char *) bin.data, &(result->result)) == DuckDBError) {
        return handle_query_error(env, result);
    }

    ERL_NIF_TERM eresult = enif_make_resource(env, result);
    enif_release_resource(result);

    return make_ok_tuple(env, eresult);
}

static ERL_NIF_TERM
make_date_tuple(ErlNifEnv *env, duckdb_date_struct date) {
    return enif_make_tuple3(env,
            enif_make_int(env, date.year),
            enif_make_int(env, date.month),
            enif_make_int(env, date.day));
}

static ERL_NIF_TERM
make_time_tuple(ErlNifEnv *env, duckdb_time_struct time) {
    if(time.micros != 0.0) {
        return enif_make_tuple3(env,
                enif_make_int(env, time.hour),
                enif_make_int(env, time.min),
                enif_make_double(env, (double) time.sec + (time.micros / 1000000.0)));
    } 

    return enif_make_tuple3(env,
            enif_make_int(env, time.hour),
            enif_make_int(env, time.min),
            enif_make_int(env, time.sec));
}

/*
 * Extract result, chunk version 
 */

static ERL_NIF_TERM
extract_data_boolean(ErlNifEnv *env, bool *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i = 0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            if(vector_data[i + offset]) {
                data[i] = atom_true;
            } else {
                data[i] = atom_false;
            }
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_utinyint(ErlNifEnv *env, uint8_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i = 0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            data[i] = enif_make_uint(env, vector_data[i + offset]);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_usmallint(ErlNifEnv *env, uint16_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i = 0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            data[i] = enif_make_uint(env, vector_data[i + offset]);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_uinteger(ErlNifEnv *env, uint32_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i = 0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            data[i] = enif_make_uint(env, vector_data[i + offset]);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_ubigint(ErlNifEnv *env, uint64_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i = 0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            data[i] = enif_make_uint64(env, vector_data[i + offset]);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_tinyint(ErlNifEnv *env, int8_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            data[i] = enif_make_int(env, vector_data[i + offset]);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_smallint(ErlNifEnv *env, int16_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            data[i] = enif_make_int(env, vector_data[i + offset]);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_integer(ErlNifEnv *env, int32_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            data[i] = enif_make_int(env, vector_data[i + offset]);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_integer_literal(ErlNifEnv *env, int64_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            data[i] = enif_make_int64(env, vector_data[i + offset]);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_bigint(ErlNifEnv *env, int64_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            data[i] = enif_make_int64(env, vector_data[i + offset]);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_float(ErlNifEnv *env, float *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            data[i] = enif_make_double(env, (double) vector_data[i + offset]);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_double(ErlNifEnv *env, double *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            data[i] = enif_make_double(env, vector_data[i + offset]);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_timestamp(ErlNifEnv *env, duckdb_timestamp *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            duckdb_timestamp_struct timestamp = duckdb_from_timestamp(vector_data[i + offset]);
            data[i] = enif_make_tuple2(env,
                    make_date_tuple(env, timestamp.date),
                    make_time_tuple(env, timestamp.time));
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_date(ErlNifEnv *env, duckdb_date *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            duckdb_date_struct date = duckdb_from_date(vector_data[i + offset]);
            data[i] = make_date_tuple(env, date);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_time(ErlNifEnv *env, duckdb_time *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            duckdb_time_struct time= duckdb_from_time(vector_data[i + offset]);
            data[i] = make_time_tuple(env, time);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static int
is_round(double n) {
    double epsilon = 1e-10;
    double d = n - (double) ((int) n);

    if(d < 0) {
        return -d < epsilon;
    }

    return d < epsilon;
}

static ERL_NIF_TERM
extract_data_interval(ErlNifEnv *env, duckdb_interval *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            duckdb_interval interval = vector_data[i + offset];

            double seconds = (double) interval.micros / 1000000.0;
            int hours = (int) (seconds / 3600);
            seconds -= hours * 3600;
            int minutes = (int) (seconds / 60);
            seconds -= minutes * 60;

            ERL_NIF_TERM erl_seconds;

            if(is_round(seconds)) {
                erl_seconds = enif_make_int(env, (int) seconds);
            } else {
                erl_seconds = enif_make_double(env, seconds);
            }

            data[i] = enif_make_tuple3(env,
                    enif_make_tuple3(env,
                        enif_make_int(env, hours),
                        enif_make_int(env, minutes),
                        erl_seconds),
                    enif_make_int(env, interval.days),
                    enif_make_int(env, interval.months));
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}


static ERL_NIF_TERM
extract_data_hugeint(ErlNifEnv *env, duckdb_hugeint *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            duckdb_hugeint huge = vector_data[i + offset];
            data[i] = enif_make_tuple3(env, atom_hugeint, enif_make_int64(env, huge.upper), enif_make_uint64(env, huge.lower));
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_uhugeint(ErlNifEnv *env, duckdb_uhugeint *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            duckdb_uhugeint huge = vector_data[i + offset];
            data[i] = enif_make_tuple3(env, atom_uhugeint, enif_make_uint64(env, huge.upper), enif_make_uint64(env, huge.lower));
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_uuid(ErlNifEnv *env, duckdb_hugeint *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            duckdb_hugeint huge = vector_data[i + offset];
            char buf[16];

            // First bit is flipped because of sorting.
            int64_t upper = huge.upper ^ (((int64_t) 1) << 63);

            buf[0] = upper >> 56 & 0xFF; 
            buf[1] = upper >> 48 & 0xFF; 
            buf[2] = upper >> 40 & 0xFF; 
            buf[3] = upper >> 32 & 0xFF; 
            buf[4] = upper >> 24 & 0xFF; 
            buf[5] = upper >> 16 & 0xFF; 
            buf[6] = upper >>  8 & 0xFF; 
            buf[7] = upper       & 0xFF; 

            buf[8] =  huge.lower >> 56 & 0xFF; 
            buf[9] =  huge.lower >> 48 & 0xFF; 
            buf[10] = huge.lower >> 40 & 0xFF; 
            buf[11] = huge.lower >> 32 & 0xFF; 
            buf[12] = huge.lower >> 24 & 0xFF; 
            buf[13] = huge.lower >> 16 & 0xFF; 
            buf[14] = huge.lower >>  8 & 0xFF; 
            buf[15] = huge.lower       & 0xFF; 

            data[i] = make_binary(env, buf, sizeof(buf));
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_varchar(ErlNifEnv *env, duckdb_string_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            duckdb_string_t value = vector_data[i + offset];
            data[i] = make_binary(env, duckdb_string_t_data(&value), duckdb_string_t_length(value));
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_string_literal(ErlNifEnv *env, duckdb_string_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            duckdb_string_t value = vector_data[i + offset];
            data[i] = make_binary(env, duckdb_string_t_data(&value), duckdb_string_t_length(value));
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_uint8_enum(ErlNifEnv *env, duckdb_logical_type logical_type, uint8_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            uint8_t enum_index = vector_data[i + offset];
            char *value = duckdb_enum_dictionary_value(logical_type, (idx_t) enum_index);
            data[i] = make_binary(env, value, strlen(value));
            duckdb_free(value);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_uint16_enum(ErlNifEnv *env, duckdb_logical_type logical_type, uint16_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            uint16_t enum_index = vector_data[i + offset];
            char *value = duckdb_enum_dictionary_value(logical_type, (idx_t) enum_index);
            data[i] = make_binary(env, value, strlen(value));
            duckdb_free(value);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_uint32_enum(ErlNifEnv *env, duckdb_logical_type logical_type, uint32_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            uint32_t enum_index = vector_data[i + offset];
            char *value = duckdb_enum_dictionary_value(logical_type, (idx_t) enum_index);
            data[i] = make_binary(env, value, strlen(value));
            duckdb_free(value);
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}


static ERL_NIF_TERM
extract_data_enum(ErlNifEnv *env, duckdb_logical_type logical_type, void *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    duckdb_type enum_internal_type_id  = duckdb_enum_internal_type(logical_type);

    switch(enum_internal_type_id) {
        case DUCKDB_TYPE_UTINYINT:
            return extract_data_uint8_enum(env, logical_type, (uint8_t *) vector_data, validity_mask, offset, count);
        case DUCKDB_TYPE_USMALLINT:
            return extract_data_uint16_enum(env, logical_type, (uint16_t *) vector_data, validity_mask, offset, count);
        case DUCKDB_TYPE_UINTEGER:
            return extract_data_uint32_enum(env, logical_type, (uint32_t *) vector_data, validity_mask, offset, count);
        default:
            return enif_raise_exception(env, make_atom(env, "unexpected_internal_type"));
    }
}

/**
 * Complex nested types.
 */

static ERL_NIF_TERM extract_data(ErlNifEnv *, duckdb_logical_type, duckdb_vector, uint64_t, uint64_t);
 
static ERL_NIF_TERM
extract_data_list(ErlNifEnv *env, duckdb_vector vector, duckdb_logical_type logical_type, duckdb_list_entry_t *vector_data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];
    duckdb_vector child_vector = duckdb_list_vector_get_child(vector);
    duckdb_logical_type list_child_type = duckdb_list_type_child_type(logical_type);

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            duckdb_list_entry_t entry = vector_data[i + offset];
            data[i] = extract_data(env, list_child_type, child_vector, entry.offset, entry.length);
        } else {
            data[i] = null_term;
        }
    }

    duckdb_destroy_logical_type(&list_child_type);

    return enif_make_list_from_array(env, data, count);
}


static ERL_NIF_TERM
extract_data_struct(ErlNifEnv *env, duckdb_vector vector, duckdb_logical_type logical_type, uint64_t *validity_mask, uint64_t offset, uint64_t count)  {
    ERL_NIF_TERM data[count];
    idx_t child_count = duckdb_struct_type_child_count(logical_type);

    for(idx_t i=0; i < count; i++) {
        if(duckdb_validity_row_is_valid(validity_mask, i + offset)) {
            data[i] = enif_make_new_map(env);

            for(idx_t j=0; j < child_count; j++) {
                char *child_name = duckdb_struct_type_child_name(logical_type, j);
                ERL_NIF_TERM key = make_binary(env, child_name, strlen(child_name));
                duckdb_free(child_name);

                duckdb_logical_type child_type = duckdb_struct_type_child_type(logical_type, j);
                duckdb_vector child_vector = duckdb_struct_vector_get_child(vector, j);
                ERL_NIF_TERM list = extract_data(env, child_type, child_vector, i, 1);
                duckdb_destroy_logical_type(&child_type);

                ERL_NIF_TERM value, tail;
                enif_get_list_cell(env, list, &value, &tail);

                enif_make_map_put(env, data[i], key, value, &data[i]);
            }
        } else {
            data[i] = null_term;
        }
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
extract_data_no_extract(ErlNifEnv *env, const char *type_name, uint64_t offset, uint64_t count) {
    ERL_NIF_TERM data[count];

    for(idx_t i=0; i < count; i++) {
        data[i] = enif_make_tuple2(env, make_atom(env, "no_extract"), make_atom(env, type_name));
    }

    return enif_make_list_from_array(env, data, count);
}

static ERL_NIF_TERM
internal_extract_data(ErlNifEnv *env, duckdb_vector vector, duckdb_logical_type logical_type, duckdb_type type_id, void *data, uint64_t *validity_mask, uint64_t offset, uint64_t count) {
    switch(type_id) {
        case DUCKDB_TYPE_BOOLEAN:
            return extract_data_boolean(env, (bool *) data, validity_mask, offset, count);

        // Signed Integers
        case DUCKDB_TYPE_TINYINT:
            return extract_data_tinyint(env, (int8_t *) data, validity_mask, offset, count);
        case DUCKDB_TYPE_SMALLINT:
            return extract_data_smallint(env, (int16_t *) data, validity_mask, offset, count);
        case DUCKDB_TYPE_INTEGER:
            return extract_data_integer(env, (int32_t *) data, validity_mask, offset, count);
        case DUCKDB_TYPE_BIGINT:
            return extract_data_bigint(env, (int64_t *) data, validity_mask, offset, count);

        // Unsigned Integers
        case DUCKDB_TYPE_UTINYINT:
            return extract_data_utinyint(env, (uint8_t *) data, validity_mask, offset, count);
        case DUCKDB_TYPE_USMALLINT:
            return extract_data_usmallint(env, (uint16_t *) data, validity_mask, offset, count);
        case DUCKDB_TYPE_UINTEGER:
            return extract_data_uinteger(env, (uint32_t *) data, validity_mask, offset, count);
        case DUCKDB_TYPE_UBIGINT:
            return extract_data_ubigint(env, (uint64_t *) data, validity_mask, offset, count);

        // Floats and Doubles
        case DUCKDB_TYPE_FLOAT:
            return extract_data_float(env, (float *) data, validity_mask, offset, count);
        case DUCKDB_TYPE_DOUBLE:
            return extract_data_double(env, (double *) data, validity_mask, offset, count);
            
        // Date and time records
        case DUCKDB_TYPE_TIMESTAMP:
            return extract_data_timestamp(env, (duckdb_timestamp *) data, validity_mask, offset, count);
        case DUCKDB_TYPE_DATE:
            return extract_data_date(env, (duckdb_date *) data, validity_mask, offset, count);
        case DUCKDB_TYPE_TIME:
            return extract_data_time(env, (duckdb_time *) data, validity_mask, offset, count);

        // Interval
        case DUCKDB_TYPE_INTERVAL:
            return extract_data_interval(env, (duckdb_interval *) data, validity_mask, offset, count);
        
        // Large integers
        case DUCKDB_TYPE_HUGEINT:
            return extract_data_hugeint(env, (duckdb_hugeint *) data, validity_mask, offset, count);
        case DUCKDB_TYPE_UHUGEINT:
            return extract_data_uhugeint(env, (duckdb_uhugeint *) data, validity_mask, offset, count);
            
        // Binary like types
        case DUCKDB_TYPE_VARCHAR:
            return extract_data_varchar(env, (duckdb_string_t *) data, validity_mask, offset, count);
        case DUCKDB_TYPE_BLOB:
            return extract_data_varchar(env, (duckdb_string_t *) data, validity_mask, offset, count);

        case DUCKDB_TYPE_DECIMAL:
            return extract_data_no_extract(env, "decimal", offset, count);

        // Timestamps
        case DUCKDB_TYPE_TIMESTAMP_S:
            return extract_data_no_extract(env, "timestamp_s", offset, count);
        case DUCKDB_TYPE_TIMESTAMP_MS:
            return extract_data_no_extract(env, "timestamp_ms", offset, count);
        case DUCKDB_TYPE_TIMESTAMP_NS:
            return extract_data_no_extract(env, "timestamp_ns", offset, count);

        // Compound types
        case DUCKDB_TYPE_ENUM:
            return extract_data_enum(env, logical_type, data, validity_mask, offset, count);
        case DUCKDB_TYPE_LIST:
            return extract_data_list(env, vector, logical_type, (duckdb_list_entry_t *) data, validity_mask, offset, count);
        case DUCKDB_TYPE_STRUCT:
            return extract_data_struct(env, vector, logical_type, validity_mask, offset, count);
        case DUCKDB_TYPE_MAP:
            return extract_data_no_extract(env, "map", offset, count);
        case DUCKDB_TYPE_ARRAY:
            return extract_data_no_extract(env, "array", offset, count);

        case DUCKDB_TYPE_UUID:
            return extract_data_uuid(env, (duckdb_hugeint *) data, validity_mask, offset, count);

        case DUCKDB_TYPE_UNION:
            return extract_data_no_extract(env, "union", offset, count);

        case DUCKDB_TYPE_BIT:
            return extract_data_no_extract(env, "bit", offset, count);

        case DUCKDB_TYPE_TIME_TZ:
            return extract_data_no_extract(env, "time_tz", offset, count);
        case DUCKDB_TYPE_TIMESTAMP_TZ:
            return extract_data_no_extract(env, "timestamp_tz", offset, count);

        case DUCKDB_TYPE_ANY:
            return extract_data_no_extract(env, "any", offset, count);

        case DUCKDB_TYPE_VARINT:
            return extract_data_no_extract(env, "varint", offset, count);

        case DUCKDB_TYPE_STRING_LITERAL:
            return extract_data_string_literal(env, (duckdb_string_t *) data, validity_mask, offset, count);

        case DUCKDB_TYPE_INTEGER_LITERAL:
            return extract_data_integer_literal(env, (int64_t *) data, validity_mask, offset, count);

        default:
            return extract_data_no_extract(env, "default", offset, count);
    }
}

static ERL_NIF_TERM
extract_data(ErlNifEnv *env, duckdb_logical_type logical_type, duckdb_vector vector, uint64_t offset, uint64_t count) {
    void *data = duckdb_vector_get_data(vector);
    uint64_t *validity_mask = duckdb_vector_get_validity(vector);
    duckdb_type type_id = duckdb_get_type_id(logical_type); 

    return internal_extract_data(env, vector, logical_type, type_id, data, validity_mask, offset, count);
}

static ERL_NIF_TERM
extract_chunk_types(ErlNifEnv *env, duckdb_data_chunk chunk, idx_t column_count) {
    ERL_NIF_TERM column[column_count];

    for(idx_t i=0; i < column_count; i++) {
        duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, i);
        duckdb_logical_type logical_type = duckdb_vector_get_column_type(vector);
        duckdb_type type_id = duckdb_get_type_id(logical_type);

        const char *type_name = duckdb_type_name(type_id);
        column[i] = make_atom(env, type_name);
    }

    return enif_make_list_from_array(env, column, column_count); 
}

static ERL_NIF_TERM
extract_chunk_columns(ErlNifEnv *env, duckdb_data_chunk chunk, idx_t column_count) {
    ERL_NIF_TERM column[column_count];
    idx_t tuple_count = duckdb_data_chunk_get_size(chunk);

    for(idx_t i=0; i < column_count; i++) {
        duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, i);
        duckdb_logical_type logical_type = duckdb_vector_get_column_type(vector);

        column[i] = extract_data(env, logical_type, vector, 0, tuple_count);
    }

    return enif_make_list_from_array(env, column, column_count); 
}


/**
 * Chunks
 */


static ERL_NIF_TERM
extract_column_names(ErlNifEnv *env, duckdb_result result, idx_t count) {
    ERL_NIF_TERM names[count];

    for(idx_t i=0; i < count; i++) {
        const char *name = duckdb_column_name(&result, i);
        names[i] = make_binary(env, name, strlen(name));
    }

    return enif_make_list_from_array(env, names, count); 
}

static ERL_NIF_TERM
educkdb_column_names(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_result *res;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_result_type, (void **) &res)) {
        return enif_make_badarg(env);
    }

    idx_t count = duckdb_column_count(&res->result);
    return extract_column_names(env, res->result, count);
}

static ERL_NIF_TERM
educkdb_fetch_chunk(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_result *res;
    duckdb_data_chunk chunk;
    educkdb_data_chunk *echunk;
    ERL_NIF_TERM rchunk;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_result_type, (void **) &res)) {
        return enif_make_badarg(env);
    }

    chunk = duckdb_fetch_chunk(res->result);

    if(chunk == NULL) {
        return enif_make_atom(env, "$end");
    }

    echunk = enif_alloc_resource(educkdb_data_chunk_type, sizeof(educkdb_data_chunk));
    if(!echunk) {
        duckdb_destroy_data_chunk(&chunk);
        return enif_raise_exception(env, make_atom(env, "no_memory"));
    }

    enif_keep_resource(res);
    echunk->result = res;

    echunk->data_chunk = chunk;
    rchunk = enif_make_resource(env, echunk);
    enif_release_resource(echunk);

    return rchunk;
}

static ERL_NIF_TERM
make_chunks(ErlNifEnv *env, educkdb_result *result, idx_t chunk_count) { 
    ERL_NIF_TERM chunks[chunk_count];

    for(idx_t i=0; i < chunk_count; i++) {
        duckdb_data_chunk chunk = duckdb_result_get_chunk(result->result, i);
        if(chunk == NULL) {
            return enif_raise_exception(env, make_atom(env, "no_chunk"));
        }

        educkdb_data_chunk *echunk = enif_alloc_resource(educkdb_data_chunk_type, sizeof(educkdb_data_chunk));
        if(echunk == NULL) {
            duckdb_destroy_data_chunk(&chunk);
            return enif_raise_exception(env, make_atom(env, "no_memory"));
        }

        enif_keep_resource(result);
        echunk->result = result;
        echunk->data_chunk = chunk;
        chunks[i] = enif_make_resource(env, echunk);

        enif_release_resource(echunk);
    }

    return enif_make_list_from_array(env, chunks, chunk_count); 
}

static ERL_NIF_TERM
educkdb_get_chunks(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_result *res;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_result_type, (void **) &res)) {
        return enif_make_badarg(env);
    }

    idx_t chunk_count = duckdb_result_chunk_count(res->result);
    return make_chunks(env, res, chunk_count);
}


static ERL_NIF_TERM
educkdb_chunk_get_column_count(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_data_chunk *chunk;
    idx_t count;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_data_chunk_type, (void **) &chunk)) {
        return enif_make_badarg(env);
    }

    count = duckdb_data_chunk_get_column_count(chunk->data_chunk);

    return enif_make_uint64(env, count);
}

static ERL_NIF_TERM
educkdb_chunk_get_column_types(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_data_chunk *chunk;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_data_chunk_type, (void **) &chunk)) {
        return enif_make_badarg(env);
    }

    return extract_chunk_types(env, chunk->data_chunk, duckdb_data_chunk_get_column_count(chunk->data_chunk));
}

static ERL_NIF_TERM
educkdb_chunk_get_columns(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_data_chunk *chunk;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_data_chunk_type, (void **) &chunk)) {
        return enif_make_badarg(env);
    }

    return extract_chunk_columns(env, chunk->data_chunk, duckdb_data_chunk_get_column_count(chunk->data_chunk));
}

static ERL_NIF_TERM
educkdb_chunk_get_size(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_data_chunk *chunk;
    idx_t size;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_data_chunk_type, (void **) &chunk)) {
        return enif_make_badarg(env);
    }

    size = duckdb_data_chunk_get_size(chunk->data_chunk);

    return enif_make_uint64(env, size);
}


/*
 * Prepared Statements.
 */

static ERL_NIF_TERM
educkdb_prepare(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_connection *conn;
    educkdb_prepared_statement *prepared_statement;
    ErlNifBinary bin;
    ERL_NIF_TERM eos = enif_make_int(env, 0);
    ERL_NIF_TERM eprepared_statement;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_connection_type, (void **) &conn)) {
        return enif_make_badarg(env);
    }
    
    if(!enif_inspect_iolist_as_binary(env, enif_make_list2(env, argv[1], eos), &bin)) {
        return enif_make_badarg(env);
    }

    prepared_statement = enif_alloc_resource(educkdb_prepared_statement_type, sizeof(educkdb_prepared_statement));
    if(!prepared_statement) {
        return enif_raise_exception(env, make_atom(env, "no_memory"));
    }
    prepared_statement->connection = NULL;

    if(duckdb_prepare(conn->connection, (char *) bin.data, &(prepared_statement->statement)) == DuckDBError) {
        /* Don't pass errors as a prepared_statment's, but as an error tuple
         * with the error message in it. ({error, {prepare, binary()}})
         */
        const char *error_msg = duckdb_prepare_error(prepared_statement->statement);
        ERL_NIF_TERM erl_error_msg = enif_make_string(env, error_msg, ERL_NIF_LATIN1);
        enif_release_resource(prepared_statement);

        return enif_make_tuple2(env, atom_error,
                enif_make_tuple2(env,
                    make_atom(env, "prepare"), erl_error_msg));
    }

    enif_keep_resource(conn);
    prepared_statement->connection = conn;
    eprepared_statement = enif_make_resource(env, prepared_statement);
    enif_release_resource(prepared_statement);

    return make_ok_tuple(env, eprepared_statement);
}

static ERL_NIF_TERM
educkdb_execute_prepared(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
 
    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    educkdb_result *result = enif_alloc_resource(educkdb_result_type, sizeof(educkdb_result));
    if(!result) {
        return enif_raise_exception(env, make_atom(env, "no_memory"));
    }

    if(duckdb_execute_prepared(stmt->statement, &(result->result)) == DuckDBError) {
        return handle_query_error(env, result);
    }

    ERL_NIF_TERM eresult = enif_make_resource(env, result);
    enif_release_resource(result);

    return make_ok_tuple(env, eresult);
}

static ERL_NIF_TERM
educkdb_parameter_name(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    const char* name;
    ERL_NIF_TERM erl_name;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    name = duckdb_parameter_name(stmt->statement, index);
    if(!name) {
        return enif_make_badarg(env);
    }

    erl_name = make_binary(env, name, strlen(name));
    duckdb_free((char *)name);

    return erl_name;
}

static ERL_NIF_TERM
educkdb_parameter_index(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    char name[MAX_ATOM_LENGTH];
    educkdb_prepared_statement *stmt;
    ErlNifBinary bin;
    idx_t index;
    duckdb_state rc;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_atom(env, argv[1], name, sizeof(name), ERL_NIF_LATIN1)) {
        if(!enif_inspect_binary(env, argv[1], &bin)) {
            return enif_make_badarg(env);
        } else {
            if(bin.size + 1 > sizeof(name)) {
                return enif_make_badarg(env);
            }

            memcpy(name, (char *)bin.data, bin.size);
            name[bin.size] = '\0';
        }
    } 

    rc = duckdb_bind_parameter_index(stmt->statement, &index, name);
    if(rc == DuckDBError) {
        return enif_make_atom(env, "none");
    }

    return enif_make_uint64(env, index);
}
 
static ERL_NIF_TERM
educkdb_parameter_type(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    duckdb_type type;
    const char* type_name;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    type = duckdb_param_type(stmt->statement, index);
    if(type == DUCKDB_TYPE_INVALID) {
        return enif_make_badarg(env);
    }

    type_name = duckdb_type_name(type);
    return make_atom(env, type_name);
}

static ERL_NIF_TERM
educkdb_clear_bindings(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    duckdb_state rc;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    rc = duckdb_clear_bindings(stmt->statement);
    if(rc == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_statement_type(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    return make_atom(env,
            duckdb_statement_type_name(
                duckdb_prepared_statement_type(stmt->statement)));
}

static ERL_NIF_TERM
educkdb_parameter_count(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    return enif_make_uint64(env, duckdb_nparams(stmt->statement));
}

static ERL_NIF_TERM
educkdb_bind_boolean(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    int value;
    bool bind_value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_int(env, argv[2], &value)) {
        return enif_make_badarg(env);
    } 

    bind_value = (value != 0);

    if(duckdb_bind_boolean(stmt->statement, (idx_t) index, bind_value) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_int8(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    int value;
    int8_t bind_value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_int(env, argv[2], &value)) {
        return enif_make_badarg(env);
    } 

    if(value > INT8_MAX || value < INT8_MIN) {
        return enif_make_badarg(env);
    }

    bind_value = (int8_t) value;

    if(duckdb_bind_int8(stmt->statement, (idx_t) index, bind_value) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_int16(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    int value;
    int16_t bind_value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_int(env, argv[2], &value)) {
        return enif_make_badarg(env);
    } 

    if(value > INT16_MAX || value < INT16_MIN) {
        return enif_make_badarg(env);
    }

    bind_value = (int16_t) value;

    if(duckdb_bind_int16(stmt->statement, (idx_t) index, bind_value) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_int32(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    int value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_int(env, argv[2], &value)) {
        return enif_make_badarg(env);
    } 

    if(duckdb_bind_int32(stmt->statement, (idx_t) index, value) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_int64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    ErlNifSInt64 value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }
    
    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_int64(env, argv[2], &value)) {
        return enif_make_badarg(env);
    } 

    if(duckdb_bind_int64(stmt->statement, (idx_t) index, value) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_uint8(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    unsigned int value;
    uint8_t bind_value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_uint(env, argv[2], &value)) {
        return enif_make_badarg(env);
    } 

    if(value > UINT8_MAX) {
        return enif_make_badarg(env);
    }

    bind_value = (uint8_t) value;

    if(duckdb_bind_uint8(stmt->statement, (idx_t) index, bind_value) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_uint16(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    unsigned int value;
    uint16_t bind_value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_uint(env, argv[2], &value)) {
        return enif_make_badarg(env);
    } 

    if(value > UINT16_MAX) {
        return enif_make_badarg(env);
    }

    bind_value = (uint16_t) value;

    if(duckdb_bind_uint16(stmt->statement, (idx_t) index, bind_value) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_uint32(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    unsigned int value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_uint(env, argv[2], &value)) {
        return enif_make_badarg(env);
    } 

    if(duckdb_bind_uint32(stmt->statement, (idx_t) index, value) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_uint64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    ErlNifUInt64 value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }
    
    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_uint64(env, argv[2], &value)) {
        return enif_make_badarg(env);
    } 

    if(duckdb_bind_uint64(stmt->statement, (idx_t) index, value) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}


static ERL_NIF_TERM
educkdb_bind_float(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    double value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }
    
    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_double(env, argv[2], &value)) {
        return enif_make_badarg(env);
    } 

    if(duckdb_bind_double(stmt->statement, (idx_t) index, (float) value) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_double(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    double value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }
    
    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_double(env, argv[2], &value)) {
        return enif_make_badarg(env);
    } 

    if(duckdb_bind_double(stmt->statement, (idx_t) index, value) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_date(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    int value; // in gregorian days 
    duckdb_date date;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }
    
    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_int(env, argv[2], &value)) {
        return atom_error;
    }

    date.days = value - DAY_EPOCH;
    if(duckdb_bind_date(stmt->statement, (idx_t) index, date) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_time(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    ErlNifSInt64 value;
    duckdb_time time; // microseconds since 00:00:00:000

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }
    
    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_int64(env, argv[2], &value)) {
        return enif_make_badarg(env);
    }

    time.micros = value;
    if(duckdb_bind_time(stmt->statement, (idx_t) index, time) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_timestamp(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    ErlNifSInt64 value;
    duckdb_timestamp timestamp;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }
    
    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(!enif_get_int64(env, argv[2], &value)) {
        return enif_make_badarg(env);
    }

    timestamp.micros = value - MICS_EPOCH;
    if(duckdb_bind_timestamp(stmt->statement, (idx_t) index, timestamp) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_varchar(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;
    ErlNifBinary binary;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }
    
    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 
    
    if(!enif_inspect_iolist_as_binary(env, argv[2], &binary)) {
        return make_error_tuple(env, "no_iodata");
    }

    if(duckdb_bind_varchar_length(stmt->statement, (idx_t) index, (const char *)binary.data, binary.size) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_bind_null(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    ErlNifUInt64 index;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }
    
    if(!enif_get_uint64(env, argv[1], &index)) {
        return enif_make_badarg(env);
    } 

    if(duckdb_bind_null(stmt->statement, (idx_t) index) == DuckDBError) {
        return atom_error;
    }

    return atom_ok;
}

/*
 * Appender
 */

static ERL_NIF_TERM
get_appender_error(ErlNifEnv *env, duckdb_appender appender) {
    const char *error_msg = duckdb_appender_error(appender);

    if(error_msg == NULL) {
        return enif_make_tuple2(env, atom_error, make_atom(env, "unknown"));
    }

    ERL_NIF_TERM erl_error_msg = enif_make_string(env, error_msg, ERL_NIF_LATIN1);
    return enif_make_tuple2(env, atom_error,
            enif_make_tuple2(env,
                make_atom(env, "appender"), erl_error_msg));
}
 
static ERL_NIF_TERM
educkdb_appender_create(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_connection *conn;
    educkdb_appender *appender;
    char atom_schema[10];
    const char *schema = NULL;
    ErlNifBinary schema_bin;
    ErlNifBinary table_bin;
    ERL_NIF_TERM eos = enif_make_int(env, 0);

    ERL_NIF_TERM eappender;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_connection_type, (void **) &conn)) {
        return enif_make_badarg(env);
    }

    if(enif_get_atom(env, argv[1], atom_schema, sizeof(atom_schema), ERL_NIF_LATIN1)) {
        if(strncmp(atom_schema, "undefined", sizeof(atom_schema)) == 0) {
            schema = NULL;
        } else {
            return enif_make_badarg(env);
        }
    } else if(enif_inspect_iolist_as_binary(env, enif_make_list2(env, argv[1], eos), &schema_bin)) {
        schema = (const char *) schema_bin.data;
    } else {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_iolist_as_binary(env, enif_make_list2(env, argv[2], eos), &table_bin)) {
        return enif_make_badarg(env);
    }

    appender = enif_alloc_resource(educkdb_appender_type, sizeof(educkdb_appender));
    if(!appender) {
        return enif_raise_exception(env, make_atom(env, "no_memory"));
    }
    appender->connection = NULL;

    if(duckdb_appender_create(conn->connection, schema, (const char *) table_bin.data, &(appender->appender)) == DuckDBError) {
        /* Don't pass errors as a prepared_statment's, but as an error tuple
         * with the error message in it. ({error, {prepare, binary()}})
         */
        ERL_NIF_TERM error = get_appender_error(env, appender->appender);
        enif_release_resource(appender);

        return error;
    }

    enif_keep_resource(conn);
    appender->connection = conn;
    eappender = enif_make_resource(env, appender);
    enif_release_resource(appender);

    return make_ok_tuple(env, eappender);
}

static ERL_NIF_TERM
educkdb_append_boolean(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    int value;
    bool append_value;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[1], &value)) {
        return enif_make_badarg(env);
    }

    if(value > INT8_MAX || value < INT8_MIN) {
        return enif_make_badarg(env);
    }

    append_value = (value != 0);

    if(duckdb_append_bool(appender->appender, append_value) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_int8(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    int value;
    int8_t append_value;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[1], &value)) {
        return enif_make_badarg(env);
    }

    if(value > INT8_MAX || value < INT8_MIN) {
        return enif_make_badarg(env);
    }

    append_value = (int8_t) value;

    if(duckdb_append_int8(appender->appender, append_value) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_int16(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    int value;
    int16_t append_value;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[1], &value)) {
        return enif_make_badarg(env);
    } 

    if(value > INT16_MAX || value < INT16_MIN) {
        return enif_make_badarg(env);
    }

    append_value = (int16_t) value;

    if(duckdb_append_int16(appender->appender, append_value) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_int32(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    int value;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[1], &value)) {
        return enif_make_badarg(env);
    } 

    if(duckdb_append_int32(appender->appender, value) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_int64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    ErlNifSInt64 value;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int64(env, argv[1], &value)) {
        return enif_make_badarg(env);
    } 

    if(duckdb_append_int64(appender->appender, value) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_uint8(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    unsigned int value;
    uint8_t append_value;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint(env, argv[1], &value)) {
        return enif_make_badarg(env);
    } 

    if(value > UINT8_MAX) {
        return enif_make_badarg(env);
    }

    append_value = (uint8_t) value;

    if(duckdb_append_uint8(appender->appender, append_value) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_uint16(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    unsigned int value;
    uint16_t append_value;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint(env, argv[1], &value)) {
        return enif_make_badarg(env);
    } 

    if(value > UINT16_MAX) {
        return enif_make_badarg(env);
    }

    append_value = (uint16_t) value;

    if(duckdb_append_uint16(appender->appender, append_value) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_uint32(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    unsigned int value;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint(env, argv[1], &value)) {
        return enif_make_badarg(env);
    } 

    if(duckdb_append_uint32(appender->appender, value) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_uint64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    ErlNifUInt64 value;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_uint64(env, argv[1], &value)) {
        return enif_make_badarg(env);
    } 

    if(duckdb_append_uint64(appender->appender, value) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_float(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    double value;
    float append_value;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_double(env, argv[1], &value)) {
        return enif_make_badarg(env);
    } 

    append_value = (float) value;

    if(duckdb_append_float(appender->appender, append_value) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_double(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    double value;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_double(env, argv[1], &value)) {
        return enif_make_badarg(env);
    } 

    if(duckdb_append_double(appender->appender, value) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_date(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    int value; // in gregorian days 
    duckdb_date date;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[1], &value)) {
        return atom_error;
    }

    date.days = value - DAY_EPOCH;
    if(duckdb_append_date(appender->appender, date) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_time(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    ErlNifSInt64 value;
    duckdb_time time; // microseconds since 00:00:00:000

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int64(env, argv[1], &value)) {
        return enif_make_badarg(env);
    }

    time.micros = value;
    if(duckdb_append_time(appender->appender, time) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_timestamp(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    ErlNifSInt64 value;
    duckdb_timestamp timestamp;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int64(env, argv[1], &value)) {
        return enif_make_badarg(env);
    }

    timestamp.micros = value - MICS_EPOCH;
    if(duckdb_append_timestamp(appender->appender, timestamp) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_varchar(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;
    ErlNifBinary binary;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_iolist_as_binary(env, argv[1], &binary)) {
        return make_error_tuple(env, "no_iodata");
    }

    if(duckdb_append_varchar_length(appender->appender, (const char *) binary.data, binary.size) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_append_null(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(duckdb_append_null(appender->appender) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_appender_flush(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(duckdb_appender_flush(appender->appender) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

static ERL_NIF_TERM
educkdb_appender_end_row(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_appender *appender;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_appender_type, (void **) &appender)) {
        return enif_make_badarg(env);
    }

    if(duckdb_appender_end_row(appender->appender) == DuckDBError) {
        return get_appender_error(env, appender->appender);
    }

    return atom_ok;
}

/*
 * Load the nif. Initialize some stuff and such
 */
static int
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    educkdb_database_type = enif_open_resource_type(env, "educkdb_nif",
            "educkdb_database_type", destruct_educkdb_database,
            ERL_NIF_RT_CREATE, NULL);
    if(!educkdb_database_type) return -1;

    educkdb_connection_type = enif_open_resource_type(env, NIF_NAME,
            "educkdb_connection_type", destruct_educkdb_connection,
            ERL_NIF_RT_CREATE, NULL);
    if(!educkdb_connection_type) return -1;

    educkdb_result_type = enif_open_resource_type(env, NIF_NAME,
            "educkdb_result", destruct_educkdb_result,
            ERL_NIF_RT_CREATE, NULL);
    if(!educkdb_result_type) return -1;

    educkdb_data_chunk_type = enif_open_resource_type(env, NIF_NAME,
            "educkdb_data_chunk", destruct_educkdb_data_chunk,
            ERL_NIF_RT_CREATE, NULL);
    if(!educkdb_data_chunk_type) return -1;

    educkdb_prepared_statement_type = enif_open_resource_type(env, NIF_NAME,
            "educkdb_prepared_statement_type", destruct_educkdb_prepared_statement,
            ERL_NIF_RT_CREATE, NULL);
    if(!educkdb_prepared_statement_type) return -1;

    educkdb_appender_type = enif_open_resource_type(env, NIF_NAME,
            "educkdb_appender_type", destruct_educkdb_appender,
            ERL_NIF_RT_CREATE, NULL);
    if(!educkdb_appender_type) return -1;

    atom_educkdb = make_atom(env, "educkdb");
    atom_ok = make_atom(env, "ok");
    atom_error = make_atom(env, "error");
    atom_null = make_atom(env, "null");
    atom_undefined = make_atom(env, "undefined");
    atom_true = make_atom(env, "true");
    atom_false = make_atom(env, "false");
    atom_type = make_atom(env, "type");
    atom_data = make_atom(env, "data");
    atom_hugeint = make_atom(env, "hugeint");
    atom_uhugeint = make_atom(env, "uhugeint");

    null_term = atom_undefined;

    return 0;
}

static int on_reload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    return 0;
}

static int on_upgrade(ErlNifEnv* env, void** priv, void** old_priv_data, ERL_NIF_TERM load_info)
{
    return 0;
}

static ErlNifFunc nif_funcs[] = {
    // Connect
    {"open", 2, educkdb_open, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"close", 1, educkdb_close, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"config_flag_info", 0, educkdb_config_flag_info},
    {"connect", 1, educkdb_connect, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"disconnect", 1, educkdb_disconnect, ERL_NIF_DIRTY_JOB_IO_BOUND},

    // Queries
    {"prepare", 2, educkdb_prepare, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"query", 2, educkdb_query, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"execute_prepared", 1, educkdb_execute_prepared, ERL_NIF_DIRTY_JOB_IO_BOUND},

    // Result
    {"column_names", 1, educkdb_column_names},
    {"fetch_chunk", 1, educkdb_fetch_chunk, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"get_chunks", 1, educkdb_get_chunks, ERL_NIF_DIRTY_JOB_IO_BOUND},

    // Chunks
    {"chunk_column_count", 1, educkdb_chunk_get_column_count},
    {"chunk_column_types", 1, educkdb_chunk_get_column_types},
    {"chunk_columns", 1, educkdb_chunk_get_columns},
    {"chunk_size", 1, educkdb_chunk_get_size},

    // Prepare
    {"statement_type", 1, educkdb_statement_type},
    {"parameter_count", 1, educkdb_parameter_count},
    {"parameter_name", 2, educkdb_parameter_name},
    {"parameter_index", 2, educkdb_parameter_index},
    {"parameter_type", 2, educkdb_parameter_type},
    {"clear_bindings", 1, educkdb_clear_bindings},
    {"bind_boolean_intern", 3, educkdb_bind_boolean},
    {"bind_int8", 3, educkdb_bind_int8},
    {"bind_int16", 3, educkdb_bind_int16},
    {"bind_int32", 3, educkdb_bind_int32},
    {"bind_int64", 3, educkdb_bind_int64},
    {"bind_uint8", 3, educkdb_bind_uint8},
    {"bind_uint16", 3, educkdb_bind_uint16},
    {"bind_uint32", 3, educkdb_bind_uint32},
    {"bind_uint64", 3, educkdb_bind_uint64},
    {"bind_float", 3, educkdb_bind_float},
    {"bind_double", 3, educkdb_bind_double},

    {"bind_date_intern", 3, educkdb_bind_date},
    {"bind_time_intern", 3, educkdb_bind_time},
    {"bind_timestamp_intern", 3, educkdb_bind_timestamp},

    {"bind_varchar", 3, educkdb_bind_varchar},

    {"bind_null", 2, educkdb_bind_null},

    // Appender
    {"appender_create", 3, educkdb_appender_create},
    {"append_boolean_intern", 2, educkdb_append_boolean},
    {"append_int8", 2, educkdb_append_int8},
    {"append_int16", 2, educkdb_append_int16},
    {"append_int32", 2, educkdb_append_int32},
    {"append_int64", 2, educkdb_append_int64},
    {"append_uint8", 2, educkdb_append_uint8},
    {"append_uint16", 2, educkdb_append_uint16},
    {"append_uint32", 2, educkdb_append_uint32},
    {"append_uint64", 2, educkdb_append_uint64},
    {"append_float", 2, educkdb_append_float},
    {"append_double", 2, educkdb_append_double},

    {"append_date_intern", 2, educkdb_append_date},
    {"append_time_intern", 2, educkdb_append_time},
    {"append_timestamp_intern", 2, educkdb_append_timestamp},

    {"append_varchar", 2, educkdb_append_varchar},
    {"append_null", 1, educkdb_append_null},
    {"appender_flush", 1, educkdb_appender_flush, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"appender_end_row", 1, educkdb_appender_end_row}
};

ERL_NIF_INIT(educkdb, nif_funcs, on_load, on_reload, on_upgrade, NULL);

