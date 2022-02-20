/*
 * Copyright 2022 Maas-Maarten Zeeman
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
#include "queue.h"

#define MAX_ATOM_LENGTH 255         /* from atom.h, not exposed in erlang include */
#define MAX_PATHNAME 512            /* unfortunately not in duckdb.h. */

#define CHUNK_SIZE 500             /* The target number of cells to get from a query result in one step before yielding */

#define NIF_NAME "educkdb_nif"

static ErlNifResourceType *educkdb_database_type = NULL;
static ErlNifResourceType *educkdb_connection_type = NULL;
static ErlNifResourceType *educkdb_result_type = NULL;
static ErlNifResourceType *educkdb_prepared_statement_type = NULL;
static ErlNifResourceType *educkdb_appender_type = NULL;

/* Database reference */
typedef struct {
    duckdb_database database;
} educkdb_database;

/* Database connection context and thread */
typedef struct {
    ErlNifTid tid;
    ErlNifThreadOpts *opts;
    queue *commands;
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
    educkdb_connection *connection;
    duckdb_appender appender;
} educkdb_appender;


typedef enum {
    cmd_unknown,

    cmd_query,
    cmd_execute_prepared,

    cmd_stop
} command_type;

typedef struct {
    command_type type;

    ErlNifEnv *env;
    educkdb_prepared_statement *stmt;
    ErlNifPid pid;
    
    ERL_NIF_TERM ref;
    ERL_NIF_TERM arg;
} educkdb_command;

static ERL_NIF_TERM atom_educkdb;
static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM atom_column;
static ERL_NIF_TERM atom_null;
static ERL_NIF_TERM atom_true;
static ERL_NIF_TERM atom_false;

static ERL_NIF_TERM push_command(ErlNifEnv *env, educkdb_connection *conn, educkdb_command *cmd);

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
    ErlNifBinary blob;
    ERL_NIF_TERM term;

    if(!enif_alloc_binary(size, &blob)) {
        return atom_error;
    }

    memcpy(blob.data, bytes, size);
    term = enif_make_binary(env, &blob);
    enif_release_binary(&blob);

    return term;
}

static void
command_destroy(void *obj)
{
    educkdb_command *cmd = (educkdb_command *) obj;

    if(cmd->env != NULL) {
        enif_free_env(cmd->env);
        cmd->env = NULL;
    }

    if(cmd->stmt != NULL) {
        enif_release_resource(cmd->stmt);
        cmd->stmt = NULL;
    }

    enif_free(cmd);
}

static educkdb_command *
command_create(command_type type)
{
    educkdb_command *cmd = (educkdb_command *) enif_alloc(sizeof(educkdb_command));
    if(cmd == NULL)
        return NULL;

    cmd->type = type;
    cmd->env = enif_alloc_env();
    if(cmd->env == NULL) {
        command_destroy(cmd);
        return NULL;
    }

    cmd->ref = enif_make_ref(cmd->env);

    cmd->arg = 0;
    cmd->stmt = NULL;

    return cmd;
}

/*
 *
 */
static void
destruct_educkdb_database(ErlNifEnv *env, void *arg)
{
    educkdb_database *database = (educkdb_database *) arg;
    duckdb_close(&(database->database));
}
 
/*
 *
 */
static void
destruct_educkdb_connection(ErlNifEnv *env, void *arg) {
    educkdb_connection *conn = (educkdb_connection *) arg;
    educkdb_command *cmd;

    if(conn->tid) { 
        cmd = command_create(cmd_stop);
        if(cmd) {
            /* Send the stop command to the command thread.
            */
            queue_push(conn->commands, cmd);

            /* Wait for the thread to finish
            */
            enif_thread_join(conn->tid, NULL);
        }

        enif_thread_opts_destroy(conn->opts);

        conn->tid = NULL;
    }

    if(conn->commands) {
        /* The thread has finished... now remove the command queue, and close
         * the database (if it was still open).
         */
        while(queue_has_item(conn->commands)) {
            command_destroy(queue_pop(conn->commands));
        }
        queue_destroy(conn->commands);
        conn->commands = NULL;
    }

    duckdb_disconnect(&(conn->connection));
}

/*
 * Destroy a materialized result
 */
static void
destruct_educkdb_result(ErlNifEnv *env, void *arg) {
    educkdb_result *res = (educkdb_result *) arg;
    duckdb_destroy_result(&(res->result));
}

static void
destruct_educkdb_prepared_statement(ErlNifEnv *env, void *arg) {
    educkdb_prepared_statement *stmt = (educkdb_prepared_statement *) arg;

    if(stmt->connection) {
        enif_release_resource(stmt->connection);
        stmt->connection = NULL;
    }

    duckdb_destroy_prepare(&(stmt->statement));
}

static void
destruct_educkdb_appender(ErlNifEnv *env, void *arg) {
    educkdb_appender *appender = (educkdb_appender *) arg;

    if(appender->connection) {
        enif_release_resource(appender->connection);
        appender->connection = NULL;
    }

    /* Does a flush close and destroy */
    duckdb_appender_destroy(&(appender->appender));
}

static const char*
duckdb_type_name(duckdb_type t) {
    switch(t) {
        case DUCKDB_TYPE_INVALID:   return "invalid";
        case DUCKDB_TYPE_BOOLEAN:   return "boolean";
        case DUCKDB_TYPE_TINYINT:   return "tinyint";
        case DUCKDB_TYPE_SMALLINT:  return "smallint";
        case DUCKDB_TYPE_INTEGER:   return "integer";
        case DUCKDB_TYPE_BIGINT:    return "bigint";
        case DUCKDB_TYPE_UTINYINT:  return "utinyint";
        case DUCKDB_TYPE_USMALLINT: return "usmallint";
        case DUCKDB_TYPE_UINTEGER:  return "uinteger";
        case DUCKDB_TYPE_UBIGINT:   return "ubigint";
        case DUCKDB_TYPE_FLOAT:     return "float";
        case DUCKDB_TYPE_DOUBLE:    return "double";
        case DUCKDB_TYPE_TIMESTAMP: return "timestamp";
        case DUCKDB_TYPE_DATE:      return "date";
        case DUCKDB_TYPE_TIME:      return "time";
        case DUCKDB_TYPE_INTERVAL:  return "interval";
        case DUCKDB_TYPE_HUGEINT:   return "hugeint";
        case DUCKDB_TYPE_VARCHAR:   return "varchar";
        case DUCKDB_TYPE_BLOB:      return "blob";
    }
}

static ERL_NIF_TERM
do_query(ErlNifEnv *env, educkdb_connection *conn, const ERL_NIF_TERM arg) {
    ErlNifBinary bin;
    educkdb_result *result;
    ERL_NIF_TERM eos = enif_make_int(env, 0);
    ERL_NIF_TERM eresult;

    if(!enif_inspect_iolist_as_binary(env, enif_make_list2(env, arg, eos), &bin)) {
        return make_error_tuple(env, "no_iodata");
    }

    result = enif_alloc_resource(educkdb_result_type, sizeof(educkdb_result));
    if(!result) {
        return make_error_tuple(env, "no_memory");
    }

    /* Run the query, this is handled in a separate thread started by the nif to
     * prevent it from hijacking the vm's scheduler for too long.
     * The result datastructure is passed back
     */
    if(duckdb_query(conn->connection, (char *) bin.data, &(result->result)) == DuckDBError) {
        /* Don't pass errors as a result data structure, but as an error tuple
         * with the error message in it.
         */
        const char *error_msg = duckdb_result_error(&(result->result)); 
        ERL_NIF_TERM erl_error_msg = enif_make_string(env, error_msg, ERL_NIF_LATIN1);
        enif_release_resource(result);

        return enif_make_tuple2(env, atom_error,
                enif_make_tuple2(env,
                    make_atom(env, "result"), erl_error_msg));
    }

    eresult = enif_make_resource(env, result);
    enif_release_resource(result);

    return make_ok_tuple(env, eresult);
}

static ERL_NIF_TERM
do_execute_prepared(ErlNifEnv *env, educkdb_prepared_statement *stmt, const ERL_NIF_TERM arg) {
    educkdb_result *result;
    ERL_NIF_TERM eresult;

    result = enif_alloc_resource(educkdb_result_type, sizeof(educkdb_result));
    if(!result) {
        return make_error_tuple(env, "no_memory");
    }

    if(duckdb_execute_prepared(stmt->statement, &(result->result)) == DuckDBError) {
        /* Don't pass errors as a result data structure, but as an error tuple
         * with the error message in it.
         */
        const char *error_msg = duckdb_result_error(&(result->result));
        ERL_NIF_TERM erl_error_msg = enif_make_string(env, error_msg, ERL_NIF_LATIN1);
        enif_release_resource(result);

        return enif_make_tuple2(env, atom_error,
                enif_make_tuple2(env,
                    make_atom(env, "result"), erl_error_msg));
    }

    eresult = enif_make_resource(env, result);
    enif_release_resource(result);

    return make_ok_tuple(env, eresult);
}

static ERL_NIF_TERM
evaluate_command(educkdb_command *cmd, educkdb_connection *conn) {
    switch(cmd->type) {
        case cmd_query:
            return do_query(cmd->env, conn, cmd->arg);
        case cmd_execute_prepared:
            return do_execute_prepared(cmd->env, cmd->stmt, cmd->arg);
        case cmd_unknown:      // not handled
        case cmd_stop:         // not handled here
            break;
    }

    return make_error_tuple(cmd->env, "invalid_command");
}


static ERL_NIF_TERM
push_command(ErlNifEnv *env, educkdb_connection *conn, educkdb_command *cmd) {
    ERL_NIF_TERM ref;

    if(&(cmd->pid) == NULL) {
        return make_error_tuple(env, "no_pid");
    }

    if(!queue_push(conn->commands, cmd)) {
        return make_error_tuple(env, "command_push");
    }

    ref = enif_make_copy(env, cmd->ref);
    return make_ok_tuple(env, ref);
}

static ERL_NIF_TERM
make_answer(educkdb_command *cmd, ERL_NIF_TERM answer)
{
    return enif_make_tuple3(cmd->env, atom_educkdb, cmd->ref, answer);
}

static void *
educkdb_connection_run(void *arg)
{
    educkdb_connection *conn = (educkdb_connection *) arg;
    educkdb_command *cmd;
    int continue_running = 1;
    ErlNifEnv *env = enif_alloc_env();

    while(continue_running) {
        cmd = queue_pop(conn->commands);

        if(cmd->type == cmd_stop) {
            continue_running = 0;
        } else {
            ERL_NIF_TERM response = evaluate_command(cmd, conn);
            ERL_NIF_TERM answer = enif_make_tuple3(cmd->env, atom_educkdb, cmd->ref, response);
            enif_send(env, &(cmd->pid), cmd->env, answer); 
        }

        command_destroy(cmd);
    }

    enif_free_env(env);
    return NULL;
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
    if(size <= 0)
        return make_error_tuple(env, "filename");

    /* [TODO] get options from the second attribute */

    // create the configuration object
    if (duckdb_create_config(&config) == DuckDBError) {
        return make_error_tuple(env, "create_config");
    }

    if(!enif_map_iterator_create(env, argv[1], &opts_iter, ERL_NIF_MAP_ITERATOR_FIRST)) {
        return enif_make_badarg(env);
    }
    while(enif_map_iterator_get_pair(env, &opts_iter, &key, &value)) {
        char key_str[50];
        char value_str[50];

        if(enif_get_atom(env, argv[1], key_str, sizeof(key_str), ERL_NIF_LATIN1)) {
            continue;
        }

        if(enif_get_string(env, argv[0], filename, MAX_PATHNAME, ERL_NIF_LATIN1) <= 0) {
            continue;
        }
        
        duckdb_set_config(&config, key_str, value_str);

        enif_map_iterator_next(env, &opts_iter);
    }
    enif_map_iterator_destroy(env, &opts_iter);

    database = enif_alloc_resource(educkdb_database_type, sizeof(educkdb_database));
    if(!database) {
        return make_error_tuple(env, "no_memory");
    }

    rc = duckdb_open_ext(filename, &(database->database), config, &open_error);
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

    duckdb_close(&(db->database));

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
 * connect_cmd
 *
 * Creates the communication thread for operations which potentially can't finish
 * within 1ms. 
 *
 */
static ERL_NIF_TERM
educkdb_connect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    educkdb_database *db;
    educkdb_connection *conn;
    duckdb_state rc;
    ERL_NIF_TERM db_conn;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_database_type, (void **) &db)) {
        return enif_make_badarg(env);
    }

    /* Initialize the connection resource */
    conn = enif_alloc_resource(educkdb_connection_type, sizeof(educkdb_connection));
    if(!conn) {
        return make_error_tuple(env, "no_memory");
    }

    /* Connect to the database. Internally this can mean the new connections
     * has to wait on a lock from the database connection manager. So this 
     * call must be dirty */
    rc = duckdb_connect(db->database, &(conn->connection));
    if(rc == DuckDBError) {
        enif_release_resource(conn);
        return make_error_tuple(env, "duckdb_connect");
    }
    
    /* Create command queue */
    conn->commands = queue_create();
    if(!conn->commands) {
        duckdb_close(&(conn->connection));
        enif_release_resource(conn);
        return make_error_tuple(env, "command_queue");
    }

    /* Start command processing thread */
    conn->opts = enif_thread_opts_create("thread_opts");
    if(conn->opts == NULL) {
        duckdb_close(&(conn->connection));
        queue_destroy(conn->commands);
        enif_release_resource(conn);
        return make_error_tuple(env, "thread_opts");
    }

    conn->opts->suggested_stack_size = 8192; 

    if(enif_thread_create("educkdb_connection", &conn->tid, educkdb_connection_run, conn, conn->opts) != 0) {
        duckdb_close(&(conn->connection));
        queue_destroy(conn->commands);
        enif_thread_opts_destroy(conn->opts);
        enif_release_resource(conn);
        return make_error_tuple(env, "thread_create");
    }

    db_conn = enif_make_resource(env, conn);
    enif_release_resource(conn);

    return make_ok_tuple(env, db_conn);
}

/*
 * disconnect
 *
 * Finalizes the communication thread, and disconnects the connection.
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

    /* Simply call destruct, so the thread stops and disconnect.
     * Note: this will immediately remove all commands from the queue.
     */
    destruct_educkdb_connection(env, (void *)conn);

    return atom_ok;
}

/*
 * Query
 */

/*
 * query_cmd
 *
 * Check the input values, and put the command on the queue to make
 * sure queries are handled in one calling thread.
 */
static ERL_NIF_TERM
educkdb_query_cmd(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_connection *conn;
    educkdb_command *cmd = NULL;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_connection_type, (void **) &conn)) {
        return enif_make_badarg(env);
    }

    cmd = command_create(cmd_query);
    if(!cmd) {
        return make_error_tuple(env, "command_create");
    }

    cmd->arg = enif_make_copy(cmd->env, argv[1]);
    enif_self(env, &(cmd->pid));

    return push_command(env, conn, cmd);
}

static ERL_NIF_TERM
make_cell(ErlNifEnv *env, duckdb_result *result, idx_t col, idx_t row) {
    if(duckdb_value_is_null(result, col, row)) {
        return atom_null;
    }

    switch(duckdb_column_type(result, col)) {
        case DUCKDB_TYPE_BOOLEAN:
            if(duckdb_value_boolean(result, col, row)) {
                return atom_true;
            } else {
                return atom_false;
            };
        case DUCKDB_TYPE_TINYINT:
        case DUCKDB_TYPE_SMALLINT:
        case DUCKDB_TYPE_INTEGER:
            {
                int32_t value = duckdb_value_int32(result, col, row);
                return enif_make_int(env, value);
            }
        case DUCKDB_TYPE_BIGINT:
            {
                int64_t value = duckdb_value_int64(result, col, row);
                return enif_make_int64(env, value);
            }
        case DUCKDB_TYPE_UTINYINT:
        case DUCKDB_TYPE_USMALLINT:
        case DUCKDB_TYPE_UINTEGER:
            {
                uint32_t value = duckdb_value_uint32(result, col, row);
                return enif_make_uint(env, value);
            }
        case DUCKDB_TYPE_UBIGINT:
            {
                uint64_t value = duckdb_value_uint64(result, col, row);
                return enif_make_uint64(env, value);
            }
        case DUCKDB_TYPE_FLOAT:
            // Erlang does not have floats, fall through to double.
        case DUCKDB_TYPE_DOUBLE:
            {
                double value = duckdb_value_double(result, col, row);
                return enif_make_double(env, value);
            }
        case DUCKDB_TYPE_TIMESTAMP:
            return make_atom(env, "todo");

        case DUCKDB_TYPE_DATE:
            return make_atom(env, "todo");

        case DUCKDB_TYPE_TIME:
            return make_atom(env, "todo");

        case DUCKDB_TYPE_INTERVAL:
            return make_atom(env, "todo");
        case DUCKDB_TYPE_HUGEINT:
            // record with two 64 bit integers
            return make_atom(env, "todo");

        case DUCKDB_TYPE_VARCHAR:
            {
                char *value = duckdb_value_varchar(result, col, row);
                if(value != NULL) {
                    ERL_NIF_TERM value_binary;
                    value_binary = make_binary(env, value, strlen(value));
                    if(value_binary == atom_error) {
                        // [todo] handle error 
                    }
                    duckdb_free(value);
                    value = NULL;
                    return value_binary;
                }
            }
        case DUCKDB_TYPE_BLOB:
            return make_atom(env, "todo");
        default:
            return atom_error;
    }
}

inline static idx_t
min_idx(idx_t a, idx_t b) {
    if(a < b) return a;
    return b;
}

inline static idx_t
max_idx(idx_t a, idx_t b) {
    if(a > b) return a;
    return b;
}

static ERL_NIF_TERM
educkdb_yield_extract_result(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_result *res;
    ERL_NIF_TERM column_info, rows, row, cell;
    unsigned long int row_count, column_count, from_row, downto_row, row_chunk_size;
    int pct;
    struct timeval start, stop, slice;
    gettimeofday(&start, NULL);

    if(argc != 6) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_result_type, (void **) &res)) {
        return enif_make_badarg(env);
    }
    row_count = duckdb_row_count(&(res->result));
    column_count = duckdb_column_count(&(res->result));

    if(!enif_is_list(env, argv[2])) {
        return enif_make_badarg(env);
    }
    if(!enif_get_uint64(env, argv[3], &from_row)) {
        return enif_make_badarg(env);
    }
    if(!enif_get_uint64(env, argv[4], &downto_row)) {
        return enif_make_badarg(env);
    }

    rows = argv[2];
    for(idx_t r=from_row; r-- > downto_row; ) {
        row = enif_make_list(env, 0);

        for(idx_t c=column_count; c-- > 0; ) {
            cell = make_cell(env, &(res->result), c, r);
            row = enif_make_list_cell(env, cell, row);
        }
        rows = enif_make_list_cell(env, row, rows);
    }

    if(downto_row != 0) {
        // Schedule another batch;
        if(!enif_get_uint64(env, argv[5], &row_chunk_size)) {
            return enif_make_badarg(env);
        }

        ERL_NIF_TERM new_argv[6];

        new_argv[0] = argv[0];
        new_argv[1] = argv[1];
        new_argv[2] = rows;
        new_argv[3] = argv[4];
        new_argv[4] = enif_make_uint64(env, downto_row - min_idx(downto_row, row_chunk_size));
        new_argv[5] = argv[5];

        gettimeofday(&stop, NULL);
        timersub(&stop, &start, &slice);
        pct = (int)((slice.tv_sec*1000000+slice.tv_usec)/10);
        if (pct > 100) {
            pct = 100;
        } else if (pct == 0) {
            pct = 1;
        } else {
        }

        /* Adjust the row_chunk_size when needed */
        if(pct < 20) {
            new_argv[5] = enif_make_uint64(env, row_chunk_size + CHUNK_SIZE);
        } else if(pct > 80) {
            new_argv[5] = enif_make_uint64(env, row_chunk_size / 2);
        } else {
            new_argv[5] = argv[5];
        }

        /* Inform the scheduler how much time we used */
        enif_consume_timeslice(env, pct);
        
        return enif_schedule_nif(env, "yield_extract_result", 0, educkdb_yield_extract_result, argc, new_argv);
    }

    if(!enif_is_list(env, argv[1])) {
        return enif_make_badarg(env);
    }

    return enif_make_tuple3(env, atom_ok, argv[1], rows);
}


static ERL_NIF_TERM
educkdb_extract_result(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_result *res;
    idx_t c, r;
    idx_t row_count, column_count;
    const char *column_name;
    const char *column_type_name;

    ERL_NIF_TERM column_info;
    ERL_NIF_TERM column_info_tuple;
    ERL_NIF_TERM rows, row, cell;

    ERL_NIF_TERM type_atom;
    ERL_NIF_TERM name_binary;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_result_type, (void **) &res)) {
        return enif_make_badarg(env);
    }

    /* For small number of results we can directly return the results without
     * rescheduling the nif.
     */

    /* Column info */
    row_count = duckdb_row_count(&(res->result));
    column_count = duckdb_column_count(&(res->result));

    /* The row count can be 0, while the column_info still contains data, so we 
     * have to prevent to return column info when there are no rows.
     **/
    column_info = enif_make_list(env, 0);
    if(row_count > 0) {
        for(c=column_count; c-- > 0; ) {
            column_name = duckdb_column_name(&(res->result), c);
            name_binary = make_binary(env, column_name, strlen(column_name));
            column_type_name = duckdb_type_name(duckdb_column_type(&(res->result), c));
            type_atom = make_atom(env, column_type_name);

            column_info_tuple = enif_make_tuple3(env, atom_column, name_binary, type_atom);
            column_info = enif_make_list_cell(env, column_info_tuple, column_info);
        }
    }
    
    /* Rows */
    rows = enif_make_list(env, 0);
    if(row_count == 0) {
        return enif_make_tuple3(env, atom_ok, column_info, rows);
    }

    /* Prepare args for yielding nif call */
    ERL_NIF_TERM new_args[6];

    /* Determine the number of rows we can return in one yield call */
    idx_t row_chunk_size = 1;
    if(row_count > 0 && column_count > 0 && column_count < CHUNK_SIZE) {
        row_chunk_size = CHUNK_SIZE / column_count;
    }

    new_args[0] = argv[0];
    new_args[1] = column_info;
    new_args[2] = rows;
    new_args[3] = enif_make_uint64(env, (unsigned long int) row_count); 
    new_args[4] = enif_make_uint64(env, (unsigned long int) row_count - min_idx(row_count, row_chunk_size)); 
    new_args[5] = enif_make_uint64(env, (unsigned long int) row_chunk_size);

    return educkdb_yield_extract_result(env, 6, new_args);
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
        return make_error_tuple(env, "no_memory");
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

/*
 * execute_prepared_cmd
 *
 * Check the input values, and put the command on the queue to make
 * sure queries are handled in one calling thread. Queries can also
 * run for an unknown amount of time, so instead of scheduling it on
 * a dirty scheduler, pass the reference to the prepared statement
 * via the queue, and run the query.
 */
static ERL_NIF_TERM
educkdb_execute_prepared_cmd(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
    educkdb_prepared_statement *stmt;
    educkdb_command *cmd = NULL;
 
    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], educkdb_prepared_statement_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    cmd = command_create(cmd_execute_prepared);
    if(!cmd) {
        return make_error_tuple(env, "command_create");
    }

    /* Make sure the reference to the statement is kept */
    cmd->stmt = stmt;
    enif_keep_resource(stmt);
    enif_self(env, &(cmd->pid));

    return push_command(env, stmt->connection, cmd);
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
        return make_error_tuple(env, "no_memory");
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
    atom_column = make_atom(env, "column");
    atom_null = make_atom(env, "null");
    atom_true = make_atom(env, "true");
    atom_false = make_atom(env, "false");

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
    {"open", 2, educkdb_open, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"close", 1, educkdb_close, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"config_flag_info", 0, educkdb_config_flag_info},

    {"connect", 1, educkdb_connect, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"disconnect", 1, educkdb_disconnect, ERL_NIF_DIRTY_JOB_IO_BOUND},

    {"query_cmd", 2, educkdb_query_cmd},
    {"extract_result", 1, educkdb_extract_result},

    {"prepare", 2, educkdb_prepare},
    {"execute_prepared_cmd", 1, educkdb_execute_prepared_cmd},

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
    {"bind_varchar", 3, educkdb_bind_varchar},
    {"bind_null", 2, educkdb_bind_null},

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
    {"append_varchar", 2, educkdb_append_varchar},
    {"append_null", 1, educkdb_append_null},
    {"appender_flush", 1, educkdb_appender_flush, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"appender_end_row", 1, educkdb_appender_end_row}
};

ERL_NIF_INIT(educkdb, nif_funcs, on_load, on_reload, on_upgrade, NULL);

