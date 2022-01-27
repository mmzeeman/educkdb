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

#include <erl_nif.h>
#include <string.h>
#include <stdio.h>

#include <duckdb.h>
#include "queue.h"

#define MAX_ATOM_LENGTH 255         /* from atom.h, not exposed in erlang include */

static ErlNifResourceType *educkdb_database_type = NULL;
static ErlNifResourceType *educkdb_connection_type = NULL;
/*
static ErlNifResourceType *esqlite_statement_type = NULL;
static ErlNifResourceType *esqlite_backup_type = NULL;
*/

/* Database referend */
typedef struct {
    duckdb_database *database;
} educkdb_database;

/* Database connection context and thread */
typedef struct {
    ErlNifTid tid;
    ErlNifThreadOpts* opts;

    duckdb_connection *connection;
    queue *commands;

} educkdb_connection;

typedef enum {
    cmd_unknown,

    cmd_connect,
    cmd_disconnect,

    cmd_stop
} command_type;

typedef struct {
    command_type type;

    ErlNifEnv *env;
    ERL_NIF_TERM ref;
    ErlNifPid pid;

    ERL_NIF_TERM arg;
    ERL_NIF_TERM stmt;
} educkdb_command;

static ERL_NIF_TERM atom_educkdb;
static ERL_NIF_TERM push_command(ErlNifEnv *env, educkdb_connection *conn, educkdb_command *cmd);

static ERL_NIF_TERM
make_atom(ErlNifEnv *env, const char *atom_name)
{
    ERL_NIF_TERM atom;

    if(enif_make_existing_atom(env, atom_name, &atom, ERL_NIF_LATIN1))
        return atom;

    return enif_make_atom(env, atom_name);
}

static ERL_NIF_TERM
make_ok_tuple(ErlNifEnv *env, ERL_NIF_TERM value)
{
    return enif_make_tuple2(env, make_atom(env, "ok"), value);
}

static ERL_NIF_TERM
make_error_tuple(ErlNifEnv *env, const char *reason)
{
    return enif_make_tuple2(env, make_atom(env, "error"), make_atom(env, reason));
}

static void
command_destroy(void *obj)
{
    educkdb_command *cmd = (educkdb_command *) obj;

    if(cmd->env != NULL) {
        enif_free_env(cmd->env);
        cmd->env = NULL;
    }

    enif_free(cmd);
}

static educkdb_command *
command_create()
{
    educkdb_command *cmd = (educkdb_command *) enif_alloc(sizeof(educkdb_command));
    if(cmd == NULL)
        return NULL;

    cmd->env = enif_alloc_env();
    if(cmd->env == NULL) {
        command_destroy(cmd);
        return NULL;
    }

    cmd->type = cmd_unknown;
    cmd->ref = 0;
    cmd->arg = 0;
    cmd->stmt = 0;

    return cmd;
}

/*
 *
 */
static void
destruct_educkdb_connection(ErlNifEnv *env, void *arg)
{
    educkdb_connection *conn = (educkdb_connection *) arg;
    educkdb_command *cmd;
   
    if(conn->tid) { 
        cmd = command_create();
        if(cmd) {
            /* Send the stop command to the command thread.
            */
            cmd->type = cmd_stop;
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

    if(conn->connection) {
        duckdb_disconnect(conn->connection);
        conn->connection = NULL;
    }
}



static ERL_NIF_TERM
evaluate_command(educkdb_command *cmd, educkdb_connection *conn)
{
    // esqlite_statement *stmt = NULL;

    /*
    if(cmd->stmt) {
        if(!enif_get_resource(cmd->env, cmd->stmt, esqlite_statement_type, (void **) &stmt)) {
	    return make_error_tuple(cmd->env, "invalid_statement");
        }
    }
    */

    switch(cmd->type) {
        /*
        case cmd_open:
            return do_open(cmd->env, conn, cmd->arg);
        case cmd_update_hook_set:
            return do_set_update_hook(cmd->env, conn, cmd->arg);
        case cmd_exec:
            return do_exec(cmd->env, conn, cmd->arg);
        case cmd_changes:
            return do_changes(cmd->env, conn, cmd->arg);
        case cmd_prepare:
            return do_prepare(cmd->env, conn, cmd->arg);
        case cmd_multi_step:
            return do_multi_step(cmd->env, conn->db, stmt->statement, cmd->arg);
        case cmd_reset:
            return do_reset(cmd->env, conn->db, stmt->statement);
        case cmd_bind:
            return do_bind(cmd->env, conn->db, stmt->statement, cmd->arg);
        case cmd_column_names:
            return do_column_names(cmd->env, stmt->statement);
        case cmd_column_types:
            return do_column_types(cmd->env, stmt->statement);
        case cmd_backup_init:
            return do_backup_init(cmd->env, conn->db, cmd->arg);
        case cmd_backup_step:
            return do_backup_step(cmd->env, conn->db, cmd->arg);
        case cmd_backup_remaining:
            return do_backup_remaining(cmd->env, cmd->arg);
        case cmd_backup_pagecount:
            return do_backup_pagecount(cmd->env, cmd->arg);
        case cmd_backup_finish:
            return do_backup_finish(cmd->env, cmd->arg);
        case cmd_close:
            return do_close(cmd->env, conn, cmd->arg);
        case cmd_last_insert_rowid:
            return do_last_insert_rowid(cmd->env, conn);
        case cmd_insert:
            return do_insert(cmd->env, conn, cmd->arg);
        case cmd_get_autocommit:
            return do_get_autocommit(cmd->env, conn);
        */
        case cmd_connect:
            /* [TODO] can be a no-op to make sure the thread and queue is functioning. */
        case cmd_disconnect:
            /* [TODO] */
        case cmd_unknown:      // not handled
        case cmd_stop:         // not handled here
            break;
    }

    return make_error_tuple(cmd->env, "invalid_command");
}


static ERL_NIF_TERM
push_command(ErlNifEnv *env, educkdb_connection *conn, educkdb_command *cmd) {
    if(!queue_push(conn->commands, cmd))
        return make_error_tuple(env, "command_push_failed");

    return make_atom(env, "ok");
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

    while(continue_running) {
        cmd = queue_pop(conn->commands);

        if(cmd->type == cmd_stop) {
            continue_running = 0;
        } else {
            enif_send(NULL, &cmd->pid, cmd->env, make_answer(cmd, evaluate_command(cmd, conn)));
        }

        command_destroy(cmd);
    }

    return NULL;
}

/*
 * Start the processing thread
 */
/*
static ERL_NIF_TERM
esqlite_start(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    ERL_NIF_TERM db_conn;

    // / * Initialize the resource * /
    conn = enif_alloc_resource(esqlite_connection_type, sizeof(esqlite_connection));
    if(!conn)
        return make_error_tuple(env, "no_memory");

    conn->db = NULL;

    / * Create command queue * /
    conn->commands = queue_create();
    if(!conn->commands) {
        enif_release_resource(conn);
        return make_error_tuple(env, "command_queue_create_failed");
    }

    / * Start command processing thread * /
    conn->opts = enif_thread_opts_create("esqlite_thread_opts");
    if(conn->opts == NULL) {
        return make_error_tuple(env, "thread_opts_failed");
    }

    conn->opts->suggested_stack_size = 128; 

    if(enif_thread_create("esqlite_connection", &conn->tid, esqlite_connection_run, conn, conn->opts) != 0) {
        enif_thread_opts_destroy(conn->opts);
        enif_release_resource(conn);
        return make_error_tuple(env, "thread_create_failed");
    }

    db_conn = enif_make_resource(env, conn);
    enif_release_resource(conn);

    return make_ok_tuple(env, db_conn);
}
*/

/*
 * Open the database. 
 *
 * Note: dirty nif call.
 */
static ERL_NIF_TERM
educkdb_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    if(argc != 2)
        return enif_make_badarg(env);

    /*
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);

    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");

    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    */


    return enif_make_atom(env, "ok");
}

/*
 * Close the database. 
 *
 * Note: dirty nif call.
 */
static ERL_NIF_TERM
educkdb_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    if(argc != 1)
        return enif_make_badarg(env);

    /*
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);

    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");

    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    */


    return enif_make_atom(env, "ok");
}

/*
 * connect_cmd
 *
 * Creates the communication thread for operations which potentially can't finish
 * within 1ms. 
 *
 */
static ERL_NIF_TERM
educkdb_connect_cmd(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    if(argc != 1)
        return enif_make_badarg(env);

    /*
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);

    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");

    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    */

    return enif_make_atom(env, "ok");
}

/*
 * disconnect_cmd
 *
 * Finalizes the communication thread, and disconnects the connection.
 *
 */
static ERL_NIF_TERM
educkdb_disconnect_cmd(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    if(argc != 1)
        return enif_make_badarg(env);

    /*
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);

    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");

    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    */

    return enif_make_atom(env, "ok");
}



/*
 * Load the nif. Initialize some stuff and such
 */
static int
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    ErlNifResourceType *rt;

    rt = enif_open_resource_type(env, "educkdb_nif", "educkdb_connection_type", destruct_educkdb_connection,
            ERL_NIF_RT_CREATE, NULL);
    if(!rt) return -1;
    educkdb_connection_type = rt;

    /*
    rt =  enif_open_resource_type(env, "educkdb_nif", "educkdb_statement_type", destruct_esqlite_statement,
            ERL_NIF_RT_CREATE, NULL);
    if(!rt) return -1;
    esqlite_statement_type = rt;
    */

    atom_educkdb = make_atom(env, "educkdb");

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
    {"connect_cmd", 1, educkdb_connect_cmd},
    {"disconnect_cmd", 1, educkdb_disconnect_cmd}
};

ERL_NIF_INIT(educkdb, nif_funcs, on_load, on_reload, on_upgrade, NULL);

