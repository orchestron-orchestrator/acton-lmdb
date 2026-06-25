#include <lmdb.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/stat.h>
#include "rts/rts.h"

// LMDB normally uses TLS (thread local storage) for some stuff, which is why
// LMDB normally should run on one thread and one thread can only open one LMDB
// database at a time. It's basically as a convenience, like there is nothing to
// clean up on thread destruction since it's in TLS, and I think sometimes as a
// slight optimization. It doesn't really do much for us in Acton since we have
// our own ways of cleaning up and need to interop well with the actor / thread
// semantics, essentially that actors can migrate between threads and a thread
// might run any actor so its ultimately forbidden to rely on TLS - programs
// shouild only rely on actor state!

void lmdbQ_libQ___ext_init__() {
}

static void raise_lmdb_error(const char *op, int rc) {
    char error_buf[256];
    snprintf(error_buf, sizeof(error_buf), "%s: %s (%d)", op, mdb_strerror(rc), rc);
    RAISE(lmdbQ_libQ_LMDBError, to$str(error_buf));
}

/* Method dispatch target for proc def _pin_affinity() in WriteTransaction */
$R lmdbQ_libQ_WriteTransactionD__pin_affinityG_local(lmdbQ_libQ_WriteTransaction self, $Cont c$cont) {
    pin_actor_affinity();
    WorkerCtx wctx = GET_WCTX();
    long wtid = SHARED_RQ;
    if (wctx != NULL) {
        wtid = wctx->id;
    }
    return $R_CONT(c$cont, to$int(wtid));
}

/* Method dispatch target for proc def _set_affinity(wtid: int) in WriteCursor */
$R lmdbQ_libQ_WriteCursorD__set_affinityG_local(lmdbQ_libQ_WriteCursor self, $Cont c$cont, int64_t wtid) {
    set_actor_affinity((int)wtid);
    return $R_CONT(c$cont, B_None);
}

B_str lmdbQ_libQ_version() {
    char version_str[128];
    int major, minor, patch;
    const char *version = mdb_version(&major, &minor, &patch);

    snprintf(version_str, sizeof(version_str), "%s (%d.%d.%d)",
             version, major, minor, patch);

    return to$str(version_str);
}

B_tuple lmdbQ_libQ__env_create_and_open(B_str path, int64_t max_size) {
    MDB_env *env;
    MDB_dbi dbi;
    MDB_txn *txn;
    int rc;

    // Create environment
    rc = mdb_env_create(&env);
    if (rc != 0) {
        raise_lmdb_error("mdb_env_create", rc);
    }

    // Set map size - this is safe even when reopening an existing database
    // LMDB will use the larger of the specified size and the existing size
    rc = mdb_env_set_mapsize(env, (size_t)max_size);
    if (rc != 0) {
        mdb_env_close(env);
        raise_lmdb_error("mdb_env_set_mapsize", rc);
    }

    // Open environment
    const char* cpath = (const char*)fromB_str(path);
    // Create directory if it doesn't exist
    mkdir(cpath, 0755);

    // Use MDB_NOTLS to avoid thread-local storage, which is fundamentally
    // incompatible with Acton RTS worker and actor scheduling semantics
    rc = mdb_env_open(env, cpath, MDB_NOTLS, 0664);
    if (rc != 0) {
        mdb_env_close(env);
        raise_lmdb_error("mdb_env_open", rc);
    }

    // Open database - use a read-write transaction
    rc = mdb_txn_begin(env, NULL, 0, &txn);
    if (rc != 0) {
        mdb_env_close(env);
        raise_lmdb_error("mdb_txn_begin(open)", rc);
    }

    // Open the default database (NULL name)
    // For the default (unnamed) database, we should always use MDB_CREATE
    // as it's safe to use even if the database already exists
    rc = mdb_dbi_open(txn, NULL, MDB_CREATE, &dbi);
    if (rc != 0) {
        mdb_txn_abort(txn);
        mdb_env_close(env);
        raise_lmdb_error("mdb_dbi_open", rc);
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        mdb_env_close(env);
        raise_lmdb_error("mdb_txn_commit(open)", rc);
    }

    // Get max key size for this environment
    int max_key_size = mdb_env_get_maxkeysize(env);

    // Return (env_ptr, dbi, max_key_size) as tuple
    return $NEWTUPLE(3, to$int((intptr_t)env), to$int((int)dbi), to$int(max_key_size));
}

B_NoneType lmdbQ_libQ__put(int64_t env_ptr, int64_t dbi, B_bytes key, B_bytes value) {
    MDB_env *env = (MDB_env*)(intptr_t)env_ptr;
    MDB_txn *txn;
    MDB_val mkey, mval;
    int rc;

    rc = mdb_txn_begin(env, NULL, 0, &txn);
    if (rc != 0) {
        raise_lmdb_error("mdb_txn_begin(put)", rc);
    }

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;
    mval.mv_size = value->nbytes;
    mval.mv_data = value->str;

    rc = mdb_put(txn, (MDB_dbi)dbi, &mkey, &mval, 0);
    if (rc != 0) {
        mdb_txn_abort(txn);
        raise_lmdb_error("mdb_put", rc);
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        raise_lmdb_error("mdb_txn_commit(put)", rc);
    }

    return B_None;
}

B_bytes lmdbQ_libQ__get(int64_t env_ptr, int64_t dbi, B_bytes key) {
    MDB_env *env = (MDB_env*)(intptr_t)env_ptr;
    MDB_txn *txn;
    MDB_val mkey, mval;
    int rc;

    rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) {
        raise_lmdb_error("mdb_txn_begin(get)", rc);
    }

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;

    rc = mdb_get(txn, (MDB_dbi)dbi, &mkey, &mval);

    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);  // Safe to abort when no data returned
        return (B_bytes)B_None;
    } else if (rc != 0) {
        mdb_txn_abort(txn);  // Safe to abort when error occurred
        raise_lmdb_error("mdb_get", rc);
    }

    // Copy the data BEFORE aborting the transaction
    B_bytes result = to$bytesD_len(mval.mv_data, mval.mv_size);
    mdb_txn_abort(txn);  // Now safe to abort after data is copied
    return result;
}

bool lmdbQ_libQ__delete(int64_t env_ptr, int64_t dbi, B_bytes key) {
    MDB_env *env = (MDB_env*)(intptr_t)env_ptr;
    MDB_txn *txn;
    MDB_val mkey;
    int rc;

    rc = mdb_txn_begin(env, NULL, 0, &txn);
    if (rc != 0) {
        raise_lmdb_error("mdb_txn_begin(delete)", rc);
    }

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;

    rc = mdb_del(txn, (MDB_dbi)dbi, &mkey, NULL);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return false;
    } else if (rc != 0) {
        mdb_txn_abort(txn);
        raise_lmdb_error("mdb_del", rc);
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        raise_lmdb_error("mdb_txn_commit(delete)", rc);
    }

    return true;
}

B_NoneType lmdbQ_libQ__close(int64_t env_ptr, int64_t dbi) {
    MDB_env *env = (MDB_env*)(intptr_t)env_ptr;
    // Force a synchronous flush to disk before closing
    int rc = mdb_env_sync(env, 1);
    if (rc != 0) {
        // Even if sync fails, we still close the environment
        mdb_env_close(env);
        raise_lmdb_error("mdb_env_sync", rc);
    }
    // Note: mdb_dbi_close is typically not needed for the default database
    // and closing the environment closes all databases
    mdb_env_close(env);
    return B_None;
}

// Transaction functions

int64_t lmdbQ_libQ__txn_begin_read(int64_t env_ptr) {
    MDB_env *env = (MDB_env*)(intptr_t)env_ptr;
    MDB_txn *txn;
    int rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) {
        raise_lmdb_error("mdb_txn_begin(read)", rc);
    }
    return (int64_t)(intptr_t)txn;
}

int64_t lmdbQ_libQ__txn_begin_write(int64_t env_ptr) {
    MDB_env *env = (MDB_env*)(intptr_t)env_ptr;
    MDB_txn *txn;
    int rc = mdb_txn_begin(env, NULL, 0, &txn);
    if (rc != 0) {
        raise_lmdb_error("mdb_txn_begin(write)", rc);
    }
    return (int64_t)(intptr_t)txn;
}

B_NoneType lmdbQ_libQ__txn_commit(int64_t txn_ptr) {
    MDB_txn *txn = (MDB_txn*)(intptr_t)txn_ptr;
    int rc = mdb_txn_commit(txn);
    if (rc != 0) {
        raise_lmdb_error("mdb_txn_commit", rc);
    }
    return B_None;
}

B_NoneType lmdbQ_libQ__txn_abort(int64_t txn_ptr) {
    MDB_txn *txn = (MDB_txn*)(intptr_t)txn_ptr;
    mdb_txn_abort(txn);
    return B_None;
}

B_NoneType lmdbQ_libQ__txn_put(int64_t txn_ptr, int64_t dbi, B_bytes key, B_bytes value) {
    MDB_txn *txn = (MDB_txn*)(intptr_t)txn_ptr;
    MDB_val mkey, mval;

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;
    mval.mv_size = value->nbytes;
    mval.mv_data = value->str;

    int rc = mdb_put(txn, (MDB_dbi)dbi, &mkey, &mval, 0);
    if (rc != 0) {
        raise_lmdb_error("mdb_put(txn)", rc);
    }
    return B_None;
}

B_bytes lmdbQ_libQ__txn_get(int64_t txn_ptr, int64_t dbi, B_bytes key) {
    MDB_txn *txn = (MDB_txn*)(intptr_t)txn_ptr;
    MDB_val mkey, mval;

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;

    int rc = mdb_get(txn, (MDB_dbi)dbi, &mkey, &mval);
    if (rc == MDB_NOTFOUND) {
        return (B_bytes)B_None;
    } else if (rc != 0) {
        raise_lmdb_error("mdb_get(txn)", rc);
    }

    // Copy the data to return
    return to$bytesD_len(mval.mv_data, mval.mv_size);
}

bool lmdbQ_libQ__txn_delete(int64_t txn_ptr, int64_t dbi, B_bytes key) {
    MDB_txn *txn = (MDB_txn*)(intptr_t)txn_ptr;
    MDB_val mkey;

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;

    int rc = mdb_del(txn, (MDB_dbi)dbi, &mkey, NULL);
    if (rc == MDB_NOTFOUND) {
        return false;
    } else if (rc != 0) {
        raise_lmdb_error("mdb_del(txn)", rc);
    }
    return true;
}

// Cursor functions

int64_t lmdbQ_libQ__cursor_open(int64_t txn_ptr, int64_t dbi) {
    MDB_txn *txn = (MDB_txn*)(intptr_t)txn_ptr;
    MDB_cursor *cursor;

    int rc = mdb_cursor_open(txn, (MDB_dbi)dbi, &cursor);
    if (rc != 0) {
        raise_lmdb_error("mdb_cursor_open", rc);
    }
    return (int64_t)(intptr_t)cursor;
}

B_NoneType lmdbQ_libQ__cursor_close(int64_t cursor_ptr) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)cursor_ptr;
    if (cursor != NULL) {
        mdb_cursor_close(cursor);
    }
    return B_None;
}

B_tuple lmdbQ_libQ__cursor_first(int64_t cursor_ptr) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)cursor_ptr;
    MDB_val key, val;

    int rc = mdb_cursor_get(cursor, &key, &val, MDB_FIRST);
    if (rc == MDB_NOTFOUND) {
        return (B_tuple)B_None;
    } else if (rc != 0) {
        raise_lmdb_error("mdb_cursor_get(first)", rc);
    }

    return $NEWTUPLE(2, to$bytesD_len(key.mv_data, key.mv_size),
                        to$bytesD_len(val.mv_data, val.mv_size));
}

B_tuple lmdbQ_libQ__cursor_last(int64_t cursor_ptr) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)cursor_ptr;
    MDB_val key, val;

    int rc = mdb_cursor_get(cursor, &key, &val, MDB_LAST);
    if (rc == MDB_NOTFOUND) {
        return (B_tuple)B_None;
    } else if (rc != 0) {
        raise_lmdb_error("mdb_cursor_get(last)", rc);
    }

    return $NEWTUPLE(2, to$bytesD_len(key.mv_data, key.mv_size),
                        to$bytesD_len(val.mv_data, val.mv_size));
}

B_tuple lmdbQ_libQ__cursor_next(int64_t cursor_ptr) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)cursor_ptr;
    MDB_val key, val;

    int rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
    if (rc == MDB_NOTFOUND) {
        return (B_tuple)B_None;
    } else if (rc != 0) {
        raise_lmdb_error("mdb_cursor_get(next)", rc);
    }

    return $NEWTUPLE(2, to$bytesD_len(key.mv_data, key.mv_size),
                        to$bytesD_len(val.mv_data, val.mv_size));
}

B_tuple lmdbQ_libQ__cursor_prev(int64_t cursor_ptr) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)cursor_ptr;
    MDB_val key, val;

    int rc = mdb_cursor_get(cursor, &key, &val, MDB_PREV);
    if (rc == MDB_NOTFOUND) {
        return (B_tuple)B_None;
    } else if (rc != 0) {
        raise_lmdb_error("mdb_cursor_get(prev)", rc);
    }

    return $NEWTUPLE(2, to$bytesD_len(key.mv_data, key.mv_size),
                        to$bytesD_len(val.mv_data, val.mv_size));
}

B_tuple lmdbQ_libQ__cursor_seek(int64_t cursor_ptr, B_bytes seek_key) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)cursor_ptr;
    MDB_val key, val;

    key.mv_size = seek_key->nbytes;
    key.mv_data = seek_key->str;

    // MDB_SET_RANGE positions at key or next key if not found
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);
    if (rc == MDB_NOTFOUND) {
        return (B_tuple)B_None;
    } else if (rc != 0) {
        raise_lmdb_error("mdb_cursor_get(seek)", rc);
    }

    return $NEWTUPLE(2, to$bytesD_len(key.mv_data, key.mv_size),
                        to$bytesD_len(val.mv_data, val.mv_size));
}

B_tuple lmdbQ_libQ__cursor_seek_prefix(int64_t cursor_ptr, B_bytes prefix) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)cursor_ptr;
    MDB_val key, val;

    key.mv_size = prefix->nbytes;
    key.mv_data = prefix->str;

    // MDB_SET_RANGE positions at key or next key if not found
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);
    if (rc == MDB_NOTFOUND) {
        return (B_tuple)B_None;
    } else if (rc != 0) {
        raise_lmdb_error("mdb_cursor_get(seek_prefix)", rc);
    }

    // Check if the key starts with the prefix
    if (key.mv_size >= prefix->nbytes &&
        memcmp(key.mv_data, prefix->str, prefix->nbytes) == 0) {
        return $NEWTUPLE(2, to$bytesD_len(key.mv_data, key.mv_size),
                            to$bytesD_len(val.mv_data, val.mv_size));
    }

    return (B_tuple)B_None;
}

B_NoneType lmdbQ_libQ__cursor_put(int64_t cursor_ptr, B_bytes key, B_bytes value) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)cursor_ptr;
    MDB_val mkey, mval;

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;
    mval.mv_size = value->nbytes;
    mval.mv_data = value->str;

    int rc = mdb_cursor_put(cursor, &mkey, &mval, 0);
    if (rc != 0) {
        raise_lmdb_error("mdb_cursor_put", rc);
    }
    return B_None;
}

B_NoneType lmdbQ_libQ__cursor_delete(int64_t cursor_ptr) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)cursor_ptr;

    int rc = mdb_cursor_del(cursor, 0);
    if (rc != 0) {
        raise_lmdb_error("mdb_cursor_del", rc);
    }
    return B_None;
}
