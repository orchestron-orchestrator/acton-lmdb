#include <lmdb.h>
#include <stdlib.h>
#include <sys/stat.h>

// LMDB normally uses TLS (thread local storage) for some stuff, which is why
// LMDB normally should run on one thread and one thread can only open one LMDB
// database at a time. It's basically as a convenience, like there is nothing to
// clean up on thread destruction since it's in TLS, and I think sometimes as a
// slight optimization. It doesn't really do much for us in Acton since we have
// our own ways of cleaning up and need to interop well with the actor / thread
// semantics, essentially that actors can migrate between threads and a thread
// might run any actor so its ultimately forbidden to rely on TLS - programs
// shouild only rely on actor state!

void lmdbQ___ext_init__() {
}

B_str lmdbQ_version() {
    char version_str[128];
    int major, minor, patch;
    const char *version = mdb_version(&major, &minor, &patch);

    snprintf(version_str, sizeof(version_str), "%s (%d.%d.%d)",
             version, major, minor, patch);

    return to$str(version_str);
}

B_tuple lmdbQ__env_create_and_open(B_str path, B_int max_size) {
    MDB_env *env;
    MDB_dbi dbi;
    MDB_txn *txn;
    int rc;

    // Create environment
    rc = mdb_env_create(&env);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    // Set map size - this is safe even when reopening an existing database
    // LMDB will use the larger of the specified size and the existing size
    rc = mdb_env_set_mapsize(env, (size_t)from$int(max_size));
    if (rc != 0) {
        mdb_env_close(env);
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    // Open environment
    const char* cpath = fromB_str(path);
    // Create directory if it doesn't exist
    mkdir(cpath, 0755);

    // Use MDB_NOTLS to avoid thread-local storage, which is fundamentally
    // incompatible with Acton RTS worker and actor scheduling semantics
    rc = mdb_env_open(env, cpath, MDB_NOTLS, 0664);
    if (rc != 0) {
        mdb_env_close(env);
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    // Open database - use a read-write transaction
    rc = mdb_txn_begin(env, NULL, 0, &txn);
    if (rc != 0) {
        mdb_env_close(env);
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    // Open the default database (NULL name)
    // For the default (unnamed) database, we should always use MDB_CREATE
    // as it's safe to use even if the database already exists
    rc = mdb_dbi_open(txn, NULL, MDB_CREATE, &dbi);
    if (rc != 0) {
        mdb_txn_abort(txn);
        mdb_env_close(env);
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        mdb_env_close(env);
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    // Get max key size for this environment
    int max_key_size = mdb_env_get_maxkeysize(env);

    // Return (env_ptr, dbi, max_key_size) as tuple
    return $NEWTUPLE(3, to$int((intptr_t)env), to$int((int)dbi), to$int(max_key_size));
}

B_NoneType lmdbQ__put(B_int env_ptr, B_int dbi, B_bytes key, B_bytes value) {
    MDB_env *env = (MDB_env*)(intptr_t)from$int(env_ptr);
    MDB_txn *txn;
    MDB_val mkey, mval;
    int rc;

    rc = mdb_txn_begin(env, NULL, 0, &txn);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;
    mval.mv_size = value->nbytes;
    mval.mv_data = value->str;

    rc = mdb_put(txn, (MDB_dbi)from$int(dbi), &mkey, &mval, 0);
    if (rc != 0) {
        mdb_txn_abort(txn);
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    return B_None;
}

B_bytes lmdbQ__get(B_int env_ptr, B_int dbi, B_bytes key) {
    MDB_env *env = (MDB_env*)(intptr_t)from$int(env_ptr);
    MDB_txn *txn;
    MDB_val mkey, mval;
    int rc;

    rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;

    rc = mdb_get(txn, (MDB_dbi)from$int(dbi), &mkey, &mval);

    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);  // Safe to abort when no data returned
        return (B_bytes)B_None;
    } else if (rc != 0) {
        mdb_txn_abort(txn);  // Safe to abort when error occurred
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    // Copy the data BEFORE aborting the transaction
    B_bytes result = to$bytesD_len(mval.mv_data, mval.mv_size);
    mdb_txn_abort(txn);  // Now safe to abort after data is copied
    return result;
}

B_bool lmdbQ__delete(B_int env_ptr, B_int dbi, B_bytes key) {
    MDB_env *env = (MDB_env*)(intptr_t)from$int(env_ptr);
    MDB_txn *txn;
    MDB_val mkey;
    int rc;

    rc = mdb_txn_begin(env, NULL, 0, &txn);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;

    rc = mdb_del(txn, (MDB_dbi)from$int(dbi), &mkey, NULL);
    if (rc == MDB_NOTFOUND) {
        mdb_txn_abort(txn);
        return B_False;
    } else if (rc != 0) {
        mdb_txn_abort(txn);
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    rc = mdb_txn_commit(txn);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    return B_True;
}

B_NoneType lmdbQ__close(B_int env_ptr, B_int dbi) {
    MDB_env *env = (MDB_env*)(intptr_t)from$int(env_ptr);
    // Force a synchronous flush to disk before closing
    int rc = mdb_env_sync(env, 1);
    if (rc != 0) {
        // Even if sync fails, we still close the environment
        mdb_env_close(env);
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }
    // Note: mdb_dbi_close is typically not needed for the default database
    // and closing the environment closes all databases
    mdb_env_close(env);
    return B_None;
}

// Transaction functions

B_int lmdbQ__txn_begin_read(B_int env_ptr) {
    MDB_env *env = (MDB_env*)(intptr_t)from$int(env_ptr);
    MDB_txn *txn;
    int rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }
    return to$int((intptr_t)txn);
}

B_int lmdbQ__txn_begin_write(B_int env_ptr) {
    MDB_env *env = (MDB_env*)(intptr_t)from$int(env_ptr);
    MDB_txn *txn;
    int rc = mdb_txn_begin(env, NULL, 0, &txn);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }
    return to$int((intptr_t)txn);
}

B_NoneType lmdbQ__txn_commit(B_int txn_ptr) {
    MDB_txn *txn = (MDB_txn*)(intptr_t)from$int(txn_ptr);
    int rc = mdb_txn_commit(txn);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }
    return B_None;
}

B_NoneType lmdbQ__txn_abort(B_int txn_ptr) {
    MDB_txn *txn = (MDB_txn*)(intptr_t)from$int(txn_ptr);
    mdb_txn_abort(txn);
    return B_None;
}

B_NoneType lmdbQ__txn_put(B_int txn_ptr, B_int dbi, B_bytes key, B_bytes value) {
    MDB_txn *txn = (MDB_txn*)(intptr_t)from$int(txn_ptr);
    MDB_val mkey, mval;

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;
    mval.mv_size = value->nbytes;
    mval.mv_data = value->str;

    int rc = mdb_put(txn, (MDB_dbi)from$int(dbi), &mkey, &mval, 0);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }
    return B_None;
}

B_bytes lmdbQ__txn_get(B_int txn_ptr, B_int dbi, B_bytes key) {
    MDB_txn *txn = (MDB_txn*)(intptr_t)from$int(txn_ptr);
    MDB_val mkey, mval;

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;

    int rc = mdb_get(txn, (MDB_dbi)from$int(dbi), &mkey, &mval);
    if (rc == MDB_NOTFOUND) {
        return (B_bytes)B_None;
    } else if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    // Copy the data to return
    return to$bytesD_len(mval.mv_data, mval.mv_size);
}

B_bool lmdbQ__txn_delete(B_int txn_ptr, B_int dbi, B_bytes key) {
    MDB_txn *txn = (MDB_txn*)(intptr_t)from$int(txn_ptr);
    MDB_val mkey;

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;

    int rc = mdb_del(txn, (MDB_dbi)from$int(dbi), &mkey, NULL);
    if (rc == MDB_NOTFOUND) {
        return B_False;
    } else if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }
    return B_True;
}

// Cursor functions

B_int lmdbQ__cursor_open(B_int txn_ptr, B_int dbi) {
    MDB_txn *txn = (MDB_txn*)(intptr_t)from$int(txn_ptr);
    MDB_cursor *cursor;

    int rc = mdb_cursor_open(txn, (MDB_dbi)from$int(dbi), &cursor);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }
    return to$int((intptr_t)cursor);
}

B_NoneType lmdbQ__cursor_close(B_int cursor_ptr) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)from$int(cursor_ptr);
    mdb_cursor_close(cursor);
    return B_None;
}

B_tuple lmdbQ__cursor_first(B_int cursor_ptr) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)from$int(cursor_ptr);
    MDB_val key, val;

    int rc = mdb_cursor_get(cursor, &key, &val, MDB_FIRST);
    if (rc == MDB_NOTFOUND) {
        return (B_tuple)B_None;
    } else if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    return $NEWTUPLE(2, to$bytesD_len(key.mv_data, key.mv_size),
                        to$bytesD_len(val.mv_data, val.mv_size));
}

B_tuple lmdbQ__cursor_last(B_int cursor_ptr) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)from$int(cursor_ptr);
    MDB_val key, val;

    int rc = mdb_cursor_get(cursor, &key, &val, MDB_LAST);
    if (rc == MDB_NOTFOUND) {
        return (B_tuple)B_None;
    } else if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    return $NEWTUPLE(2, to$bytesD_len(key.mv_data, key.mv_size),
                        to$bytesD_len(val.mv_data, val.mv_size));
}

B_tuple lmdbQ__cursor_next(B_int cursor_ptr) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)from$int(cursor_ptr);
    MDB_val key, val;

    int rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
    if (rc == MDB_NOTFOUND) {
        return (B_tuple)B_None;
    } else if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    return $NEWTUPLE(2, to$bytesD_len(key.mv_data, key.mv_size),
                        to$bytesD_len(val.mv_data, val.mv_size));
}

B_tuple lmdbQ__cursor_prev(B_int cursor_ptr) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)from$int(cursor_ptr);
    MDB_val key, val;

    int rc = mdb_cursor_get(cursor, &key, &val, MDB_PREV);
    if (rc == MDB_NOTFOUND) {
        return (B_tuple)B_None;
    } else if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    return $NEWTUPLE(2, to$bytesD_len(key.mv_data, key.mv_size),
                        to$bytesD_len(val.mv_data, val.mv_size));
}

B_tuple lmdbQ__cursor_seek(B_int cursor_ptr, B_bytes seek_key) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)from$int(cursor_ptr);
    MDB_val key, val;

    key.mv_size = seek_key->nbytes;
    key.mv_data = seek_key->str;

    // MDB_SET_RANGE positions at key or next key if not found
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);
    if (rc == MDB_NOTFOUND) {
        return (B_tuple)B_None;
    } else if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    return $NEWTUPLE(2, to$bytesD_len(key.mv_data, key.mv_size),
                        to$bytesD_len(val.mv_data, val.mv_size));
}

B_tuple lmdbQ__cursor_seek_prefix(B_int cursor_ptr, B_bytes prefix) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)from$int(cursor_ptr);
    MDB_val key, val;

    key.mv_size = prefix->nbytes;
    key.mv_data = prefix->str;

    // MDB_SET_RANGE positions at key or next key if not found
    int rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);
    if (rc == MDB_NOTFOUND) {
        return (B_tuple)B_None;
    } else if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }

    // Check if the key starts with the prefix
    if (key.mv_size >= prefix->nbytes &&
        memcmp(key.mv_data, prefix->str, prefix->nbytes) == 0) {
        return $NEWTUPLE(2, to$bytesD_len(key.mv_data, key.mv_size),
                            to$bytesD_len(val.mv_data, val.mv_size));
    }

    return (B_tuple)B_None;
}

B_NoneType lmdbQ__cursor_put(B_int cursor_ptr, B_bytes key, B_bytes value) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)from$int(cursor_ptr);
    MDB_val mkey, mval;

    mkey.mv_size = key->nbytes;
    mkey.mv_data = key->str;
    mval.mv_size = value->nbytes;
    mval.mv_data = value->str;

    int rc = mdb_cursor_put(cursor, &mkey, &mval, 0);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }
    return B_None;
}

B_NoneType lmdbQ__cursor_delete(B_int cursor_ptr) {
    MDB_cursor *cursor = (MDB_cursor*)(intptr_t)from$int(cursor_ptr);

    int rc = mdb_cursor_del(cursor, 0);
    if (rc != 0) {
        RAISE(lmdbQ_LMDBError, to$str(mdb_strerror(rc)));
    }
    return B_None;
}
