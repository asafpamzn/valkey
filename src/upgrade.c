#include "server.h"
#include "rdb.h"

/* Forward declaration from cluster.c */
void createDumpPayload(rio *payload, robj *o, robj *key, int dbid);

/* ========================== Upgrade State Management ========================== */

void upgradeInit(void) {
    server.upgrade = NULL;
}

void upgradeFree(void) {
    if (server.upgrade == NULL) return;
    if (server.upgrade->iter) {
        kvstoreIteratorRelease(server.upgrade->iter);
        server.upgrade->iter = NULL;
    }
    zfree(server.upgrade);
    server.upgrade = NULL;
}

static void upgradeAbort(const char *reason) {
    if (server.upgrade == NULL) return;
    serverLog(LL_WARNING, "UPGRADE aborted: %s", reason);
    server.upgrade->state = UPGRADE_STATE_ABORTED;

    /* Release iterator */
    if (server.upgrade->iter) {
        kvstoreIteratorRelease(server.upgrade->iter);
        server.upgrade->iter = NULL;
    }

    /* Re-enable rehashing */
    hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);
}

static const char *upgradeStateStr(int state) {
    switch (state) {
    case UPGRADE_STATE_NONE: return "none";
    case UPGRADE_STATE_SCANNING: return "scanning";
    case UPGRADE_STATE_REPLAY: return "replay";
    case UPGRADE_STATE_DRAINING: return "draining";
    case UPGRADE_STATE_DONE: return "done";
    case UPGRADE_STATE_ABORTED: return "aborted";
    case UPGRADE_STATE_PAUSED: return "paused";
    default: return "unknown";
    }
}

/* ========================== Replication Stream Injection ========================== */

/* Inject a command into the replication buffer (bypasses the replica guard
 * in replicationFeedReplicas). This allows a replica to inject commands
 * for its own sub-replicas. */
static void upgradeInjectCommand(robj **argv, int argc) {
    if (server.repl_backlog == NULL) return;
    prepareReplicasToWrite();

    char aux[LONG_STR_SIZE + 3];
    int len;

    /* *<argc>\r\n */
    aux[0] = '*';
    len = ll2string(aux + 1, sizeof(aux) - 1, argc);
    aux[len + 1] = '\r';
    aux[len + 2] = '\n';
    feedReplicationBuffer(aux, len + 3);

    for (int j = 0; j < argc; j++) {
        long objlen = stringObjectLen(argv[j]);
        aux[0] = '$';
        len = ll2string(aux + 1, sizeof(aux) - 1, objlen);
        aux[len + 1] = '\r';
        aux[len + 2] = '\n';
        feedReplicationBuffer(aux, len + 3);
        feedReplicationBufferWithObject(argv[j]);
        feedReplicationBuffer("\r\n", 2);
    }
}

/* ========================== Key Transfer ========================== */

/* Serialize a key and inject UPGRADE.RESTORE into the replication stream.
 * Returns the number of bytes injected, or 0 if key was skipped. */
static long long upgradeTransferKey(serverDb *db, robj *entry) {
    sds keyname = objectGetKey(entry);
    if (keyname == NULL) return 0;

    /* Check if key is expired */
    mstime_t expire = objectGetExpire(entry);
    if (expire != -1 && expire < mstime()) return 0;

    long long ttl = (expire != -1) ? expire : 0;

    rio payload;
    robj keyobj;
    initStaticStringObject(keyobj, keyname);
    createDumpPayload(&payload, entry, &keyobj, db->id);

    sds serialized = payload.io.buffer.ptr;
    size_t serialized_len = sdslen(serialized);

    robj *argv[5];
    argv[0] = createStringObject("UPGRADE.RESTORE", 15);
    argv[1] = createStringObject(keyname, sdslen(keyname));
    argv[2] = createStringObjectFromLongLong(ttl);
    argv[3] = createStringObject(serialized, serialized_len);
    argv[4] = createStringObjectFromLongLong(db->id);

    upgradeInjectCommand(argv, 5);

    long long bytes = (long long)serialized_len;

    decrRefCount(argv[0]);
    decrRefCount(argv[1]);
    decrRefCount(argv[2]);
    decrRefCount(argv[3]);
    decrRefCount(argv[4]);
    sdsfree(serialized);

    return bytes;
}

/* ========================== Iteration Processing ========================== */

static void upgradeStartNextDb(void) {
    upgradeState *us = server.upgrade;

    /* Release previous iterator */
    if (us->iter) {
        kvstoreIteratorRelease(us->iter);
        us->iter = NULL;
    }

    /* Find next non-NULL db */
    while (us->current_db < server.dbnum) {
        if (server.db[us->current_db] != NULL) {
            us->iter = kvstoreIteratorInit(server.db[us->current_db]->keys, HASHTABLE_ITER_SAFE);
            return;
        }
        us->current_db++;
    }
}

static void upgradeTransitionToReplay(void) {
    upgradeState *us = server.upgrade;

    serverLog(LL_NOTICE,
              "UPGRADE scan complete. Keys transferred: %lld, skipped: %lld, bytes: %lld. Entering replay phase.",
              us->keys_transferred, us->keys_skipped, us->bytes_transferred);

    us->state = UPGRADE_STATE_REPLAY;

    /* Release iterator */
    if (us->iter) {
        kvstoreIteratorRelease(us->iter);
        us->iter = NULL;
    }

    /* Re-enable rehashing */
    hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);
}

/* ========================== Event Loop Integration ========================== */

void upgradeProcessCycle(void) {
    upgradeState *us = server.upgrade;
    if (us == NULL) return;

    switch (us->state) {
    case UPGRADE_STATE_SCANNING: {
        if (us->iter == NULL) {
            /* All dbs done */
            upgradeTransitionToReplay();
            return;
        }

        /* Check target replica health */
        if (us->target_replica == NULL ||
            us->target_replica->flag.close_asap ||
            us->target_replica->flag.close_after_reply) {
            upgradeAbort("target replica disconnected");
            return;
        }

        serverDb *db = server.db[us->current_db];
        int batch = 0;
        void *entry;

        while (batch < UPGRADE_KEYS_PER_CYCLE && kvstoreIteratorNext(us->iter, &entry)) {
            long long bytes = upgradeTransferKey(db, (robj *)entry);
            if (bytes > 0) {
                us->keys_transferred++;
                us->bytes_transferred += bytes;
            } else {
                us->keys_skipped++;
            }
            batch++;
        }

        /* If iterator exhausted, move to next db */
        if (batch < UPGRADE_KEYS_PER_CYCLE) {
            kvstoreIteratorRelease(us->iter);
            us->iter = NULL;
            us->current_db++;

            /* Find next non-NULL db */
            while (us->current_db < server.dbnum) {
                if (server.db[us->current_db] != NULL) {
                    us->iter = kvstoreIteratorInit(server.db[us->current_db]->keys, HASHTABLE_ITER_SAFE);
                    break;
                }
                us->current_db++;
            }

            /* If no more dbs, transition */
            if (us->iter == NULL) {
                upgradeTransitionToReplay();
            }
        }
        break;
    }

    case UPGRADE_STATE_REPLAY:
        /* Primary input is being processed normally now.
         * Transition to DRAINING once the primary querybuf is drained. */
        if (server.primary == NULL || sdslen(server.primary->querybuf) == 0) {
            us->state = UPGRADE_STATE_DRAINING;
            serverLog(LL_NOTICE, "UPGRADE replay complete. Entering drain phase.");
        }
        break;

    case UPGRADE_STATE_DRAINING: {
        if (us->target_replica == NULL || us->target_replica->repl_data == NULL) {
            upgradeAbort("target replica disconnected during drain");
            break;
        }

        long long replica_offset = us->target_replica->repl_data->repl_ack_off;
        long long lag = server.primary_repl_offset - replica_offset;

        if (lag <= 0) {
            serverLog(LL_NOTICE,
                      "UPGRADE complete! Transferred %lld keys (%lld bytes) in %lld ms.",
                      us->keys_transferred, us->bytes_transferred,
                      mstime() - us->start_time);
            us->state = UPGRADE_STATE_DONE;
        }
        break;
    }

    case UPGRADE_STATE_DONE:
    case UPGRADE_STATE_ABORTED:
    case UPGRADE_STATE_PAUSED:
    case UPGRADE_STATE_NONE:
        break;
    }
}

void upgradeCron(void) {
    upgradeState *us = server.upgrade;
    if (us == NULL) return;

    if (us->state == UPGRADE_STATE_SCANNING ||
        us->state == UPGRADE_STATE_REPLAY ||
        us->state == UPGRADE_STATE_DRAINING) {
        if (us->target_replica == NULL) {
            upgradeAbort("target replica disconnected");
        }
    }
}

/* ========================== UPGRADE Command ========================== */

void upgradeCommand(client *c) {
    /* Handle subcommands */
    if (c->argc >= 2 && !strcasecmp(objectGetVal(c->argv[1]), "status")) {
        upgradeState *us = server.upgrade;
        if (us == NULL) {
            addReplyError(c, "No upgrade in progress");
            return;
        }
        addReplyMapLen(c, 7);
        addReplyBulkCString(c, "state");
        addReplyBulkCString(c, upgradeStateStr(us->state));
        addReplyBulkCString(c, "current_db");
        addReplyLongLong(c, us->current_db);
        addReplyBulkCString(c, "keys_transferred");
        addReplyLongLong(c, us->keys_transferred);
        addReplyBulkCString(c, "keys_skipped");
        addReplyLongLong(c, us->keys_skipped);
        addReplyBulkCString(c, "bytes_transferred");
        addReplyLongLong(c, us->bytes_transferred);
        addReplyBulkCString(c, "elapsed_ms");
        addReplyLongLong(c, mstime() - us->start_time);
        addReplyBulkCString(c, "target_client_id");
        addReplyLongLong(c, (long long)us->target_client_id);
        return;
    }

    if (c->argc >= 2 && !strcasecmp(objectGetVal(c->argv[1]), "abort")) {
        if (server.upgrade == NULL) {
            addReplyError(c, "No upgrade in progress");
            return;
        }
        upgradeAbort("user requested abort");
        addReply(c, shared.ok);
        return;
    }

    if (c->argc >= 2 && !strcasecmp(objectGetVal(c->argv[1]), "pause")) {
        if (server.upgrade == NULL || server.upgrade->state != UPGRADE_STATE_SCANNING) {
            addReplyError(c, "No active upgrade scan to pause");
            return;
        }
        server.upgrade->state = UPGRADE_STATE_PAUSED;
        addReply(c, shared.ok);
        return;
    }

    if (c->argc >= 2 && !strcasecmp(objectGetVal(c->argv[1]), "resume")) {
        if (server.upgrade == NULL || server.upgrade->state != UPGRADE_STATE_PAUSED) {
            addReplyError(c, "No paused upgrade to resume");
            return;
        }
        server.upgrade->state = UPGRADE_STATE_SCANNING;
        addReply(c, shared.ok);
        return;
    }

    /* Main UPGRADE <replica-client-id> command */
    if (c->argc < 2) {
        addReplyError(c, "Usage: UPGRADE <replica-client-id> | STATUS | PAUSE | RESUME | ABORT");
        return;
    }

    if (server.upgrade != NULL && server.upgrade->state != UPGRADE_STATE_DONE &&
        server.upgrade->state != UPGRADE_STATE_ABORTED) {
        addReplyError(c, "An upgrade is already in progress. Use UPGRADE ABORT first.");
        return;
    }

    /* Verify replication backlog exists */
    if (server.repl_backlog == NULL) {
        addReplyError(c, "No replication backlog. Is the target replica connected?");
        return;
    }

    /* Parse replica client ID */
    long long client_id;
    if (getLongLongFromObjectOrReply(c, c->argv[1], &client_id, NULL) != C_OK) return;

    /* Find the replica client */
    client *replica = lookupClientByID(client_id);
    if (replica == NULL) {
        addReplyError(c, "No client found with the specified ID");
        return;
    }

    if (!replica->flag.replica) {
        addReplyError(c, "The specified client is not a replica");
        return;
    }

    if (replica->repl_data == NULL || replica->repl_data->repl_state != REPLICA_STATE_ONLINE) {
        addReplyError(c, "The specified replica is not in ONLINE state");
        return;
    }

    /* Free previous upgrade state if any */
    if (server.upgrade != NULL) {
        upgradeFree();
    }

    /* Initialize upgrade state */
    server.upgrade = zcalloc(sizeof(upgradeState));
    server.upgrade->state = UPGRADE_STATE_SCANNING;
    server.upgrade->target_replica = replica;
    server.upgrade->target_client_id = client_id;
    server.upgrade->current_db = 0;
    server.upgrade->iter = NULL;
    server.upgrade->keys_transferred = 0;
    server.upgrade->keys_skipped = 0;
    server.upgrade->bytes_transferred = 0;
    server.upgrade->start_time = mstime();

    /* Disable rehashing */
    hashtableSetResizePolicy(HASHTABLE_RESIZE_FORBID);

    /* Start iteration from first non-NULL db */
    upgradeStartNextDb();

    serverLog(LL_NOTICE,
              "UPGRADE started. Target replica client ID: %lld. Primary input paused, scanning keyspace.",
              client_id);

    addReply(c, shared.ok);
}

/* ========================== UPGRADE.RESTORE Command (new_replica Side) ========================== */

/* UPGRADE.RESTORE <key> <ttl-ms> <serialized-data> <dbid>
 * Internal command received via replication. Only writes the key if it doesn't exist. */
void upgradeRestoreCommand(client *c) {
    if (c->argc != 5) {
        addReplyError(c, "wrong number of arguments for UPGRADE.RESTORE");
        return;
    }

    long long dbid;
    if (getLongLongFromObjectOrReply(c, c->argv[4], &dbid, NULL) != C_OK) return;
    if (dbid < 0 || dbid >= server.dbnum) {
        addReplyError(c, "invalid DB index");
        return;
    }

    serverDb *db = server.db[dbid];
    if (db == NULL) {
        addReplyError(c, "DB does not exist");
        return;
    }

    robj *key = c->argv[1];

    /* NX semantics: skip if key already exists */
    if (lookupKeyWriteWithFlags(db, key, LOOKUP_NOTOUCH | LOOKUP_NOEXPIRE) != NULL) {
        addReply(c, shared.ok);
        return;
    }

    long long ttl;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &ttl, NULL) != C_OK) return;

    /* If absolute TTL is in the past, skip */
    if (ttl > 0 && ttl < mstime()) {
        addReply(c, shared.ok);
        return;
    }

    /* Verify and deserialize payload */
    unsigned char *payload_data = (unsigned char *)objectGetVal(c->argv[3]);
    size_t payload_len = sdslen(objectGetVal(c->argv[3]));
    uint16_t rdbver;

    if (verifyDumpPayload(payload_data, payload_len, &rdbver) == C_ERR) {
        serverLog(LL_WARNING, "UPGRADE.RESTORE: invalid payload for key '%s'",
                  (char *)objectGetVal(key));
        addReplyError(c, "DUMP payload version or checksum are wrong");
        return;
    }

    rio payload;
    rioInitWithBuffer(&payload, objectGetVal(c->argv[3]));

    int type = rdbLoadObjectType(&payload);
    if (type == -1) {
        addReplyError(c, "Bad data format");
        return;
    }

    robj *obj = rdbLoadObject(type, &payload, objectGetVal(key), dbid, NULL, RDBFLAGS_NONE, 0);
    if (obj == NULL) {
        addReplyError(c, "Bad data format");
        return;
    }

    /* Double-check NX after deserialization */
    if (lookupKeyWriteWithFlags(db, key, LOOKUP_NOTOUCH | LOOKUP_NOEXPIRE) != NULL) {
        decrRefCount(obj);
        addReply(c, shared.ok);
        return;
    }

    dbAdd(db, key, &obj);
    if (ttl > 0) {
        setExpire(c, db, key, ttl);
    }

    signalModifiedKey(c, db, key);
    server.dirty++;
    addReply(c, shared.ok);
}
