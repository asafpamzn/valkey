#include "server.h"
#include "rdb.h"
#include "anet.h"
#include <pthread.h>

/* Forward declaration from cluster.c */
void createDumpPayload(rio *payload, robj *o, robj *key, int dbid);

#define UPGRADE_SEND_BUF_SIZE  (1024 * 1024)

/* ========================== State Management ========================== */

void upgradeInit(void) {
    server.upgrade = NULL;
    server.upgrade_recv = NULL;
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

static const char *upgradeStateStr(int state) {
    switch (state) {
    case UPGRADE_STATE_NONE: return "none";
    case UPGRADE_STATE_SCANNING: return "scanning";
    case UPGRADE_STATE_REPLAY: return "replay";
    case UPGRADE_STATE_DRAINING: return "draining";
    case UPGRADE_STATE_DONE: return "done";
    case UPGRADE_STATE_ABORTED: return "aborted";
    default: return "unknown";
    }
}

/* ========================== Low-level I/O ========================== */

#define UPGRADE_IO_TIMEOUT 30000 /* 30 seconds for thread I/O */

static int upgradeWriteAll(connection *conn, const char *buf, size_t len) {
    while (len > 0) {
        ssize_t n = connSyncWrite(conn, (char *)buf, len, UPGRADE_IO_TIMEOUT);
        if (n <= 0) return -1;
        buf += n;
        len -= n;
    }
    return 0;
}

static ssize_t upgradeReadLine(connection *conn, char *buf, size_t maxlen) {
    return connSyncReadLine(conn, buf, maxlen, UPGRADE_IO_TIMEOUT);
}

/* ========================== Buffered Reader ========================== */

#define UPGRADE_READ_BUF_SIZE (16 * 1024)

typedef struct upgradeReader {
    connection *conn;
    char buf[UPGRADE_READ_BUF_SIZE];
    int pos;
    int len;
} upgradeReader;

static void upgradeReaderInit(upgradeReader *r, connection *conn) {
    r->conn = conn;
    r->pos = 0;
    r->len = 0;
}

static int upgradeReaderFill(upgradeReader *r) {
    /* We need a single partial read (not "read exactly N bytes").
     * For TLS: connSyncRead does a single SSL_read (returns 1..size bytes).
     * For TCP: connSyncRead loops until ALL size bytes arrive, so we use
     * read() directly on the blocking socket (returns partial data). */
    ssize_t n;
    if (r->conn->type == connectionTypeTcp()) {
        n = read(r->conn->fd, r->buf, UPGRADE_READ_BUF_SIZE);
    } else {
        n = connSyncRead(r->conn, r->buf, UPGRADE_READ_BUF_SIZE, UPGRADE_IO_TIMEOUT);
    }
    if (n <= 0) return -1;
    r->pos = 0;
    r->len = (int)n;
    return 0;
}

static ssize_t upgradeBufReadLine(upgradeReader *r, char *buf, size_t maxlen) {
    size_t pos = 0;
    while (pos < maxlen - 1) {
        if (r->pos >= r->len) {
            if (upgradeReaderFill(r) == -1) return -1;
        }
        char c = r->buf[r->pos++];
        buf[pos++] = c;
        if (c == '\n') break;
    }
    buf[pos] = '\0';
    return (ssize_t)pos;
}

static int upgradeBufReadExact(upgradeReader *r, char *buf, size_t len) {
    while (len > 0) {
        if (r->pos >= r->len) {
            if (upgradeReaderFill(r) == -1) return -1;
        }
        size_t avail = r->len - r->pos;
        size_t take = avail < len ? avail : len;
        memcpy(buf, r->buf + r->pos, take);
        r->pos += take;
        buf += take;
        len -= take;
    }
    return 0;
}

/* ========================== Sender Thread (m_replica side) ========================== */

typedef struct upgradeSendWorker {
    pthread_t thread;
    int thread_id;
    int total_threads;
    connection *conn;
    long long keys_transferred;
    long long bytes_transferred;
    int error;
    volatile int done;  /* Set atomically by thread before returning */
    char errmsg[256];
} upgradeSendWorker;

static int upgradeSendKey(connection *conn, sds keyname, long long ttl, sds serialized, int dbid, sds *sendbuf) {
    char ttlstr[21], dbidstr[21];
    int ttllen = ll2string(ttlstr, sizeof(ttlstr), ttl);
    int dbidlen = ll2string(dbidstr, sizeof(dbidstr), dbid);

    *sendbuf = sdscatfmt(*sendbuf, "*5\r\n$15\r\nUPGRADE.RESTORE\r\n$%u\r\n",
                         (unsigned)sdslen(keyname));
    *sendbuf = sdscatlen(*sendbuf, keyname, sdslen(keyname));
    *sendbuf = sdscatfmt(*sendbuf, "\r\n$%u\r\n", (unsigned)ttllen);
    *sendbuf = sdscatlen(*sendbuf, ttlstr, ttllen);
    *sendbuf = sdscatfmt(*sendbuf, "\r\n$%u\r\n", (unsigned)sdslen(serialized));
    *sendbuf = sdscatlen(*sendbuf, serialized, sdslen(serialized));
    *sendbuf = sdscatfmt(*sendbuf, "\r\n$%u\r\n", (unsigned)dbidlen);
    *sendbuf = sdscatlen(*sendbuf, dbidstr, dbidlen);
    *sendbuf = sdscatlen(*sendbuf, "\r\n", 2);

    if (upgradeWriteAll(conn, *sendbuf, sdslen(*sendbuf)) == -1) return -1;
    sdsclear(*sendbuf);
    return 0;
}

typedef struct upgradeSendCtx {
    upgradeSendWorker *worker;
    sds sendbuf;
    int dbid;
} upgradeSendCtx;

static void upgradeSendEntryCallback(void *privdata, void *entry) {
    upgradeSendCtx *ctx = (upgradeSendCtx *)privdata;
    upgradeSendWorker *w = ctx->worker;
    robj *obj = (robj *)entry;

    if (w->error) return;

    sds keyname = objectGetKey(obj);
    if (keyname == NULL) return;

    mstime_t expire = objectGetExpire(obj);
    if (expire != -1 && expire < mstime()) return;
    long long ttl = (expire != -1) ? expire : 0;

    rio payload;
    robj keyobj;
    initStaticStringObject(keyobj, keyname);
    createDumpPayload(&payload, obj, &keyobj, ctx->dbid);

    sds serialized = payload.io.buffer.ptr;

    if (upgradeSendKey(w->conn, keyname, ttl, serialized, ctx->dbid, &ctx->sendbuf) == -1) {
        snprintf(w->errmsg, sizeof(w->errmsg), "write error: %s", strerror(errno));
        w->error = 1;
        sdsfree(serialized);
        return;
    }

    w->bytes_transferred += sdslen(serialized);
    w->keys_transferred++;
    sdsfree(serialized);
}

static void *upgradeSendWorkerMain(void *arg) {
    upgradeSendWorker *w = (upgradeSendWorker *)arg;
    upgradeSendCtx ctx = { .worker = w, .sendbuf = sdsempty(), .dbid = 0 };

    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        serverDb *db = server.db[dbid];
        if (db == NULL) continue;

        hashtable *ht = kvstoreGetHashtable(db->keys, 0);
        if (ht == NULL || hashtableSize(ht) == 0) continue;

        if (w->thread_id == 0) {
            serverLog(LL_NOTICE, "UPGRADE sender[0]: db%d has %zu keys, %zu buckets",
                      dbid, hashtableSize(ht), hashtableNumBuckets(ht));
        }

        ctx.dbid = dbid;
        hashtableIterateBucketRange(ht, w->total_threads, w->thread_id,
                                    upgradeSendEntryCallback, &ctx);
        if (w->error) break;
    }

    if (!w->error && sdslen(ctx.sendbuf) > 0) {
        if (upgradeWriteAll(w->conn, ctx.sendbuf, sdslen(ctx.sendbuf)) == -1) {
            snprintf(w->errmsg, sizeof(w->errmsg), "flush error: %s", connGetLastError(w->conn));
            w->error = 1;
        }
    }
    sdsfree(ctx.sendbuf);

    if (!w->error) {
        const char *done_cmd = "*1\r\n$12\r\nUPGRADE.DONE\r\n";
        if (upgradeWriteAll(w->conn, done_cmd, strlen(done_cmd)) == -1) {
            w->error = 1;
        }
    }
    rdbFreeLzfThreadBuffer();
    atomic_store_explicit((_Atomic int *)&w->done, 1, memory_order_release);
    return NULL;
}

/* ========================== Receiver Thread (new_replica side) ========================== */

typedef struct upgradeRecvThreadArg {
    connection *conn;
    int thread_id;
    long long *keys_inserted;
    long long *bytes_received;
    long long *keys_per_db;  /* Array of size server.dbnum — per-db counts */
    int *done_flag;
    int *error_flag;
    pthread_mutex_t *insert_mutex;
} upgradeRecvThreadArg;

static sds upgradeRecvReadBulk(upgradeReader *r, int len) {
    sds s = sdsnewlen(NULL, len);
    if (upgradeBufReadExact(r, s, len) == -1) {
        sdsfree(s);
        return NULL;
    }
    char crlf[2];
    if (upgradeBufReadExact(r, crlf, 2) == -1) {
        sdsfree(s);
        return NULL;
    }
    return s;
}

static void *upgradeRecvWorkerMain(void *arg) {
    upgradeRecvThreadArg *targ = (upgradeRecvThreadArg *)arg;
    upgradeReader reader;
    upgradeReaderInit(&reader, targ->conn);
    char linebuf[256];

    while (1) {
        if (upgradeBufReadLine(&reader, linebuf, sizeof(linebuf)) <= 0) { serverLog(LL_WARNING, "UPGRADE recv thread %d: readLine argc failed", targ->thread_id); *targ->error_flag = 1; break; }
        if (linebuf[0] != '*') { serverLog(LL_WARNING, "UPGRADE recv thread %d: expected '*', got '%c' (0x%02x), line='%.40s'", targ->thread_id, linebuf[0], (unsigned char)linebuf[0], linebuf); *targ->error_flag = 1; break; }
        int argc = atoi(linebuf + 1);

        /* Read command name */
        if (upgradeBufReadLine(&reader, linebuf, sizeof(linebuf)) <= 0) { serverLog(LL_WARNING, "UPGRADE recv thread %d: readLine cmdlen failed", targ->thread_id); *targ->error_flag = 1; break; }
        int cmdlen = atoi(linebuf + 1);
        char cmdname[64];
        if (cmdlen >= (int)sizeof(cmdname)) { serverLog(LL_WARNING, "UPGRADE recv thread %d: cmdlen %d >= 64", targ->thread_id, cmdlen); *targ->error_flag = 1; break; }
        if (upgradeBufReadExact(&reader, cmdname, cmdlen) == -1) { serverLog(LL_WARNING, "UPGRADE recv thread %d: readExact cmdname failed", targ->thread_id); *targ->error_flag = 1; break; }
        cmdname[cmdlen] = '\0';
        char crlf[2];
        if (upgradeBufReadExact(&reader, crlf, 2) == -1) { serverLog(LL_WARNING, "UPGRADE recv thread %d: readExact crlf failed", targ->thread_id); *targ->error_flag = 1; break; }

        if (argc == 1 && !strcasecmp(cmdname, "UPGRADE.DONE")) {
            /* Thread 0 continues for delta phase; others exit */
            if (targ->thread_id != 0) break;
            /* Thread 0: continue reading delta commands until UPGRADE.REPLINFO */
            continue;
        }

        /* UPGRADE.REPLINFO <replid> <offset> — end of delta (thread 0 only) */
        if (argc == 3 && !strcasecmp(cmdname, "UPGRADE.REPLINFO")) {
            /* Read replid */
            if (upgradeBufReadLine(&reader, linebuf, sizeof(linebuf)) <= 0) { *targ->error_flag = 1; break; }
            int replidlen = atoi(linebuf + 1);
            char replidbuf[CONFIG_RUN_ID_SIZE + 1];
            if (replidlen > CONFIG_RUN_ID_SIZE || upgradeBufReadExact(&reader, replidbuf, replidlen) == -1) { *targ->error_flag = 1; break; }
            replidbuf[replidlen] = '\0';
            char crlf_tmp[2];
            if (upgradeBufReadExact(&reader, crlf_tmp, 2) == -1) { *targ->error_flag = 1; break; }

            /* Read offset */
            if (upgradeBufReadLine(&reader, linebuf, sizeof(linebuf)) <= 0) { *targ->error_flag = 1; break; }
            int offlen = atoi(linebuf + 1);
            char offbuf[32];
            if (offlen >= (int)sizeof(offbuf) || upgradeBufReadExact(&reader, offbuf, offlen) == -1) { *targ->error_flag = 1; break; }
            offbuf[offlen] = '\0';
            if (upgradeBufReadExact(&reader, crlf_tmp, 2) == -1) { *targ->error_flag = 1; break; }

            /* Store replid and offset for PSYNC later */
            memcpy(server.replid, replidbuf, CONFIG_RUN_ID_SIZE + 1);
            server.primary_repl_offset = atoll(offbuf);
            serverLog(LL_NOTICE, "UPGRADE: received REPLINFO replid=%.40s offset=%lld",
                      server.replid, (long long)server.primary_repl_offset);
            break;
        }

        if (argc != 5 || strcasecmp(cmdname, "UPGRADE.RESTORE") != 0) {
            /* Delta phase: raw RESP commands from Primary (SET, DEL, etc.) */
            for (int a = 1; a < argc; a++) {
                if (upgradeBufReadLine(&reader, linebuf, sizeof(linebuf)) <= 0) { *targ->error_flag = 1; break; }
                int bulklen = atoi(linebuf + 1);
                if (bulklen >= 0) {
                    sds discard = upgradeRecvReadBulk(&reader, bulklen);
                    if (discard) sdsfree(discard); else { *targ->error_flag = 1; break; }
                }
            }
            if (*targ->error_flag) break;
            continue;
        }

        /* Read key */
        if (upgradeBufReadLine(&reader, linebuf, sizeof(linebuf)) <= 0) { *targ->error_flag = 1; break; }
        int keylen = atoi(linebuf + 1);
        sds key = upgradeRecvReadBulk(&reader, keylen);
        if (!key) { *targ->error_flag = 1; break; }

        /* Read ttl */
        if (upgradeBufReadLine(&reader, linebuf, sizeof(linebuf)) <= 0) { sdsfree(key); *targ->error_flag = 1; break; }
        int ttllen = atoi(linebuf + 1);
        char ttlbuf[32];
        if (ttllen >= (int)sizeof(ttlbuf)) { sdsfree(key); *targ->error_flag = 1; break; }
        if (upgradeBufReadExact(&reader, ttlbuf, ttllen) == -1) { sdsfree(key); *targ->error_flag = 1; break; }
        ttlbuf[ttllen] = '\0';
        if (upgradeBufReadExact(&reader, crlf, 2) == -1) { sdsfree(key); *targ->error_flag = 1; break; }
        long long ttl = strtoll(ttlbuf, NULL, 10);

        /* Read serialized payload */
        if (upgradeBufReadLine(&reader, linebuf, sizeof(linebuf)) <= 0) { sdsfree(key); *targ->error_flag = 1; break; }
        int datalen = atoi(linebuf + 1);
        sds data = upgradeRecvReadBulk(&reader, datalen);
        if (!data) { sdsfree(key); *targ->error_flag = 1; break; }

        /* Read dbid */
        if (upgradeBufReadLine(&reader, linebuf, sizeof(linebuf)) <= 0) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }
        int dbidlen = atoi(linebuf + 1);
        char dbidbuf[16];
        if (dbidlen >= (int)sizeof(dbidbuf)) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }
        if (upgradeBufReadExact(&reader, dbidbuf, dbidlen) == -1) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }
        dbidbuf[dbidlen] = '\0';
        if (upgradeBufReadExact(&reader, crlf, 2) == -1) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }
        int dbid = atoi(dbidbuf);

        if (dbid < 0 || dbid >= server.dbnum || server.db[dbid] == NULL) {
            sdsfree(key); sdsfree(data);
            continue;
        }

        /* Verify and deserialize */
        uint16_t rdbver;
        if (verifyDumpPayload((unsigned char *)data, sdslen(data), &rdbver) == C_ERR) {
            serverLog(LL_WARNING, "UPGRADE recv thread %d: verifyDumpPayload failed for key '%.40s' datalen=%zu",
                      targ->thread_id, key, sdslen(data));
            sdsfree(key); sdsfree(data);
            *targ->error_flag = 1;
            break;
        }

        rio payload;
        rioInitWithBuffer(&payload, data);
        int type = rdbLoadObjectType(&payload);
        if (type == -1) { serverLog(LL_WARNING, "UPGRADE recv thread %d: rdbLoadObjectType failed for key '%.40s' datalen=%zu", targ->thread_id, key, sdslen(data)); sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }

        int rdb_error = 0;
        robj *obj = rdbLoadObject(type, &payload, key, dbid, &rdb_error, RDBFLAGS_NONE, 0);
        if (obj == NULL) { serverLog(LL_WARNING, "UPGRADE recv thread %d: rdbLoadObject failed for key '%.40s' type=%d datalen=%zu rdb_error=%d", targ->thread_id, key, type, sdslen(data), rdb_error); sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }

        serverDb *db = server.db[dbid];
        int dict_index = server.cluster_enabled ? getKeySlot(key) : 0;
        obj = objectSetKeyAndExpire(obj, key, ttl > 0 ? ttl : -1);
        initObjectLRUOrLFU(obj);

        pthread_mutex_lock(targ->insert_mutex);
        if (kvstoreHashtableAdd(db->keys, dict_index, obj)) {
            (*targ->keys_inserted)++;
            (*targ->bytes_received) += sdslen(data);
            targ->keys_per_db[dbid]++;
        } else {
            decrRefCount(obj);
        }
        pthread_mutex_unlock(targ->insert_mutex);

        sdsfree(key);
        sdsfree(data);
    }

    *targ->done_flag = 1;
    zfree(targ);
    return NULL;
}

/* ========================== UPGRADE Command (runs on new_replica) ========================== */

/* Check if all databases are empty */
static int upgradeIsNodeEmpty(void) {
    for (int i = 0; i < server.dbnum; i++) {
        if (server.db[i] != NULL && kvstoreSize(server.db[i]->keys) > 0) {
            return 0;
        }
    }
    return 1;
}

/* UPGRADE <m_replica_host> <m_replica_port> <primary_host> <primary_port>
 * Pulls all keys from m_replica using 10 parallel threads.
 * Must be run on an empty node (new_replica). */
void upgradeCommand(client *c) {
    /* Subcommand: STATUS */
    if (c->argc >= 2 && !strcasecmp(objectGetVal(c->argv[1]), "status")) {
        upgradeState *us = server.upgrade;
        if (us == NULL) {
            addReplyError(c, "No upgrade in progress");
            return;
        }
        addReplyMapLen(c, 6);
        addReplyBulkCString(c, "state");
        addReplyBulkCString(c, upgradeStateStr(us->state));
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

    /* Main command: UPGRADE <m_replica_host> <m_replica_port> <primary_host> <primary_port> [THREADS <N>] */
    if (c->argc < 5) {
        addReplyError(c, "Usage: UPGRADE <m_replica_host> <m_replica_port> <primary_host> <primary_port> [THREADS <N>] | STATUS");
        return;
    }

    /* Pre-condition: node must be empty */
    if (!upgradeIsNodeEmpty()) {
        addReplyError(c, "UPGRADE can only run on an empty node (no keys in any database)");
        return;
    }

    char *m_host = objectGetVal(c->argv[1]);
    long long m_port_ll;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &m_port_ll, NULL) != C_OK) return;
    int m_port = (int)m_port_ll;

    char *p_host = objectGetVal(c->argv[3]);
    long long p_port_ll;
    if (getLongLongFromObjectOrReply(c, c->argv[4], &p_port_ll, NULL) != C_OK) return;
    int p_port = (int)p_port_ll;

    /* Parse optional THREADS <N> */
    int num_threads = UPGRADE_DEFAULT_THREADS;
    for (int j = 5; j < c->argc; j++) {
        if (!strcasecmp(objectGetVal(c->argv[j]), "threads") && j + 1 < c->argc) {
            long long threads_ll;
            if (getLongLongFromObjectOrReply(c, c->argv[j + 1], &threads_ll, NULL) != C_OK) return;
            if (threads_ll < 1 || threads_ll > UPGRADE_MAX_THREADS) {
                addReplyError(c, "THREADS must be between 1 and 64");
                return;
            }
            num_threads = (int)threads_ll;
            j++;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    /* Initialize state */
    if (server.upgrade != NULL) upgradeFree();
    server.upgrade = zcalloc(sizeof(upgradeState));
    server.upgrade->state = UPGRADE_STATE_SCANNING;
    server.upgrade->start_time = mstime();

    connection **conns = zmalloc(sizeof(connection *) * num_threads);

    /* Open connections to m_replica */
    serverLog(LL_NOTICE, "UPGRADE: connecting to m_replica %s:%d with %d threads...", m_host, m_port, num_threads);

    /* Phase 1: Handshake — get metadata from m_replica to pre-size hashtables */
    {
        serverLog(LL_NOTICE, "UPGRADE: Phase 1 - creating connection (type=%s)...",
                  connTypeOfReplication() == connectionTypeTcp() ? "tcp" : "tls");
        connection *initconn = connCreate(connTypeOfReplication());
        if (connBlockingConnect(initconn, m_host, m_port, 5000) != C_OK) {
            serverLog(LL_WARNING, "UPGRADE: Phase 1 connBlockingConnect failed: %s", connGetLastError(initconn));
            connClose(initconn);
            server.upgrade->state = UPGRADE_STATE_ABORTED;
            addReplyError(c, "Failed to connect to m_replica for handshake");
            return;
        }
        anetBlock(NULL, initconn->fd);
        serverLog(LL_NOTICE, "UPGRADE: Phase 1 - connected, sending UPGRADE.INIT...");

        /* Send UPGRADE.INIT */
        const char *init_cmd = "*1\r\n$12\r\nUPGRADE.INIT\r\n";
        if (upgradeWriteAll(initconn, init_cmd, strlen(init_cmd)) == -1) { connClose(initconn); server.upgrade->state = UPGRADE_STATE_ABORTED; addReplyError(c, "Handshake write error"); return; }
        serverLog(LL_NOTICE, "UPGRADE: Phase 1 - UPGRADE.INIT sent, reading response...");

        /* Read response: *<num_dbs>\r\n then for each db: :<keycount>\r\n */
        char linebuf[256];
        if (upgradeReadLine(initconn, linebuf, sizeof(linebuf)) <= 0 || linebuf[0] != '*') { serverLog(LL_WARNING, "UPGRADE: Phase 1 readLine failed"); connClose(initconn); server.upgrade->state = UPGRADE_STATE_ABORTED; addReplyError(c, "Handshake read error"); return; }
        int num_dbs_reported = atoi(linebuf + 1);

        for (int dbid = 0; dbid < num_dbs_reported; dbid++) {
            if (upgradeReadLine(initconn, linebuf, sizeof(linebuf)) <= 0) { connClose(initconn); server.upgrade->state = UPGRADE_STATE_ABORTED; addReplyError(c, "Handshake read error"); return; }
            long long num_buckets = atoll(linebuf + 1);
            if (num_buckets > 0 && dbid < server.dbnum) {
                if (server.db[dbid] == NULL) {
                    server.db[dbid] = createDatabase(dbid);
                }
                kvstoreHashtableExpand(server.db[dbid]->keys, 0, num_buckets * 7);
            }
        }

        connClose(initconn);
        serverLog(LL_NOTICE, "UPGRADE: handshake done. Pre-sized hashtables for %d dbs.", num_dbs_reported);
    }

    /* Phase 2: Open channels */
    serverLog(LL_NOTICE, "UPGRADE: Phase 2 - opening %d channels...", num_threads);
    for (int i = 0; i < num_threads; i++) {
        conns[i] = connCreate(connTypeOfReplication());
        if (connBlockingConnect(conns[i], m_host, m_port, 5000) != C_OK) {
            serverLog(LL_WARNING, "UPGRADE: connect to %s:%d failed: %s", m_host, m_port, connGetLastError(conns[i]));
            for (int j = 0; j <= i; j++) connClose(conns[j]);
            zfree(conns);
            server.upgrade->state = UPGRADE_STATE_ABORTED;
            addReplyError(c, "Failed to connect to m_replica");
            return;
        }

        anetBlock(NULL, conns[i]->fd);
        anetEnableTcpNoDelay(NULL, conns[i]->fd);

        /* Send UPGRADE.CHANNEL handshake */
        serverLog(LL_NOTICE, "UPGRADE: Phase 2 - channel %d connected, sending handshake...", i);
        char idbuf[21], threadsbuf[21];
        int idlen = snprintf(idbuf, sizeof(idbuf), "%d", i);
        int threadslen = snprintf(threadsbuf, sizeof(threadsbuf), "%d", num_threads);
        char cmd[256];
        int len = snprintf(cmd, sizeof(cmd),
                           "*3\r\n$15\r\nUPGRADE.CHANNEL\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                           idlen, idbuf, threadslen, threadsbuf);
        if (upgradeWriteAll(conns[i], cmd, len) == -1) {
            for (int j = 0; j <= i; j++) connClose(conns[j]);
            zfree(conns);
            server.upgrade->state = UPGRADE_STATE_ABORTED;
            addReplyError(c, "Failed to send UPGRADE.CHANNEL");
            return;
        }

        /* Read +OK */
        serverLog(LL_NOTICE, "UPGRADE: Phase 2 - channel %d handshake sent, waiting for +OK...", i);
        char buf[64];
        if (upgradeReadLine(conns[i], buf, sizeof(buf)) <= 0 || buf[0] != '+') {
            serverLog(LL_WARNING, "UPGRADE: channel %d handshake failed: %.40s", i, buf);
            for (int j = 0; j <= i; j++) connClose(conns[j]);
            zfree(conns);
            server.upgrade->state = UPGRADE_STATE_ABORTED;
            addReplyError(c, "UPGRADE.CHANNEL handshake failed");
            return;
        }
    }

    serverLog(LL_NOTICE, "UPGRADE: all %d channels connected. Starting receiver threads.", num_threads);

    /* Disable rehashing on receiver */
    hashtableSetResizePolicy(HASHTABLE_RESIZE_FORBID);

    /* Force-initialize the crc64 combine cache before spawning threads.
     * crc64_combine uses a lazily-initialized static table that is not
     * thread-safe. Computing a CRC over >1024 bytes triggers initialization. */
    {
        char dummy[2048];
        memset(dummy, 0, sizeof(dummy));
        crc64(0, (unsigned char *)dummy, sizeof(dummy));
    }

    /* Spawn receiver threads */
    pthread_t *threads = zmalloc(sizeof(pthread_t) * num_threads);
    long long *keys_inserted = zcalloc(sizeof(long long) * num_threads);
    long long *bytes_received = zcalloc(sizeof(long long) * num_threads);
    long long **keys_per_db = zmalloc(sizeof(long long *) * num_threads);
    int *thread_done = zcalloc(sizeof(int) * num_threads);
    int *thread_error = zcalloc(sizeof(int) * num_threads);
    pthread_mutex_t insert_mutex = PTHREAD_MUTEX_INITIALIZER;

    for (int i = 0; i < num_threads; i++) {
        keys_per_db[i] = zcalloc(sizeof(long long) * server.dbnum);
        upgradeRecvThreadArg *arg = zmalloc(sizeof(upgradeRecvThreadArg));
        arg->conn = conns[i];
        arg->thread_id = i;
        arg->keys_inserted = &keys_inserted[i];
        arg->bytes_received = &bytes_received[i];
        arg->keys_per_db = keys_per_db[i];
        arg->done_flag = &thread_done[i];
        arg->error_flag = &thread_error[i];
        arg->insert_mutex = &insert_mutex;

        if (pthread_create(&threads[i], NULL, upgradeRecvWorkerMain, arg) != 0) {
            serverLog(LL_WARNING, "UPGRADE: pthread_create failed for thread %d", i);
            thread_error[i] = 1;
            thread_done[i] = 1;
            zfree(arg);
        }
    }

    /* Wait for all threads */
    long long total_keys = 0;
    long long total_bytes = 0;
    int had_error = 0;
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        total_keys += keys_inserted[i];
        total_bytes += bytes_received[i];
        /* Thread 0 may report "error" if REPLINFO read fails (race with close).
         * Only treat as real error if it also inserted 0 keys. */
        if (thread_error[i] && i != 0) had_error = 1;
    }
    pthread_mutex_destroy(&insert_mutex);

    /* Aggregate per-db counts across all threads */
    long long *db_key_counts = zcalloc(sizeof(long long) * server.dbnum);
    for (int i = 0; i < num_threads; i++) {
        for (int d = 0; d < server.dbnum; d++) {
            db_key_counts[d] += keys_per_db[i][d];
        }
        zfree(keys_per_db[i]);
    }
    zfree(keys_per_db);

    /* Close connections and free arrays */
    for (int i = 0; i < num_threads; i++) {
        connClose(conns[i]);
    }
    zfree(conns);
    zfree(threads);
    zfree(keys_inserted);
    zfree(bytes_received);
    zfree(thread_done);
    zfree(thread_error);

    /* Re-enable rehashing */
    hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);

    server.upgrade->keys_transferred = total_keys;
    server.upgrade->bytes_transferred = total_bytes;
    server.dirty += total_keys;

    /* Fix ht->used[0] for each db (racy from concurrent thread increments) */
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        if (db_key_counts[dbid] == 0) continue;
        serverDb *db = server.db[dbid];
        if (db == NULL) continue;
        hashtable *ht = kvstoreGetHashtable(db->keys, 0);
        if (ht == NULL) continue;
        hashtableSetUsedCount(ht, db_key_counts[dbid]);
    }
    zfree(db_key_counts);

    /* Register expires */
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        serverDb *db = server.db[dbid];
        if (db == NULL) continue;
        hashtable *ht = kvstoreGetHashtable(db->keys, 0);
        if (ht == NULL) continue;

        kvstoreIterator *it = kvstoreIteratorInit(db->keys, HASHTABLE_ITER_SAFE);
        void *entry;
        while (kvstoreIteratorNext(it, &entry)) {
            robj *obj = (robj *)entry;
            mstime_t expire = objectGetExpire(obj);
            if (expire != -1) {
                kvstoreHashtableAdd(db->expires, 0, obj);
            }
        }
        kvstoreIteratorRelease(it);
    }

    if (had_error) {
        server.upgrade->state = UPGRADE_STATE_ABORTED;
        serverLog(LL_WARNING, "UPGRADE: completed with errors. %lld keys inserted.", total_keys);
        addReplyError(c, "UPGRADE completed with errors");
        return;
    }

    serverLog(LL_NOTICE, "UPGRADE: transfer complete. %lld keys in %lld ms. replid=%.40s offset=%lld",
              total_keys, mstime() - server.upgrade->start_time,
              server.replid, (long long)server.primary_repl_offset);

    /* Set up replication to Primary using the replid+offset from REPLINFO.
     * replicationCachePrimaryUsingMyself() creates a cached_primary with our
     * current server.replid and server.primary_repl_offset, which will be used
     * by PSYNC when connecting to Primary. */
    replicationCachePrimaryUsingMyself();
    replicationSetPrimary(p_host, p_port, 0, false);
    serverLog(LL_NOTICE, "UPGRADE: REPLICAOF %s:%d configured. Will PSYNC with replid=%.40s offset=%lld.",
              p_host, p_port, server.replid, (long long)server.primary_repl_offset);

    server.upgrade->state = UPGRADE_STATE_DONE;

    addReply(c, shared.ok);
}

/* ========================== UPGRADE.INIT (m_replica side — handshake) ========================== */

/* Returns metadata about all databases: bucket counts for exact pre-sizing.
 * Response format: *<num_dbs>\r\n:<bucket_count_db0>\r\n:<bucket_count_db1>\r\n...
 * new_replica uses these to create hashtables with the EXACT same number of buckets,
 * ensuring bucket_idx maps identically on both sides (no contention during parallel insert). */
void upgradeInitCommand(client *c) {
    addReplyArrayLen(c, server.dbnum);
    for (int i = 0; i < server.dbnum; i++) {
        if (server.db[i] != NULL) {
            hashtable *ht = kvstoreGetHashtable(server.db[i]->keys, 0);
            if (ht != NULL) {
                addReplyLongLong(c, (long long)hashtableNumBuckets(ht));
            } else {
                addReplyLongLong(c, 0);
            }
        } else {
            addReplyLongLong(c, 0);
        }
    }
}

/* ========================== UPGRADE.CHANNEL (m_replica side) ========================== */

void upgradeChannelCommand(client *c) {
    if (c->argc != 3) {
        addReplyError(c, "wrong number of arguments for UPGRADE.CHANNEL");
        return;
    }

    long long thread_id, total_threads;
    if (getLongLongFromObjectOrReply(c, c->argv[1], &thread_id, NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &total_threads, NULL) != C_OK) return;

    if (total_threads < 1 || total_threads > UPGRADE_MAX_THREADS) {
        addReplyError(c, "invalid total_threads");
        return;
    }
    if (thread_id < 0 || thread_id >= total_threads) {
        addReplyError(c, "invalid thread_id");
        return;
    }

    /* Initialize receiver state if first channel */
    if (server.upgrade_recv == NULL) {
        server.upgrade_recv = zcalloc(sizeof(upgradeRecvState));
        server.upgrade_recv->total_threads = (int)total_threads;
    }

    upgradeRecvState *rs = server.upgrade_recv;

    if ((int)total_threads != rs->total_threads) {
        addReplyError(c, "total_threads mismatch");
        return;
    }

    /* Store connection — we'll use the client's connection directly */
    rs->conns[thread_id] = c->conn;
    rs->clients[thread_id] = c;
    rs->channels_registered++;

    /* Reply OK immediately via direct write (bypass buffered I/O) */
    const char *ok_reply = "+OK\r\n";
    ssize_t nw = connSyncWrite(c->conn, (char *)ok_reply, 5, 5000);
    serverLog(LL_NOTICE, "UPGRADE.CHANNEL: thread %lld replied +OK (nw=%zd)", thread_id, nw);

    /* Disable event loop on this client — prevent any further I/O */
    connSetReadHandler(c->conn, NULL);
    connSetWriteHandler(c->conn, NULL);
    c->flag.pending_write = 0;

    /* If all channels registered, start sending */
    if (rs->channels_registered == rs->total_threads) {
        serverLog(LL_NOTICE, "UPGRADE.CHANNEL: all %d channels registered. Freezing and sending.", rs->total_threads);

        /* Freeze: pause primary input */
        /* (guard in processInputBuffer handles this via server.upgrade_recv) */

        /* Capture replication offset at scan start for REPLINFO */
        rs->snapshot_repl_offset = server.primary_repl_offset;

        /* Disable rehashing during threaded scan.
         * Also set dict_resizing=0 to prevent serverCron's updateDictResizePolicy()
         * from re-enabling rehashing while sender threads iterate the hashtable. */
        server.dict_resizing = 0;
        hashtableSetResizePolicy(HASHTABLE_RESIZE_FORBID);

        /* Make connections blocking and disable Nagle */
        char err[ANET_ERR_LEN];
        for (int i = 0; i < rs->total_threads; i++) {
            anetBlock(err, rs->conns[i]->fd);
            anetEnableTcpNoDelay(err, rs->conns[i]->fd);
        }

        /* Force-initialize the crc64 combine cache before spawning threads.
         * crc64_combine uses a lazily-initialized static table that is not
         * thread-safe. Computing a CRC over >1024 bytes triggers the dual-split
         * path which calls crc64_combine, initializing the cache. */
        {
            char dummy[2048];
            memset(dummy, 0, sizeof(dummy));
            crc64(0, (unsigned char *)dummy, sizeof(dummy));
        }

        /* Spawn sender threads */
        upgradeSendWorker *workers = zcalloc(sizeof(upgradeSendWorker) * rs->total_threads);
        for (int i = 0; i < rs->total_threads; i++) {
            workers[i].thread_id = i;
            workers[i].total_threads = rs->total_threads;
            workers[i].conn = rs->conns[i];
        }

        rs->workers = workers;
        rs->sending = 1;

        pthread_attr_t tattr;
        pthread_attr_init(&tattr);
        pthread_attr_setstacksize(&tattr, 1 << 21); /* 2MB */
        for (int i = 0; i < rs->total_threads; i++) {
            pthread_create(&workers[i].thread, &tattr, upgradeSendWorkerMain, &workers[i]);
        }
        pthread_attr_destroy(&tattr);

        serverLog(LL_NOTICE, "UPGRADE.CHANNEL: sender threads launched. Event loop continues.");
    }
}

/* ========================== UPGRADE.RESTORE (fallback for single-thread path) ========================== */

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
    if (lookupKeyWriteWithFlags(db, key, LOOKUP_NOTOUCH | LOOKUP_NOEXPIRE) != NULL) {
        addReply(c, shared.ok);
        return;
    }

    long long ttl;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &ttl, NULL) != C_OK) return;
    if (ttl > 0 && ttl < mstime()) { addReply(c, shared.ok); return; }

    unsigned char *payload_data = (unsigned char *)objectGetVal(c->argv[3]);
    size_t payload_len = sdslen(objectGetVal(c->argv[3]));
    uint16_t rdbver;

    if (verifyDumpPayload(payload_data, payload_len, &rdbver) == C_ERR) {
        addReplyError(c, "DUMP payload version or checksum are wrong");
        return;
    }

    rio payload;
    rioInitWithBuffer(&payload, objectGetVal(c->argv[3]));
    int type = rdbLoadObjectType(&payload);
    if (type == -1) { addReplyError(c, "Bad data format"); return; }

    robj *obj = rdbLoadObject(type, &payload, objectGetVal(key), dbid, NULL, RDBFLAGS_NONE, 0);
    if (obj == NULL) { addReplyError(c, "Bad data format"); return; }

    if (lookupKeyWriteWithFlags(db, key, LOOKUP_NOTOUCH | LOOKUP_NOEXPIRE) != NULL) {
        decrRefCount(obj);
        addReply(c, shared.ok);
        return;
    }

    dbAdd(db, key, &obj);
    if (ttl > 0) setExpire(c, db, key, ttl);
    signalModifiedKey(c, db, key);
    server.dirty++;
    addReply(c, shared.ok);
}

void upgradeDoneCommand(client *c) {
    addReply(c, shared.ok);
}

/* ========================== Cron (not much needed now) ========================== */

void upgradeProcessCycle(void) {
    /* No-op in pull-based design — upgrade is synchronous on new_replica */
}

void upgradeCron(void) {
    upgradeRecvState *rs = server.upgrade_recv;
    if (rs == NULL) return;

    /* Check if sender threads have finished (non-blocking via atomic done flag) */
    if (rs->sending) {
        int all_done = 1;
        for (int i = 0; i < rs->total_threads; i++) {
            if (!atomic_load_explicit((_Atomic int *)&rs->workers[i].done, memory_order_acquire)) {
                all_done = 0;
                break;
            }
        }
        if (!all_done) return;

        /* All threads signaled done — join them (instant, they already exited) */
        for (int i = 0; i < rs->total_threads; i++) {
            pthread_join(rs->workers[i].thread, NULL);
        }

        /* All sender threads finished — finalize */
        long long total_keys = 0, total_bytes = 0;
        int had_error = 0;
        for (int i = 0; i < rs->total_threads; i++) {
            total_keys += rs->workers[i].keys_transferred;
            total_bytes += rs->workers[i].bytes_transferred;
            if (rs->workers[i].error) {
                serverLog(LL_WARNING, "UPGRADE.CHANNEL: sender thread %d error: %s",
                          i, rs->workers[i].errmsg);
                had_error = 1;
            }
        }

        /* Free channel clients (except client[0] which we keep for delta/REPLINFO) */
        for (int i = 1; i < rs->total_threads; i++) {
            if (rs->clients[i]) {
                freeClientAsync(rs->clients[i]);
                rs->clients[i] = NULL;
            }
        }
        zfree(rs->workers);
        rs->workers = NULL;
        rs->sending = 0;

        /* Re-enable rehashing */
        server.dict_resizing = 1;
        hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);

        serverLog(LL_NOTICE, "UPGRADE.CHANNEL: bulk transfer complete. %lld keys, %lld bytes%s. Entering delta phase.",
                  total_keys, total_bytes, had_error ? " (with errors)" : "");

        /* Send REPLINFO on conn[0] or enter delta phase */
        if (server.primary_host == NULL) {
            char offstr[21];
            int offlen = ll2string(offstr, sizeof(offstr), rs->snapshot_repl_offset);
            char replinfo[256];
            int rilen = snprintf(replinfo, sizeof(replinfo),
                                 "*3\r\n$16\r\nUPGRADE.REPLINFO\r\n$40\r\n%.40s\r\n$%d\r\n%s\r\n",
                                 server.replid, offlen, offstr);
            connSyncWrite(rs->conns[0], replinfo, rilen, 5000);
            if (rs->clients[0]) {
                freeClientAsync(rs->clients[0]);
                rs->clients[0] = NULL;
            }

            serverLog(LL_NOTICE, "UPGRADE.CHANNEL: no primary, sent REPLINFO immediately.");
            zfree(server.upgrade_recv);
            server.upgrade_recv = NULL;
            return;
        } else {
            rs->delta_conn = rs->conns[0];
            rs->delta_phase = 1;
        }
    }

    /* Delta phase: send REPLINFO with snapshot offset so new_replica PSYNCs
     * from the point the scan started. This is correct because the scan
     * captured the m_replica's state at snapshot_repl_offset. */
    if (rs->delta_phase && rs->delta_conn != NULL) {
        const char *replid = server.replid;
        long long offset = rs->snapshot_repl_offset;

        char offstr[21];
        int offlen = ll2string(offstr, sizeof(offstr), offset);
        char replinfo[256];
        int len = snprintf(replinfo, sizeof(replinfo),
                           "*3\r\n$16\r\nUPGRADE.REPLINFO\r\n$40\r\n%.40s\r\n$%d\r\n%s\r\n",
                           replid, offlen, offstr);

        connSyncWrite(rs->delta_conn, replinfo, len, 5000);
        if (rs->clients[0]) {
            freeClientAsync(rs->clients[0]);
            rs->clients[0] = NULL;
        }
        rs->delta_conn = NULL;
        rs->delta_phase = 0;

        serverLog(LL_NOTICE, "UPGRADE: sent REPLINFO replid=%.40s offset=%lld",
                  replid, offset);

        zfree(server.upgrade_recv);
        server.upgrade_recv = NULL;
    }
}

