#include "server.h"
#include "rdb.h"
#include "anet.h"
#include <pthread.h>
#include <poll.h>

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

static int upgradeWriteAll(int fd, const char *buf, size_t len) {
    while (len > 0) {
        ssize_t n = write(fd, buf, len);
        if (n <= 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        buf += n;
        len -= n;
    }
    return 0;
}

static ssize_t upgradeReadLine(int fd, char *buf, size_t maxlen) {
    size_t pos = 0;
    while (pos < maxlen - 1) {
        char c;
        ssize_t n = read(fd, &c, 1);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return -1;
        }
        buf[pos++] = c;
        if (c == '\n') break;
    }
    buf[pos] = '\0';
    return (ssize_t)pos;
}

static int upgradeReadExact(int fd, char *buf, size_t len) {
    while (len > 0) {
        ssize_t n = read(fd, buf, len);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return -1;
        }
        buf += n;
        len -= n;
    }
    return 0;
}

/* ========================== Sender Thread (m_replica side) ========================== */

typedef struct upgradeSendWorker {
    pthread_t thread;
    int thread_id;
    int total_threads;
    int fd;
    long long keys_transferred;
    long long bytes_transferred;
    int error;
    char errmsg[256];
} upgradeSendWorker;

static int upgradeSendKey(int fd, sds keyname, long long ttl, sds serialized, int dbid, sds *sendbuf) {
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

    if (sdslen(*sendbuf) >= UPGRADE_SEND_BUF_SIZE) {
        if (upgradeWriteAll(fd, *sendbuf, sdslen(*sendbuf)) == -1) return -1;
        sdsclear(*sendbuf);
    }
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

    if (upgradeSendKey(w->fd, keyname, ttl, serialized, ctx->dbid, &ctx->sendbuf) == -1) {
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

        ctx.dbid = dbid;
        hashtableIterateBucketRange(ht, w->total_threads, w->thread_id,
                                    upgradeSendEntryCallback, &ctx);
        if (w->error) break;
    }

    if (!w->error && sdslen(ctx.sendbuf) > 0) {
        if (upgradeWriteAll(w->fd, ctx.sendbuf, sdslen(ctx.sendbuf)) == -1) {
            snprintf(w->errmsg, sizeof(w->errmsg), "flush error: %s", strerror(errno));
            w->error = 1;
        }
    }
    sdsfree(ctx.sendbuf);

    if (!w->error) {
        const char *done_cmd = "*1\r\n$12\r\nUPGRADE.DONE\r\n";
        if (upgradeWriteAll(w->fd, done_cmd, strlen(done_cmd)) == -1) {
            w->error = 1;
        }
    }
    return NULL;
}

/* ========================== Receiver Thread (new_replica side) ========================== */

typedef struct upgradeRecvThreadArg {
    int fd;
    int thread_id;
    long long *keys_inserted;
    int *done_flag;
    int *error_flag;
} upgradeRecvThreadArg;

static sds upgradeRecvReadBulk(int fd, int len) {
    sds s = sdsnewlen(NULL, len);
    if (upgradeReadExact(fd, s, len) == -1) {
        sdsfree(s);
        return NULL;
    }
    char crlf[2];
    if (upgradeReadExact(fd, crlf, 2) == -1) {
        sdsfree(s);
        return NULL;
    }
    return s;
}

static void *upgradeRecvWorkerMain(void *arg) {
    upgradeRecvThreadArg *targ = (upgradeRecvThreadArg *)arg;
    int fd = targ->fd;
    char linebuf[256];

    while (1) {
        if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { *targ->error_flag = 1; break; }
        if (linebuf[0] != '*') { *targ->error_flag = 1; break; }
        int argc = atoi(linebuf + 1);

        /* Read command name */
        if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { *targ->error_flag = 1; break; }
        int cmdlen = atoi(linebuf + 1);
        char cmdname[64];
        if (cmdlen >= (int)sizeof(cmdname)) { *targ->error_flag = 1; break; }
        if (upgradeReadExact(fd, cmdname, cmdlen) == -1) { *targ->error_flag = 1; break; }
        cmdname[cmdlen] = '\0';
        char crlf[2];
        if (upgradeReadExact(fd, crlf, 2) == -1) { *targ->error_flag = 1; break; }

        if (argc == 1 && !strcasecmp(cmdname, "UPGRADE.DONE")) {
            /* Thread 0 continues for delta phase; others exit */
            if (targ->thread_id != 0) break;
            /* Thread 0: continue reading delta commands until UPGRADE.REPLINFO */
            continue;
        }

        /* UPGRADE.REPLINFO <replid> <offset> — end of delta (thread 0 only) */
        if (argc == 3 && !strcasecmp(cmdname, "UPGRADE.REPLINFO")) {
            /* Read replid */
            if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { *targ->error_flag = 1; break; }
            int replidlen = atoi(linebuf + 1);
            char replidbuf[CONFIG_RUN_ID_SIZE + 1];
            if (replidlen > CONFIG_RUN_ID_SIZE || upgradeReadExact(fd, replidbuf, replidlen) == -1) { *targ->error_flag = 1; break; }
            replidbuf[replidlen] = '\0';
            char crlf_tmp[2];
            if (upgradeReadExact(fd, crlf_tmp, 2) == -1) { *targ->error_flag = 1; break; }

            /* Read offset */
            if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { *targ->error_flag = 1; break; }
            int offlen = atoi(linebuf + 1);
            char offbuf[32];
            if (offlen >= (int)sizeof(offbuf) || upgradeReadExact(fd, offbuf, offlen) == -1) { *targ->error_flag = 1; break; }
            offbuf[offlen] = '\0';
            if (upgradeReadExact(fd, crlf_tmp, 2) == -1) { *targ->error_flag = 1; break; }

            /* Store replid and offset for PSYNC later */
            memcpy(server.replid, replidbuf, CONFIG_RUN_ID_SIZE + 1);
            server.primary_repl_offset = atoll(offbuf);
            serverLog(LL_NOTICE, "UPGRADE: received REPLINFO replid=%.40s offset=%lld",
                      server.replid, (long long)server.primary_repl_offset);
            break;
        }

        if (argc != 5 || strcasecmp(cmdname, "UPGRADE.RESTORE") != 0) {
            /* Delta phase: raw RESP commands from Primary (SET, DEL, etc.)
             * Read all bulk args and apply as a command on new_replica.
             * We accumulate into a buffer; the main thread will process it later. */
            /* For now: read and discard remaining bulk args.
             * The REPLINFO offset will let us PSYNC from Primary to get these. */
            for (int a = 1; a < argc; a++) {
                if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { *targ->error_flag = 1; break; }
                int bulklen = atoi(linebuf + 1);
                if (bulklen >= 0) {
                    sds discard = upgradeRecvReadBulk(fd, bulklen);
                    if (discard) sdsfree(discard); else { *targ->error_flag = 1; break; }
                }
            }
            if (*targ->error_flag) break;
            continue;
        }

        /* Read key */
        if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { *targ->error_flag = 1; break; }
        int keylen = atoi(linebuf + 1);
        sds key = upgradeRecvReadBulk(fd, keylen);
        if (!key) { *targ->error_flag = 1; break; }

        /* Read ttl */
        if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { sdsfree(key); *targ->error_flag = 1; break; }
        int ttllen = atoi(linebuf + 1);
        char ttlbuf[32];
        if (ttllen >= (int)sizeof(ttlbuf)) { sdsfree(key); *targ->error_flag = 1; break; }
        if (upgradeReadExact(fd, ttlbuf, ttllen) == -1) { sdsfree(key); *targ->error_flag = 1; break; }
        ttlbuf[ttllen] = '\0';
        if (upgradeReadExact(fd, crlf, 2) == -1) { sdsfree(key); *targ->error_flag = 1; break; }
        long long ttl = strtoll(ttlbuf, NULL, 10);

        /* Read serialized payload */
        if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { sdsfree(key); *targ->error_flag = 1; break; }
        int datalen = atoi(linebuf + 1);
        sds data = upgradeRecvReadBulk(fd, datalen);
        if (!data) { sdsfree(key); *targ->error_flag = 1; break; }

        /* Read dbid */
        if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }
        int dbidlen = atoi(linebuf + 1);
        char dbidbuf[16];
        if (dbidlen >= (int)sizeof(dbidbuf)) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }
        if (upgradeReadExact(fd, dbidbuf, dbidlen) == -1) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }
        dbidbuf[dbidlen] = '\0';
        if (upgradeReadExact(fd, crlf, 2) == -1) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }
        int dbid = atoi(dbidbuf);

        if (dbid < 0 || dbid >= server.dbnum || server.db[dbid] == NULL) {
            sdsfree(key); sdsfree(data);
            continue;
        }

        /* Verify and deserialize */
        uint16_t rdbver;
        if (verifyDumpPayload((unsigned char *)data, sdslen(data), &rdbver) == C_ERR) {
            sdsfree(key); sdsfree(data);
            *targ->error_flag = 1;
            break;
        }

        rio payload;
        rioInitWithBuffer(&payload, data);
        int type = rdbLoadObjectType(&payload);
        if (type == -1) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }

        robj *obj = rdbLoadObject(type, &payload, key, dbid, NULL, RDBFLAGS_NONE, 0);
        if (obj == NULL) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }

        /* Insert into pre-sized hashtable via kvstoreHashtableAdd.
         * This updates kvs metadata (key_count, non_empty_hashtables).
         * The ht->used[0]++ race is fixed post-join by hashtableSetUsedCount(). */
        serverDb *db = server.db[dbid];
        int dict_index = server.cluster_enabled ? getKeySlot(key) : 0;
        obj = objectSetKeyAndExpire(obj, key, ttl > 0 ? ttl : -1);
        initObjectLRUOrLFU(obj);

        if (kvstoreHashtableAdd(db->keys, dict_index, obj)) {
            (*targ->keys_inserted)++;
        } else {
            decrRefCount(obj);
        }

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

    int *fds = zmalloc(sizeof(int) * num_threads);

    /* Open connections to m_replica */
    serverLog(LL_NOTICE, "UPGRADE: connecting to m_replica %s:%d with %d threads...", m_host, m_port, num_threads);

    /* Phase 1: Handshake — get metadata from m_replica to pre-size hashtables */
    {
        char err[ANET_ERR_LEN];
        int initfd = anetTcpNonBlockBestEffortBindConnect(err, m_host, m_port, NULL, 0);
        if (initfd == -1) {
            server.upgrade->state = UPGRADE_STATE_ABORTED;
            addReplyError(c, "Failed to connect to m_replica for handshake");
            return;
        }
        struct pollfd pfd = { .fd = initfd, .events = POLLOUT };
        if (poll(&pfd, 1, 5000) <= 0) { close(initfd); server.upgrade->state = UPGRADE_STATE_ABORTED; addReplyError(c, "Handshake connect timeout"); return; }
        int sockerr = 0; socklen_t errlen = sizeof(sockerr);
        if (getsockopt(initfd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1 || sockerr != 0) { close(initfd); server.upgrade->state = UPGRADE_STATE_ABORTED; addReplyError(c, "Handshake connect error"); return; }
        anetBlock(err, initfd);

        /* Send UPGRADE.INIT */
        const char *init_cmd = "*1\r\n$12\r\nUPGRADE.INIT\r\n";
        if (upgradeWriteAll(initfd, init_cmd, strlen(init_cmd)) == -1) { close(initfd); server.upgrade->state = UPGRADE_STATE_ABORTED; addReplyError(c, "Handshake write error"); return; }

        /* Read response: *<num_dbs>\r\n then for each db: :<keycount>\r\n */
        char linebuf[256];
        if (upgradeReadLine(initfd, linebuf, sizeof(linebuf)) <= 0 || linebuf[0] != '*') { close(initfd); server.upgrade->state = UPGRADE_STATE_ABORTED; addReplyError(c, "Handshake read error"); return; }
        int num_dbs_reported = atoi(linebuf + 1);

        for (int dbid = 0; dbid < num_dbs_reported; dbid++) {
            if (upgradeReadLine(initfd, linebuf, sizeof(linebuf)) <= 0) { close(initfd); server.upgrade->state = UPGRADE_STATE_ABORTED; addReplyError(c, "Handshake read error"); return; }
            long long num_buckets = atoll(linebuf + 1);
            if (num_buckets > 0 && dbid < server.dbnum) {
                /* Ensure db exists */
                if (server.db[dbid] == NULL) {
                    server.db[dbid] = createDatabase(dbid);
                }
                /* Pre-expand hashtable to the EXACT same number of buckets as m_replica.
                 * hashtableExpand takes a capacity (entries), so we multiply buckets by
                 * ENTRIES_PER_BUCKET to get a capacity that results in the same bucket count. */
                kvstoreHashtableExpand(server.db[dbid]->keys, 0, num_buckets * 7);
            }
        }

        close(initfd);
        serverLog(LL_NOTICE, "UPGRADE: handshake done. Pre-sized hashtables for %d dbs.", num_dbs_reported);
    }

    /* Phase 2: Open channels */
    for (int i = 0; i < num_threads; i++) {
        char err[ANET_ERR_LEN];
        fds[i] = anetTcpNonBlockBestEffortBindConnect(err, m_host, m_port, NULL, 0);
        if (fds[i] == -1) {
            serverLog(LL_WARNING, "UPGRADE: connect to %s:%d failed: %s", m_host, m_port, err);
            for (int j = 0; j < i; j++) close(fds[j]);
            zfree(fds);
            server.upgrade->state = UPGRADE_STATE_ABORTED;
            addReplyError(c, "Failed to connect to m_replica");
            return;
        }

        /* Wait for connect */
        struct pollfd pfd = { .fd = fds[i], .events = POLLOUT };
        if (poll(&pfd, 1, 5000) <= 0) {
            for (int j = 0; j <= i; j++) close(fds[j]);
            zfree(fds);
            server.upgrade->state = UPGRADE_STATE_ABORTED;
            addReplyError(c, "Connection to m_replica timed out");
            return;
        }
        int sockerr = 0;
        socklen_t errlen = sizeof(sockerr);
        if (getsockopt(fds[i], SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1 || sockerr != 0) {
            for (int j = 0; j <= i; j++) close(fds[j]);
            zfree(fds);
            server.upgrade->state = UPGRADE_STATE_ABORTED;
            addReplyError(c, "Connection to m_replica failed");
            return;
        }

        anetBlock(err, fds[i]);
        anetEnableTcpNoDelay(err, fds[i]);

        /* Send UPGRADE.CHANNEL handshake */
        char idbuf[21], threadsbuf[21];
        int idlen = snprintf(idbuf, sizeof(idbuf), "%d", i);
        int threadslen = snprintf(threadsbuf, sizeof(threadsbuf), "%d", num_threads);
        char cmd[256];
        int len = snprintf(cmd, sizeof(cmd),
                           "*3\r\n$15\r\nUPGRADE.CHANNEL\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                           idlen, idbuf, threadslen, threadsbuf);
        if (upgradeWriteAll(fds[i], cmd, len) == -1) {
            for (int j = 0; j <= i; j++) close(fds[j]);
            zfree(fds);
            server.upgrade->state = UPGRADE_STATE_ABORTED;
            addReplyError(c, "Failed to send UPGRADE.CHANNEL");
            return;
        }

        /* Read +OK */
        char buf[64];
        if (upgradeReadLine(fds[i], buf, sizeof(buf)) <= 0 || buf[0] != '+') {
            serverLog(LL_WARNING, "UPGRADE: channel %d handshake failed: %.40s", i, buf);
            for (int j = 0; j <= i; j++) close(fds[j]);
            zfree(fds);
            server.upgrade->state = UPGRADE_STATE_ABORTED;
            addReplyError(c, "UPGRADE.CHANNEL handshake failed");
            return;
        }
    }

    serverLog(LL_NOTICE, "UPGRADE: all %d channels connected. Starting receiver threads.", num_threads);

    /* Disable rehashing on receiver */
    hashtableSetResizePolicy(HASHTABLE_RESIZE_FORBID);

    /* Spawn receiver threads */
    pthread_t *threads = zmalloc(sizeof(pthread_t) * num_threads);
    long long *keys_inserted = zcalloc(sizeof(long long) * num_threads);
    int *thread_done = zcalloc(sizeof(int) * num_threads);
    int *thread_error = zcalloc(sizeof(int) * num_threads);

    for (int i = 0; i < num_threads; i++) {
        upgradeRecvThreadArg *arg = zmalloc(sizeof(upgradeRecvThreadArg));
        arg->fd = fds[i];
        arg->thread_id = i;
        arg->keys_inserted = &keys_inserted[i];
        arg->done_flag = &thread_done[i];
        arg->error_flag = &thread_error[i];

        if (pthread_create(&threads[i], NULL, upgradeRecvWorkerMain, arg) != 0) {
            serverLog(LL_WARNING, "UPGRADE: pthread_create failed for thread %d", i);
            thread_error[i] = 1;
            thread_done[i] = 1;
            zfree(arg);
        }
    }

    /* Wait for all threads */
    long long total_keys = 0;
    int had_error = 0;
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        total_keys += keys_inserted[i];
        /* Thread 0 may report "error" if REPLINFO read fails (race with close).
         * Only treat as real error if it also inserted 0 keys. */
        if (thread_error[i] && i != 0) had_error = 1;
    }

    /* Close connections and free arrays */
    for (int i = 0; i < num_threads; i++) {
        close(fds[i]);
    }
    zfree(fds);
    zfree(threads);
    zfree(keys_inserted);
    zfree(thread_done);
    zfree(thread_error);

    /* Re-enable rehashing */
    hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);

    server.upgrade->keys_transferred = total_keys;
    server.dirty += total_keys;

    /* Finalize: fix ht->used[0] (racy from concurrent threads) and register expires.
     * All keys are inserted correctly — only the counter may be off. */
    for (int dbid = 0; dbid < server.dbnum; dbid++) {
        serverDb *db = server.db[dbid];
        if (db == NULL) continue;
        hashtable *ht = kvstoreGetHashtable(db->keys, 0);
        if (ht == NULL) continue;

        /* Fix the used count to total_keys (authoritative from thread counters) */
        hashtableSetUsedCount(ht, total_keys);
        break;  /* In standalone mode, all keys are in db[0] dict_index 0 */
    }

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

/* Forward declaration */
static void upgradeRecvCheckCompletion(void);

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

    /* Store fd and client */
    int dupfd = dup(c->conn->fd);
    if (dupfd == -1) {
        addReplyError(c, "dup() failed");
        return;
    }
    rs->fds[thread_id] = dupfd;
    rs->clients[thread_id] = c;
    rs->channels_registered++;

    /* Reply OK */
    addReply(c, shared.ok);

    /* Disable event loop reading on this fd */
    connSetReadHandler(c->conn, NULL);

    /* If all channels registered, start sending */
    if (rs->channels_registered == rs->total_threads) {
        serverLog(LL_NOTICE, "UPGRADE.CHANNEL: all %d channels registered. Freezing and sending.", rs->total_threads);

        /* Flush +OK replies to all channel clients before we block */
        for (int i = 0; i < rs->total_threads; i++) {
            if (rs->clients[i]) writeToClient(rs->clients[i]);
        }

        /* Freeze: pause primary input */
        /* (guard in processInputBuffer handles this via server.upgrade_recv) */

        /* Disable rehashing */
        hashtableSetResizePolicy(HASHTABLE_RESIZE_FORBID);

        /* Make fds blocking */
        char err[ANET_ERR_LEN];
        for (int i = 0; i < rs->total_threads; i++) {
            anetBlock(err, rs->fds[i]);
        }

        /* Spawn sender threads */
        upgradeSendWorker *workers = zcalloc(sizeof(upgradeSendWorker) * rs->total_threads);
        for (int i = 0; i < rs->total_threads; i++) {
            workers[i].thread_id = i;
            workers[i].total_threads = rs->total_threads;
            workers[i].fd = rs->fds[i];
        }

        for (int i = 0; i < rs->total_threads; i++) {
            pthread_create(&workers[i].thread, NULL, upgradeSendWorkerMain, &workers[i]);
        }

        /* Wait for all sender threads */
        long long total_keys = 0, total_bytes = 0;
        int had_error = 0;
        for (int i = 0; i < rs->total_threads; i++) {
            pthread_join(workers[i].thread, NULL);
            total_keys += workers[i].keys_transferred;
            total_bytes += workers[i].bytes_transferred;
            if (workers[i].error) {
                serverLog(LL_WARNING, "UPGRADE.CHANNEL: sender thread %d error: %s", i, workers[i].errmsg);
                had_error = 1;
            }
        }

        /* Close dupped fds except fd[0] which we keep for delta forwarding */
        for (int i = 1; i < rs->total_threads; i++) {
            close(rs->fds[i]);
        }
        zfree(workers);

        /* Re-enable rehashing */
        hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);

        serverLog(LL_NOTICE, "UPGRADE.CHANNEL: bulk transfer complete. %lld keys, %lld bytes%s. Entering delta phase.",
                  total_keys, total_bytes, had_error ? " (with errors)" : "");

        /* Send REPLINFO on fd[0] and enter delta phase */
        if (server.primary_host == NULL) {
            /* No Primary connected — no delta to forward. Send REPLINFO immediately. */
            char offstr[21];
            int offlen = ll2string(offstr, sizeof(offstr), server.primary_repl_offset);
            char replinfo[256];
            int rilen = snprintf(replinfo, sizeof(replinfo),
                                 "*3\r\n$15\r\nUPGRADE.REPLINFO\r\n$40\r\n%.40s\r\n$%d\r\n%s\r\n",
                                 server.replid, offlen, offstr);
            write(rs->fds[0], replinfo, rilen);
            close(rs->fds[0]);

            serverLog(LL_NOTICE, "UPGRADE.CHANNEL: no primary, sent REPLINFO immediately.");
            zfree(server.upgrade_recv);
            server.upgrade_recv = NULL;
        } else {
            /* Primary connected — enter delta phase to forward buffered writes */
            rs->delta_fd = rs->fds[0];
            rs->delta_phase = 1;
        }
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
    /* Check if delta phase is complete on m_replica */
    if (server.upgrade_recv && server.upgrade_recv->delta_phase &&
        server.upgrade_recv->delta_fd >= 0) {
        /* Delta is done when primary querybuf is fully processed */
        if (server.primary == NULL || server.primary->querybuf == NULL ||
            sdslen(server.primary->querybuf) == 0) {
            /* Send UPGRADE.REPLINFO: replid (for PSYNC) + offset (how far we've consumed) */
            const char *replid = server.replid;
            long long offset = server.primary_repl_offset;

            char offstr[21];
            int offlen = ll2string(offstr, sizeof(offstr), offset);
            char replinfo[256];
            int len = snprintf(replinfo, sizeof(replinfo),
                               "*3\r\n$15\r\nUPGRADE.REPLINFO\r\n$40\r\n%.40s\r\n$%d\r\n%s\r\n",
                               replid, offlen, offstr);

            write(server.upgrade_recv->delta_fd, replinfo, len);
            close(server.upgrade_recv->delta_fd);
            server.upgrade_recv->delta_fd = -1;
            server.upgrade_recv->delta_phase = 0;

            serverLog(LL_NOTICE, "UPGRADE: delta forwarding complete. Sent REPLINFO replid=%.40s offset=%lld",
                      replid, offset);

            zfree(server.upgrade_recv);
            server.upgrade_recv = NULL;
        }
    }
}

static void upgradeRecvCheckCompletion(void) {
    /* No-op — completion is handled synchronously in upgradeCommand */
}
