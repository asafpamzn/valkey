#include "server.h"
#include "rdb.h"
#include "anet.h"
#include <pthread.h>
#include <poll.h>

/* Forward declarations */
void createDumpPayload(rio *payload, robj *o, robj *key, int dbid);
static void upgradeRecvCheckCompletion(void);

#define UPGRADE_NUM_THREADS    10
#define UPGRADE_SEND_BUF_SIZE  (1024 * 1024)
#define UPGRADE_KEYS_PER_CYCLE 1000

/* ========================== Upgrade State Management ========================== */

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

static void upgradeAbort(const char *reason) {
    if (server.upgrade == NULL) return;
    serverLog(LL_WARNING, "UPGRADE aborted: %s", reason);
    server.upgrade->state = UPGRADE_STATE_ABORTED;
    if (server.upgrade->iter) {
        kvstoreIteratorRelease(server.upgrade->iter);
        server.upgrade->iter = NULL;
    }
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

/* ========================== Low-level I/O helpers ========================== */

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

/* ========================== Multithreaded UPGRADE start ========================== */

/* Open a TCP connection (blocking) and send UPGRADE.CHANNEL handshake.
 * Returns fd on success, -1 on error. */
static int upgradeOpenChannel(const char *host, int port, int thread_id, int total_threads) {
    char err[ANET_ERR_LEN];
    int fd = anetTcpNonBlockBestEffortBindConnect(err, host, port, NULL, 0);
    if (fd == -1) {
        serverLog(LL_WARNING, "UPGRADE: connect to %s:%d failed: %s", host, port, err);
        return -1;
    }

    /* Wait for connect to complete */
    struct pollfd pfd = { .fd = fd, .events = POLLOUT };
    if (poll(&pfd, 1, 5000) <= 0) {
        serverLog(LL_WARNING, "UPGRADE: connect to %s:%d timed out", host, port);
        close(fd);
        return -1;
    }
    /* Check for connect error */
    int sockerr = 0;
    socklen_t errlen = sizeof(sockerr);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1 || sockerr != 0) {
        serverLog(LL_WARNING, "UPGRADE: connect to %s:%d error: %s", host, port, strerror(sockerr));
        close(fd);
        return -1;
    }

    anetBlock(err, fd);
    anetEnableTcpNoDelay(err, fd);

    /* Send UPGRADE.CHANNEL handshake */
    char cmd[128];
    int len = snprintf(cmd, sizeof(cmd),
                       "*3\r\n$15\r\nUPGRADE.CHANNEL\r\n$%d\r\n%d\r\n$%d\r\n%d\r\n",
                       (thread_id >= 10) ? 2 : 1, thread_id,
                       (total_threads >= 10) ? 2 : 1, total_threads);
    if (upgradeWriteAll(fd, cmd, len) == -1) {
        close(fd);
        return -1;
    }

    /* Wait for +OK response */
    char buf[64];
    if (upgradeReadLine(fd, buf, sizeof(buf)) <= 0 || buf[0] != '+') {
        serverLog(LL_WARNING, "UPGRADE: channel handshake failed: %.40s", buf);
        close(fd);
        return -1;
    }
    return fd;
}

/* Start the multithreaded upgrade (sender side).
 * Opens N connections to the replica and spawns N sender threads. */
static int upgradeStartMultithreaded(client *replica) {
    upgradeState *us = server.upgrade;

    /* Get replica address */
    char ip[NET_IP_STR_LEN];
    int port;
    if (replica->repl_data->replica_addr) {
        memcpy(ip, replica->repl_data->replica_addr,
               strlen(replica->repl_data->replica_addr) + 1);
    } else {
        if (connAddrPeerName(replica->conn, ip, sizeof(ip), NULL) == -1) {
            serverLog(LL_WARNING, "UPGRADE: cannot get replica address");
            return -1;
        }
    }
    port = replica->repl_data->replica_listening_port;
    if (port == 0) {
        serverLog(LL_WARNING, "UPGRADE: replica listening port unknown");
        return -1;
    }

    int num_threads = UPGRADE_NUM_THREADS;
    upgradeSendWorker *workers = zcalloc(sizeof(upgradeSendWorker) * num_threads);

    /* Open connections */
    for (int i = 0; i < num_threads; i++) {
        workers[i].thread_id = i;
        workers[i].total_threads = num_threads;
        workers[i].fd = upgradeOpenChannel(ip, port, i, num_threads);
        if (workers[i].fd == -1) {
            /* Close already-opened connections */
            for (int j = 0; j < i; j++) close(workers[j].fd);
            zfree(workers);
            return -1;
        }
    }

    /* Spawn sender threads */
    for (int i = 0; i < num_threads; i++) {
        if (pthread_create(&workers[i].thread, NULL, upgradeSendWorkerMain, &workers[i]) != 0) {
            serverLog(LL_WARNING, "UPGRADE: pthread_create failed");
            for (int j = 0; j < num_threads; j++) close(workers[j].fd);
            zfree(workers);
            return -1;
        }
    }

    /* Wait for all threads to complete */
    long long total_keys = 0, total_bytes = 0;
    int had_error = 0;
    for (int i = 0; i < num_threads; i++) {
        pthread_join(workers[i].thread, NULL);
        close(workers[i].fd);
        total_keys += workers[i].keys_transferred;
        total_bytes += workers[i].bytes_transferred;
        if (workers[i].error) {
            serverLog(LL_WARNING, "UPGRADE: thread %d error: %s", i, workers[i].errmsg);
            had_error = 1;
        }
    }
    zfree(workers);

    us->keys_transferred = total_keys;
    us->bytes_transferred = total_bytes;

    if (had_error) return -1;
    return 0;
}

/* ========================== Single-threaded path (via replication buffer) ========================== */

static void upgradeInjectCommand(robj **argv, int argc) {
    if (server.repl_backlog == NULL) return;
    prepareReplicasToWrite();

    char aux[LONG_STR_SIZE + 3];
    int len;

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

static long long upgradeTransferKeySingleThread(serverDb *db, robj *entry) {
    sds keyname = objectGetKey(entry);
    if (keyname == NULL) return 0;

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

/* ========================== Event Loop Integration ========================== */

static void upgradeStartNextDb(void) {
    upgradeState *us = server.upgrade;
    if (us->iter) {
        kvstoreIteratorRelease(us->iter);
        us->iter = NULL;
    }
    while (us->current_db < server.dbnum) {
        if (server.db[us->current_db] != NULL) {
            us->iter = kvstoreIteratorInit(server.db[us->current_db]->keys, HASHTABLE_ITER_SAFE);
            return;
        }
        us->current_db++;
    }
}

void upgradeProcessCycle(void) {
    upgradeState *us = server.upgrade;
    if (us == NULL) return;

    switch (us->state) {
    case UPGRADE_STATE_SCANNING: {
        if (us->iter == NULL) {
            serverLog(LL_NOTICE,
                      "UPGRADE scan complete. Keys: %lld, skipped: %lld, bytes: %lld.",
                      us->keys_transferred, us->keys_skipped, us->bytes_transferred);
            us->state = UPGRADE_STATE_REPLAY;
            hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);
            return;
        }

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
            long long bytes = upgradeTransferKeySingleThread(db, (robj *)entry);
            if (bytes > 0) {
                us->keys_transferred++;
                us->bytes_transferred += bytes;
            } else {
                us->keys_skipped++;
            }
            batch++;
        }

        if (batch < UPGRADE_KEYS_PER_CYCLE) {
            kvstoreIteratorRelease(us->iter);
            us->iter = NULL;
            us->current_db++;
            while (us->current_db < server.dbnum) {
                if (server.db[us->current_db] != NULL) {
                    us->iter = kvstoreIteratorInit(server.db[us->current_db]->keys, HASHTABLE_ITER_SAFE);
                    break;
                }
                us->current_db++;
            }
            if (us->iter == NULL) {
                serverLog(LL_NOTICE,
                          "UPGRADE scan complete. Keys: %lld, skipped: %lld, bytes: %lld.",
                          us->keys_transferred, us->keys_skipped, us->bytes_transferred);
                us->state = UPGRADE_STATE_REPLAY;
                hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);
            }
        }
        break;
    }

    case UPGRADE_STATE_REPLAY:
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
                      "UPGRADE complete! Keys: %lld, bytes: %lld, time: %lld ms.",
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
    /* Check receiver side completion */
    if (server.upgrade_recv != NULL) {
        upgradeRecvCheckCompletion();
    }

    /* Check sender side */
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

    /* UPGRADE <replica-client-id> [THREADED] */
    if (c->argc < 2) {
        addReplyError(c, "Usage: UPGRADE <replica-client-id> [THREADED] | STATUS | PAUSE | RESUME | ABORT");
        return;
    }

    if (server.upgrade != NULL && server.upgrade->state != UPGRADE_STATE_DONE &&
        server.upgrade->state != UPGRADE_STATE_ABORTED) {
        addReplyError(c, "An upgrade is already in progress. Use UPGRADE ABORT first.");
        return;
    }

    if (server.repl_backlog == NULL) {
        addReplyError(c, "No replication backlog. Is the target replica connected?");
        return;
    }

    long long client_id;
    if (getLongLongFromObjectOrReply(c, c->argv[1], &client_id, NULL) != C_OK) return;

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

    /* Check for THREADED option */
    int threaded = 0;
    if (c->argc >= 3 && !strcasecmp(objectGetVal(c->argv[2]), "threaded")) {
        threaded = 1;
    }

    /* Free previous state */
    if (server.upgrade != NULL) upgradeFree();

    /* Initialize */
    server.upgrade = zcalloc(sizeof(upgradeState));
    server.upgrade->state = UPGRADE_STATE_SCANNING;
    server.upgrade->target_replica = replica;
    server.upgrade->target_client_id = client_id;
    server.upgrade->current_db = 0;
    server.upgrade->iter = NULL;
    server.upgrade->start_time = mstime();

    hashtableSetResizePolicy(HASHTABLE_RESIZE_FORBID);

    if (threaded) {
        /* Multithreaded path: synchronous, blocks until all threads complete */
        serverLog(LL_NOTICE, "UPGRADE started (threaded, %d threads). Target: %lld.",
                  UPGRADE_NUM_THREADS, client_id);

        if (upgradeStartMultithreaded(replica) == -1) {
            upgradeAbort("multithreaded transfer failed");
            addReplyError(c, "UPGRADE multithreaded transfer failed");
            return;
        }

        /* All data sent. Transition directly to DRAINING (skip SCANNING/REPLAY). */
        hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);
        server.upgrade->state = UPGRADE_STATE_DRAINING;
        serverLog(LL_NOTICE, "UPGRADE threaded transfer done. Keys: %lld, bytes: %lld. Draining.",
                  server.upgrade->keys_transferred, server.upgrade->bytes_transferred);
    } else {
        /* Single-threaded path: incremental via event loop */
        serverLog(LL_NOTICE, "UPGRADE started (single-threaded). Target: %lld.", client_id);
        upgradeStartNextDb();
    }

    addReply(c, shared.ok);
}

/* ========================== Receiver Thread (new_replica side, Option B) ========================== */

typedef struct upgradeRecvThreadArg {
    int fd;
    int thread_id;
    long long *keys_inserted;
    int *done_flag;
    int *error_flag;
} upgradeRecvThreadArg;

/* Simple RESP reader: read a bulk string of given length */
static sds upgradeRecvReadBulk(int fd, int len, char *readbuf, int readbuf_size) {
    sds s = sdsnewlen(NULL, len);
    int pos = 0;
    while (pos < len) {
        int toread = len - pos;
        if (toread > readbuf_size) toread = readbuf_size;
        if (upgradeReadExact(fd, s + pos, toread) == -1) {
            sdsfree(s);
            return NULL;
        }
        pos += toread;
    }
    /* Read trailing \r\n */
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
    char readbuf[65536];

    while (1) {
        /* Read *<argc>\r\n */
        if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) {
            *targ->error_flag = 1;
            break;
        }
        if (linebuf[0] != '*') {
            *targ->error_flag = 1;
            break;
        }
        int argc = atoi(linebuf + 1);

        /* Read command name: $<len>\r\n<data>\r\n */
        if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { *targ->error_flag = 1; break; }
        int cmdlen = atoi(linebuf + 1);
        char cmdname[64];
        if (cmdlen >= (int)sizeof(cmdname) || upgradeReadExact(fd, cmdname, cmdlen) == -1) { *targ->error_flag = 1; break; }
        cmdname[cmdlen] = '\0';
        char crlf[2];
        if (upgradeReadExact(fd, crlf, 2) == -1) { *targ->error_flag = 1; break; }

        /* Check if UPGRADE.DONE */
        if (argc == 1 && !strcasecmp(cmdname, "UPGRADE.DONE")) {
            break;
        }

        /* Must be UPGRADE.RESTORE with 5 args */
        if (argc != 5 || strcasecmp(cmdname, "UPGRADE.RESTORE") != 0) {
            *targ->error_flag = 1;
            break;
        }

        /* Read key: $<len>\r\n<data>\r\n */
        if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { *targ->error_flag = 1; break; }
        int keylen = atoi(linebuf + 1);
        sds key = upgradeRecvReadBulk(fd, keylen, readbuf, sizeof(readbuf));
        if (key == NULL) { *targ->error_flag = 1; break; }

        /* Read ttl */
        if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { sdsfree(key); *targ->error_flag = 1; break; }
        int ttllen = atoi(linebuf + 1);
        char ttlbuf[32];
        if (ttllen >= (int)sizeof(ttlbuf) || upgradeReadExact(fd, ttlbuf, ttllen) == -1) { sdsfree(key); *targ->error_flag = 1; break; }
        ttlbuf[ttllen] = '\0';
        if (upgradeReadExact(fd, crlf, 2) == -1) { sdsfree(key); *targ->error_flag = 1; break; }
        long long ttl = strtoll(ttlbuf, NULL, 10);

        /* Read serialized payload */
        if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { sdsfree(key); *targ->error_flag = 1; break; }
        int datalen = atoi(linebuf + 1);
        sds data = upgradeRecvReadBulk(fd, datalen, readbuf, sizeof(readbuf));
        if (data == NULL) { sdsfree(key); *targ->error_flag = 1; break; }

        /* Read dbid */
        if (upgradeReadLine(fd, linebuf, sizeof(linebuf)) <= 0) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }
        int dbidlen = atoi(linebuf + 1);
        char dbidbuf[16];
        if (dbidlen >= (int)sizeof(dbidbuf) || upgradeReadExact(fd, dbidbuf, dbidlen) == -1) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }
        dbidbuf[dbidlen] = '\0';
        if (upgradeReadExact(fd, crlf, 2) == -1) { sdsfree(key); sdsfree(data); *targ->error_flag = 1; break; }
        int dbid = atoi(dbidbuf);

        /* Deserialize and insert */
        if (dbid < 0 || dbid >= server.dbnum || server.db[dbid] == NULL) {
            sdsfree(key);
            sdsfree(data);
            continue;
        }

        /* Verify payload */
        uint16_t rdbver;
        if (verifyDumpPayload((unsigned char *)data, sdslen(data), &rdbver) == C_ERR) {
            sdsfree(key);
            sdsfree(data);
            *targ->error_flag = 1;
            break;
        }

        rio payload;
        rioInitWithBuffer(&payload, data);

        int type = rdbLoadObjectType(&payload);
        if (type == -1) {
            sdsfree(key);
            sdsfree(data);
            *targ->error_flag = 1;
            break;
        }

        robj *obj = rdbLoadObject(type, &payload, key, dbid, NULL, RDBFLAGS_NONE, 0);
        if (obj == NULL) {
            sdsfree(key);
            sdsfree(data);
            *targ->error_flag = 1;
            break;
        }

        /* Direct insert into hashtable */
        serverDb *db = server.db[dbid];
        int dict_index = server.cluster_enabled ? getKeySlot(key) : 0;
        obj = objectSetKeyAndExpire(obj, key, ttl > 0 ? ttl : -1);
        initObjectLRUOrLFU(obj);
        if (!kvstoreHashtableAdd(db->keys, dict_index, obj)) {
            decrRefCount(obj);
        }

        (*targ->keys_inserted)++;

        sdsfree(key);
        sdsfree(data);
    }

    *targ->done_flag = 1;
    zfree(targ);
    return NULL;
}

static void upgradeRecvSpawnThreads(void) {
    upgradeRecvState *rs = server.upgrade_recv;

    serverLog(LL_NOTICE, "UPGRADE receiver: spawning %d threads for parallel insertion.", rs->total_threads);

    /* Disable rehashing on receiver */
    hashtableSetResizePolicy(HASHTABLE_RESIZE_FORBID);

    /* Make all fds blocking for thread reads and dup them
     * so the client cleanup path can close its own fd. */
    char err[ANET_ERR_LEN];
    for (int i = 0; i < rs->total_threads; i++) {
        int dupfd = dup(rs->fds[i]);
        if (dupfd == -1) {
            serverLog(LL_WARNING, "UPGRADE receiver: dup() failed for thread %d", i);
            return;
        }
        rs->fds[i] = dupfd;
        anetBlock(err, rs->fds[i]);
    }

    for (int i = 0; i < rs->total_threads; i++) {
        upgradeRecvThreadArg *arg = zmalloc(sizeof(upgradeRecvThreadArg));
        arg->fd = rs->fds[i];
        arg->thread_id = i;
        arg->keys_inserted = &rs->keys_inserted[i];
        arg->done_flag = &rs->thread_done[i];
        arg->error_flag = &rs->thread_error[i];

        if (pthread_create(&rs->threads[i], NULL, upgradeRecvWorkerMain, arg) != 0) {
            serverLog(LL_WARNING, "UPGRADE receiver: failed to create thread %d", i);
            rs->thread_error[i] = 1;
            rs->thread_done[i] = 1;
            zfree(arg);
        }
    }
}

/* Called from upgradeCron to check if all receiver threads are done */
static void upgradeRecvCheckCompletion(void) {
    upgradeRecvState *rs = server.upgrade_recv;
    if (rs == NULL || rs->all_done) return;

    for (int i = 0; i < rs->total_threads; i++) {
        if (!rs->thread_done[i]) return;
    }

    /* All threads done — join and finalize */
    long long total_keys = 0;
    int had_error = 0;
    for (int i = 0; i < rs->total_threads; i++) {
        pthread_join(rs->threads[i], NULL);
        total_keys += rs->keys_inserted[i];
        if (rs->thread_error[i]) had_error = 1;
    }

    /* Fix up hashtable counters.
     * Since threads called hashtableAdd() which increments ht->used[0],
     * the counter should already be correct. But kvs->key_count
     * was also updated by kvstoreHashtableAdd for some paths. Let's
     * just update server.dirty. */
    server.dirty += total_keys;

    /* Re-enable rehashing */
    hashtableSetResizePolicy(HASHTABLE_RESIZE_ALLOW);

    serverLog(LL_NOTICE, "UPGRADE receiver: complete. %lld keys inserted%s.",
              total_keys, had_error ? " (with errors)" : "");

    /* Close the dupped fds used by threads, then free clients normally */
    for (int i = 0; i < rs->total_threads; i++) {
        close(rs->fds[i]);
        if (rs->clients[i]) {
            freeClient(rs->clients[i]);
        }
    }

    rs->all_done = 1;
    zfree(server.upgrade_recv);
    server.upgrade_recv = NULL;
}

/* ========================== UPGRADE.CHANNEL Command (receiver side) ========================== */

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
    rs->fds[thread_id] = c->conn->fd;
    rs->clients[thread_id] = c;
    rs->channels_registered++;

    /* Reply OK — sender waits for this before spawning its thread */
    addReply(c, shared.ok);

    /* Immediately disable event loop reading on this fd.
     * The sender won't send data until it receives +OK, so no data loss. */
    connSetReadHandler(c->conn, NULL);

    /* If all channels registered, spawn receiver threads */
    if (rs->channels_registered == rs->total_threads) {
        upgradeRecvSpawnThreads();
    }
}

/* ========================== UPGRADE.RESTORE Command (receiver side) ========================== */

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

    if (ttl > 0 && ttl < mstime()) {
        addReply(c, shared.ok);
        return;
    }

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
    if (type == -1) {
        addReplyError(c, "Bad data format");
        return;
    }

    robj *obj = rdbLoadObject(type, &payload, objectGetVal(key), dbid, NULL, RDBFLAGS_NONE, 0);
    if (obj == NULL) {
        addReplyError(c, "Bad data format");
        return;
    }

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

/* UPGRADE.DONE — marks end of a channel's transfer */
void upgradeDoneCommand(client *c) {
    addReply(c, shared.ok);
}
