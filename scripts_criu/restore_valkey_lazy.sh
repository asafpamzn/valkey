#!/usr/bin/env bash
# restore_valkey_lazy.sh — CRIU lazy restore for Valkey (destination side)
# Usage:
#   sudo ./restore_valkey_lazy.sh \
#     --src-ip 10.0.14.165 \
#     --src-port 9001 \
#     --images-dir /fsx/checkpoint1/final \
#     [--valkey-port 6379] [--owner valkey] [--group valkey] [--skip-rwx-check] [--replica-of host:port]
#
# Example:
#   sudo ./restore_valkey_lazy.sh --src-ip 10.0.14.165 --src-port 9001 --images-dir /fsx/checkpoint1/final --owner ubuntu --group ubuntu

set -euo pipefail

# -------- defaults --------
SRC_IP=""
SRC_PORT=""
IMAGES_DIR=""
VALKEY_PORT=6379
OWNER="${SUDO_USER:-$USER}"
GROUP="$OWNER"
SKIP_RWX_CHECK=0
REPLICA_OF=""

LOG_DIR="/var/log/valkey"
WORK_DIR="/run/criu"
DATA_DIR="/var/lib/valkey"
RESTORE_LOG="$LOG_DIR/restore.log"
LAZY_LOG="$LOG_DIR/lazy-pages.log"

die() { echo "❌ $*" >&2; exit 1; }

need_cmd() { command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"; }

# -------- arg parsing --------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --src-ip)        SRC_IP="${2:?}"; shift 2;;
    --src-port)      SRC_PORT="${2:?}"; shift 2;;
    --images-dir)    IMAGES_DIR="${2:?}"; shift 2;;
    --valkey-port)   VALKEY_PORT="${2:?}"; shift 2;;
    --owner)         OWNER="${2:?}"; shift 2;;
    --group)         GROUP="${2:?}"; shift 2;;
    --skip-rwx-check) SKIP_RWX_CHECK=1; shift 1;;
    --replica-of)    REPLICA_OF="${2:?}"; shift 2;;
    -h|--help) sed -n '1,80p' "$0"; exit 0;;
    *) die "Unknown arg: $1";;
  esac
done

[[ -n "$SRC_IP" && -n "$SRC_PORT" && -n "$IMAGES_DIR" ]] || die "Required: --src-ip --src-port --images-dir"

# -------- preflight --------
need_cmd criu
need_cmd nc
need_cmd ss
need_cmd stat
need_cmd awk
need_cmd tee

echo "🔧 Config:"
echo "  Source page-server: ${SRC_IP}:${SRC_PORT}"
echo "  Images dir        : ${IMAGES_DIR}"
echo "  Valkey port       : ${VALKEY_PORT}"
echo "  Owner:Group       : ${OWNER}:${GROUP}"
echo "  Skip rwx check    : ${SKIP_RWX_CHECK}"
[[ -n "$REPLICA_OF" ]] && echo "  Post-restore      : replicaof $REPLICA_OF"

[[ -d "$IMAGES_DIR" ]] || die "Images dir not found: $IMAGES_DIR"

echo "🧪 Checking connectivity to source page-server..."
nc -vz -w 3 "$SRC_IP" "$SRC_PORT" || die "Cannot reach ${SRC_IP}:${SRC_PORT}"

echo "🧰 Enabling userfaultfd..."
echo 1 | sudo tee /proc/sys/vm/unprivileged_userfaultfd >/dev/null || true

echo "📁 Preparing directories & permissions..."
sudo mkdir -p "$WORK_DIR" "$LOG_DIR" "$DATA_DIR"
# CRIU is picky: match common expected perms (0750 for data dir, 0660 for logs).
sudo chmod 0750 "$DATA_DIR"
sudo touch "$LOG_DIR/stdout.log" "$LOG_DIR/stderr.log"
sudo chmod 0660 "$LOG_DIR/stdout.log" "$LOG_DIR/stderr.log"
# Ownership (adjusts to args)
sudo chown -R "$OWNER:$GROUP" "$DATA_DIR" "$LOG_DIR" || true

echo "🧹 Cleaning old lazy-pages socket..."
sudo rm -f "$WORK_DIR/lazy-pages.socket"

echo "▶️ Starting lazy-pages helper (background)..."
sudo criu lazy-pages \
  --images-dir "$IMAGES_DIR" \
  --work-dir   "$WORK_DIR" \
  --address "$SRC_IP" --port "$SRC_PORT" \
  -v1 -o "$LAZY_LOG" &

LP_PID=$!
# Wait for the socket to appear
for i in {1..50}; do
  [[ -S "$WORK_DIR/lazy-pages.socket" ]] && break
  sleep 0.1
done
[[ -S "$WORK_DIR/lazy-pages.socket" ]] || { sudo tail -n 60 "$LAZY_LOG" 2>/dev/null || true; die "lazy-pages socket didn't appear at $WORK_DIR/lazy-pages.socket"; }

echo "📎 lazy-pages helper PID: $LP_PID (socket up)"

# Build restore args
RESTORE_ARGS=(
  --images-dir "$IMAGES_DIR"
  --work-dir   "$WORK_DIR"
  --lazy-pages
  --tcp-close --ext-unix-sk
  -v4 -o "$RESTORE_LOG"
)
[[ "$SKIP_RWX_CHECK" -eq 1 ]] && RESTORE_ARGS+=(--skip-file-rwx-check)

echo "🔁 Running CRIU restore..."
if ! sudo criu restore "${RESTORE_ARGS[@]}"; then
  echo "⚠️ Restore failed. Last logs:"
  (echo "==> $LAZY_LOG"; sudo tail -n 80 "$LAZY_LOG" 2>/dev/null || true)
  (echo "==> $RESTORE_LOG"; sudo tail -n 120 "$RESTORE_LOG" 2>/dev/null || true)
  kill "$LP_PID" 2>/dev/null || true
  exit 1
fi

# The lazy-pages helper often exits on success after wiring up UFFD.
wait "$LP_PID" 2>/dev/null || true

echo "🩺 Checking Valkey port..."
for i in {1..50}; do
  if sudo ss -lntp | awk -v p=":${VALKEY_PORT}" '$4 ~ p {ok=1} END{exit ok?0:1}'; then
    echo "✅ Valkey is listening on port $VALKEY_PORT"
    break
  fi
  sleep 0.1
done

if ! sudo ss -lntp | grep -q ":${VALKEY_PORT}\b"; then
  echo "⚠️ Valkey not listening yet. Tail logs for clues:"
  (echo "==> $LAZY_LOG"; sudo tail -n 80 "$LAZY_LOG" 2>/dev/null || true)
  (echo "==> $RESTORE_LOG"; sudo tail -n 120 "$RESTORE_LOG" 2>/dev/null || true)
  exit 2
fi

# Optional post-step: make this node a replica
if [[ -n "$REPLICA_OF" ]]; then
  need_cmd valkey-cli
  HOST="${REPLICA_OF%:*}"
  PORT="${REPLICA_OF#*:}"
  echo "🔁 Setting replicaof $HOST $PORT ..."
  if ! valkey-cli -p "$VALKEY_PORT" replicaof "$HOST" "$PORT"; then
    echo "⚠️ replicaof command failed; continuing."
  fi
fi

echo "🏁 Done."
echo "   Logs:"
echo "     $RESTORE_LOG"
echo "     $LAZY_LOG"
