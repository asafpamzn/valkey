#!/usr/bin/env bash
set -euo pipefail

echo 1 | sudo tee /proc/sys/vm/unprivileged_userfaultfd >/dev/null

BASE_DIR="/fsx/checkpoint1"
FINAL_DIR="$BASE_DIR/final"
WORK_DIR="/run/criu"
PORT=9001
ROUNDS=${ROUNDS:-3}          # number of pre-dumps (tune)
VERB="-v2"                   # make logs a bit chattier

sudo rm -rf "$BASE_DIR"/pre* "$FINAL_DIR"
sudo mkdir -p "$WORK_DIR" "$FINAL_DIR"

PID=$(pgrep -x valkey-server)
: "${PID:?valkey-server PID not found}"

SRC_CONNECT_IP="$(ip -o -4 addr show up primary scope global | awk '{print $4}' | cut -d/ -f1 | head -n1)"
: "${SRC_CONNECT_IP:?failed to detect primary IPv4}"

echo "🔧 Config"
echo "  PID            : $PID (valkey-server)"
echo "  Base dir       : $BASE_DIR"
echo "  Rounds (pre)   : $ROUNDS"
echo "  Final dir      : $FINAL_DIR"
echo "  Port           : $PORT"
echo "  Source connect : $SRC_CONNECT_IP"
echo "----------------------------------------------------------------"

# --- Pre-dump rounds (no long freeze) ---
prev=""
for i in $(seq 1 "$ROUNDS"); do
  dir="$BASE_DIR/pre$i"
  sudo mkdir -p "$dir"
  echo "🟡 pre-dump #$i  -> $dir"
  sudo criu pre-dump -t "$PID" \
    -D "$dir" \
    ${prev:+--prev-images-dir "$prev"} \
    --work-dir "$WORK_DIR" \
    --track-mem \
    --tcp-close \
    $VERB -o "$dir/pre-dump.log"
  prev="$dir"
done

# --- Local page-server for the tiny final dump ---
echo "🚀 starting local page-server for final dump"
sudo criu page-server \
  --images-dir "$FINAL_DIR" \
  --work-dir   "$WORK_DIR" \
  --address 127.0.0.1 --port "$PORT" \
  $VERB -o "$FINAL_DIR/page-server.dump.log" &

echo "PLEASE START..." | sudo tee "$FINAL_DIR/done.log" >/dev/null

for _ in {1..50}; do sudo ss -lntp | grep -q ":$PORT\b" && break; sleep 0.1; done
sudo ss -lntp | grep -q ":$PORT\b" || { echo "❌ local page-server not listening"; exit 1; }

# --- Final dump (short freeze) reusing pre-dumps ---
echo "🟢 final dump (leave-running) -> $FINAL_DIR"
t0=$(date +%s%3N)
sudo criu dump -t "$PID" \
  -D "$FINAL_DIR" \
  --work-dir "$WORK_DIR" \
  --page-server --address 127.0.0.1 --port "$PORT" \
  --track-mem \
  --tcp-close \
  ${prev:+--prev-images-dir "$prev"} \
  --ext-unix-sk --leave-running \
  $VERB -o "$FINAL_DIR/dump.log"
t1=$(date +%s%3N)
pkill -f "criu page-server.*127.0.0.1.*$PORT" || true

# --- Serve images (final + parents must remain in place) ---
echo "📡 serving images for destination on 0.0.0.0:$PORT"
sudo criu page-server \
  --images-dir "$FINAL_DIR" \
  --work-dir   "$WORK_DIR" \
  --address 0.0.0.0 --port "$PORT" \
  $VERB -o "$FINAL_DIR/page-server.serve.log" &

freeze_ms=$((t1 - t0))
echo "----------------------------------------------------------------"
echo "✅ Pre-copy done. Estimated final freeze: $((freeze_ms/1000)).$((freeze_ms%1000))s"
echo "   Parents: $(ls -1d $BASE_DIR/pre* 2>/dev/null | wc -l) dirs"
echo "   Serve:   ${SRC_CONNECT_IP}:$PORT"
echo "   Logs:    $FINAL_DIR/dump.log"
echo "            $FINAL_DIR/page-server.dump.log"
echo "            $FINAL_DIR/page-server.serve.log"
