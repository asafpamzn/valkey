#!/usr/bin/env bash
set -euo pipefail

echo 1 | sudo tee /proc/sys/vm/unprivileged_userfaultfd >/dev/null

IMAGES_DIR="/fsx/checkpoint1/final"
WORK_DIR="/run/criu"
PORT=9001
sudo rm -fr "$IMAGES_DIR"/*
PID=$(pgrep -x valkey-server)
: "${PID:?valkey-server PID not found}"

SRC_CONNECT_IP="$(ip -o -4 addr show up primary scope global | awk '{print $4}' | cut -d/ -f1 | head -n1)"
: "${SRC_CONNECT_IP:?failed to detect primary IPv4}"

sudo mkdir -p "$WORK_DIR" "$IMAGES_DIR"

# 1) Start a local-only page-server for the dump
sudo criu page-server \
  --images-dir "$IMAGES_DIR" \
  --work-dir   "$WORK_DIR" \
  --address 127.0.0.1 --port "$PORT" \
  -v2 -o "$IMAGES_DIR/page-server.dump.log" &

# Wait for it
for _ in {1..50}; do
  sudo ss -lntp | grep -q ":$PORT\b" && break
  sleep 0.1
done
sudo ss -lntp | grep ":$PORT\b" >/dev/null || { echo "❌ local page-server not listening"; exit 1; }

# 2) Do the dump with leave-running
sudo criu dump -t "$PID" \
  -D "$IMAGES_DIR" \
  --work-dir "$WORK_DIR" \
  --page-server --address 127.0.0.1 --port "$PORT" \
  --tcp-close --ext-unix-sk \
  --leave-running \
  -v2 -o "$IMAGES_DIR/dump.log"

# 3) Stop the local page-server (it usually exits itself, but ensure)
pkill -f "criu page-server.*127.0.0.1.*$PORT" || true

# 4) Start a public page-server for the destination
sudo criu page-server \
  --images-dir "$IMAGES_DIR" \
  --work-dir   "$WORK_DIR" \
  --address 0.0.0.0 --port "$PORT" \
  -v2 -o "$IMAGES_DIR/page-server.serve.log" &

sleep 0.3
if sudo ss -lntp | grep -q ":$PORT\b"; then
  echo "✅ Dump complete, page-server serving for destination at ${SRC_CONNECT_IP}:$PORT"
  echo "   Logs:"
  echo "     $IMAGES_DIR/dump.log"
  echo "     $IMAGES_DIR/page-server.serve.log"
else
  echo "⚠️ Dump complete, but page-server failed to stay listening. Check $IMAGES_DIR/page-server.serve.log"
fi