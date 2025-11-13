#!/usr/bin/env bash
set -euo pipefail

# Configuration
SOURCE_HOST="ec2-54-221-42-237.compute-1.amazonaws.com"
SOURCE_USER="ubuntu"
SOURCE_SCRIPT="/home/ubuntu/work/valkey/scripts_criu/dump_replica_lazy.sh"
DEST_IP="10.0.14.165"
PORT=9002
IMAGES_DIR="/fsx/lazy"
LOG_FILE="$IMAGES_DIR/lazy-primary.log"
WAIT_TIMEOUT=300  # 5 minutes
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🚀 Starting Kill & Sync Orchestration"
echo "  Source Host    : $SOURCE_HOST"
echo "  Destination IP : $DEST_IP"
echo "  Port           : $PORT"
echo "  Images Dir     : $IMAGES_DIR"
echo "  Timeout        : ${WAIT_TIMEOUT}s"
echo "================================================================"

# Step 1: Clean up /fsx/lazy/*
echo "🧹 Step 1: Cleaning up $IMAGES_DIR/*"
sudo rm -rf "$IMAGES_DIR"/*
echo "✅ Cleanup complete"

# Step 2: Kill valkey-server
echo "🔪 Step 2: Killing valkey-server"
sudo pkill -9 valkey-server || true
echo "✅ valkey-server killed"

# Step 3: Start wait_and_replicate.sh in background
echo "🔄 Step 3: Starting wait_and_replicate.sh in background"
"$SCRIPT_DIR/wait_and_replicate.sh" &
REPLICATE_PID=$!
echo "✅ wait_and_replicate.sh started (PID: $REPLICATE_PID)"

# Step 4: Execute remote dump script on source machine
echo "📡 Step 4: Executing dump script on source machine"
sudo -u $SUDO_USER -H bash -c "ssh -o StrictHostKeyChecking=no $SOURCE_USER@$SOURCE_HOST 'bash $SOURCE_SCRIPT'"
echo "✅ Remote dump script completed"

# Step 5: Wait for "PAGE SERVER READY TO SERVE" in log file
echo "⏳ Step 5: Waiting for 'PAGE SERVER READY TO SERVE' in $LOG_FILE"
START_TIME=$(date +%s)
while true; do
  if [ -f "$LOG_FILE" ] && grep -q "PAGE SERVER READY TO SERVE" "$LOG_FILE"; then
    echo "✅ Page server ready signal detected"
    break
  fi
  
  ELAPSED=$(($(date +%s) - START_TIME))
  if [ $ELAPSED -ge $WAIT_TIMEOUT ]; then
    echo "❌ Timeout: Did not find 'PAGE SERVER READY TO SERVE' within ${WAIT_TIMEOUT}s"
    exit 1
  fi
  
  sleep 0.5
done

# Step 6: Start CRIU lazy-pages page server in background
echo "🌐 Step 6: Starting CRIU lazy-pages page server"
sudo criu lazy-pages \
  --images-dir "$IMAGES_DIR" \
  --page-server \
  --address "$DEST_IP" \
  --port "$PORT" \
  --tcp-close \
  -v2 -o "$IMAGES_DIR/lazy-server.log" &
PAGE_SERVER_PID=$!
echo "✅ Page server started (PID: $PAGE_SERVER_PID)"

# Sleep before restore
echo "⏸️  Sleeping 0.3 seconds..."
sleep 0.3

# Step 7: Start CRIU restore
echo "📦 Step 7: Starting CRIU restore"
sudo criu restore \
  --images-dir "$IMAGES_DIR" \
  --lazy-pages \
  --tcp-close \
  --skip-file-rwx-check \
  -v2 -o "$IMAGES_DIR/lazy-restore.log"

echo "================================================================"
echo "✅ Orchestration completed successfully!"
echo "📋 Logs available at:"
echo "   - Source dump:  $LOG_FILE (on source machine)"
echo "   - Page server:  $IMAGES_DIR/lazy-server.log"
echo "   - Restore:      $IMAGES_DIR/lazy-restore.log"
