#!/bin/bash
set -euo pipefail

# ============================================
# Configuration
# ============================================
LOG_FILE="/fsx/checkpoint1/final/dump.log"
RESTORE_DIR="/fsx/checkpoint1/final"
SUCCESS_MSG="Dumping finished successfully"
REPLICA_HOST="ec2-54-242-40-47.compute-1.amazonaws.com"
REPLICA_PORT=6379

# Migration mode configuration
USE_LAZY_PAGES="${USE_LAZY_PAGES:-false}"
SOURCE_HOST="${SOURCE_HOST:-ec2-54-242-40-47.compute-1.amazonaws.com}"
LAZY_PAGES_PORT="${LAZY_PAGES_PORT:-9001}"

# ============================================
# Setup
# ============================================
echo "🔧 Configuration:"
if [ "$USE_LAZY_PAGES" = "true" ]; then
  echo "  Mode: Lazy Pages (network on-demand)"
  echo "  Source: $SOURCE_HOST:$LAZY_PAGES_PORT"
else
  echo "  Mode: Traditional (FSx restore)"
fi
echo

# Ensure necessary directories and files exist
echo "📁 Preparing restore environment..."
sudo mkdir -p /var/log/valkey /var/lib/valkey
sudo chown ubuntu:ubuntu /var/log/valkey /var/lib/valkey
sudo chmod 755 /var/log/valkey /var/lib/valkey

# Create log files with correct permissions
sudo touch /var/log/valkey/stdout.log /var/log/valkey/stderr.log
sudo chown ubuntu:ubuntu /var/log/valkey/stdout.log /var/log/valkey/stderr.log
sudo chmod 660 /var/log/valkey/stdout.log /var/log/valkey/stderr.log

if [ "$USE_LAZY_PAGES" = "true" ]; then
  # ============================================
  # LAZY PAGES MODE
  # ============================================
  echo "📡 Lazy pages mode: Restore with on-demand page fetching"
  echo
  echo "⚠️  Make sure source lazy-pages server is running!"
  echo "   Source command: sudo USE_LAZY_PAGES=true ./dump_valkey.sh"
  echo
  read -p "Press Enter when source is ready..."
  
  # Wait for dump completion marker
  echo "📡 Watching $LOG_FILE for completion message..."
  while [[ ! -f "$LOG_FILE" ]]; do
    printf "."
    sleep 0.1
  done
  
  while ! sudo grep -q "$SUCCESS_MSG" "$LOG_FILE" 2>/dev/null; do
    printf "."
    sleep 0.1
  done
  
  echo
  echo "✅ Dump completed — starting lazy restore..."
  
  # Perform lazy restore
  sudo criu restore \
    -D "$RESTORE_DIR" \
    --tcp-close \
    --ext-unix-sk \
    --lazy-pages \
    --address "$SOURCE_HOST" \
    --port "$LAZY_PAGES_PORT" \
    -v4 \
    -o "$RESTORE_DIR/restore.log"
  
  echo "🚀 Restore completed. Log: $RESTORE_DIR/restore.log"
  
  if sudo grep -q "Restore finished successfully" "$RESTORE_DIR/restore.log"; then
    echo "✅ Restore finished successfully!"
    if pgrep -x valkey-server > /dev/null; then
      pid=$(pgrep -x valkey-server)
      echo "✅ Valkey is running with PID: $pid"
      ps -p "$pid" -o pid,cmd,etime
      echo
      echo "📡 Pages are being fetched on-demand from source"
      echo "   You can stop the source lazy-pages server once all pages are loaded"
    else
      echo "⚠️ Valkey process not found after restore."
    fi
  else
    echo "❌ Restore failed. Last 50 lines of log:"
    sudo tail -n 50 "$RESTORE_DIR/restore.log"
    exit 1
  fi
  
else
  # ============================================
  # TRADITIONAL MODE (FSx restore)
  # ============================================
  echo "📁 Traditional mode: Restore from FSx"
  echo "📡 Watching $LOG_FILE for completion message..."
  echo "Trigger condition: '$SUCCESS_MSG'"
  
  # Wait for file to appear if dump is still running
  while [[ ! -f "$LOG_FILE" ]]; do
    printf "."
    sleep 0.1
  done
  
  # Poll file until the success line appears
  while true; do
    if sudo grep -q "$SUCCESS_MSG" "$LOG_FILE"; then
      echo
      echo "✅ Dump completed successfully — starting restore..."
      
      # Perform restore
      sudo criu restore \
        -D "$RESTORE_DIR" \
        --tcp-close \
        --ext-unix-sk \
        -v4 \
        -o "$RESTORE_DIR/restore.log"
  
      echo "🚀 Restore completed. Log: $RESTORE_DIR/restore.log"
  
      if sudo grep -q "Restore finished successfully" "$RESTORE_DIR/restore.log"; then
        echo "✅ Restore finished successfully!"
        if pgrep -x valkey-server > /dev/null; then
          pid=$(pgrep -x valkey-server)
          echo "✅ Valkey is running with PID: $pid"
          ps -p "$pid" -o pid,cmd,etime
        else
          echo "⚠️ Valkey process not found after restore."
        fi
      else
        echo "❌ Restore failed. Last 50 lines of log:"
        sudo tail -n 50 "$RESTORE_DIR/restore.log"
        exit 1
      fi
      break
    fi
    printf "."
    sleep 0.1
  done
fi
