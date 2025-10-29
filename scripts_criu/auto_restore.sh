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

# Page-server configuration
USE_PAGE_SERVER="${USE_PAGE_SERVER:-false}"
PAGE_SERVER_PORT="${PAGE_SERVER_PORT:-6389}"
BASE_DIR="/fsx/checkpoint1"

# ============================================
# Setup
# ============================================
echo "🔧 Configuration:"
echo "  Mode: $([ "$USE_PAGE_SERVER" = "true" ] && echo "Page-Server (network streaming)" || echo "Traditional (FSx storage)")"
if [ "$USE_PAGE_SERVER" = "true" ]; then
  echo "  Listening on port: $PAGE_SERVER_PORT"
fi
echo

# Ensure necessary directories and files exist BEFORE waiting for dump
echo "📁 Preparing restore environment..."
sudo mkdir -p /var/log/valkey /var/lib/valkey
sudo chown ubuntu:ubuntu /var/log/valkey /var/lib/valkey

# Set correct directory permissions (0755, not 0750)
sudo chmod 755 /var/log/valkey /var/lib/valkey

# Create log files with CORRECT permissions (0660, not 0644)
sudo touch /var/log/valkey/stdout.log /var/log/valkey/stderr.log
sudo chown ubuntu:ubuntu /var/log/valkey/stdout.log /var/log/valkey/stderr.log
sudo chmod 660 /var/log/valkey/stdout.log /var/log/valkey/stderr.log

# ============================================
# Start Page-Server if enabled
# ============================================
PAGE_SERVER_PID=""
if [ "$USE_PAGE_SERVER" = "true" ]; then
  echo "🚀 Starting CRIU page-server on port $PAGE_SERVER_PORT..."
  
  # Ensure base directories exist
  sudo mkdir -p "$BASE_DIR/pre1" "$BASE_DIR/pre2" "$BASE_DIR/final"
  
  # Start page-server in background
  sudo criu page-server \
    --images-dir "$BASE_DIR" \
    --port "$PAGE_SERVER_PORT" \
    -v0 \
    -o "$BASE_DIR/page-server.log" &
  
  PAGE_SERVER_PID=$!
  
  # Wait a moment for page-server to start
  sleep 2
  
  # Verify page-server is running
  if sudo kill -0 "$PAGE_SERVER_PID" 2>/dev/null; then
    echo "✅ Page-server started successfully (PID: $PAGE_SERVER_PID)"
    echo "📡 Ready to receive memory pages from source..."
  else
    echo "❌ Failed to start page-server. Check $BASE_DIR/page-server.log"
    sudo tail -n 20 "$BASE_DIR/page-server.log" 2>/dev/null || true
    exit 1
  fi
  
  # Cleanup function to stop page-server on exit
  cleanup_page_server() {
    if [ -n "$PAGE_SERVER_PID" ] && sudo kill -0 "$PAGE_SERVER_PID" 2>/dev/null; then
      echo "🛑 Stopping page-server (PID: $PAGE_SERVER_PID)..."
      sudo kill "$PAGE_SERVER_PID" 2>/dev/null || true
      sleep 1
    fi
  }
  trap cleanup_page_server EXIT
fi

# ============================================
# Wait for Dump Completion
# ============================================
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
    
    # Stop page-server if it was running
    if [ "$USE_PAGE_SERVER" = "true" ] && [ -n "$PAGE_SERVER_PID" ]; then
      echo "🛑 Stopping page-server before restore..."
      sudo kill "$PAGE_SERVER_PID" 2>/dev/null || true
      sleep 1
      PAGE_SERVER_PID=""  # Clear PID so cleanup doesn't try again
    fi
    
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
