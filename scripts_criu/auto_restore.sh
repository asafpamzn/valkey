#!/bin/bash
set -euo pipefail

LOG_FILE="/fsx/checkpoint1/final/dump.log"
RESTORE_DIR="/fsx/checkpoint1/final"
SUCCESS_MSG="Dumping finished successfully"

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
    
    # Check if restore was successful
    if sudo grep -q "Restore finished successfully" "$RESTORE_DIR/restore.log"; then
      echo "✅ Restore finished successfully!"
      
      # Check if valkey is running
      sleep 1
      if pgrep -x valkey-server > /dev/null; then
        pid=$(pgrep -x valkey-server)
        echo "✅ Valkey is running with PID: $pid"
        ps -p "$pid" -o pid,cmd,etime
      else
        echo "⚠️  Valkey process not found after restore"
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