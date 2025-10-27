#!/bin/bash
set -euo pipefail

LOG_FILE="/fsx/checkpoint1/final/dump.log"
RESTORE_DIR="/fsx/checkpoint1/final"
SUCCESS_MSG="Dumping finished successfully"

echo "📡 Watching $LOG_FILE for completion message..."
echo "Trigger condition: '$SUCCESS_MSG'"

# Wait for file to appear if dump is still running
while [[ ! -f "$LOG_FILE" ]]; do
  sleep 0.1
done

# Poll file until the success line appears
while true; do
  if sudo grep -q "$SUCCESS_MSG" "$LOG_FILE"; then
    echo "✅ Dump completed successfully — starting restore..."
    #sudo criu restore -D "$RESTORE_DIR" --tcp-close --shell-job -v4 -o "$RESTORE_DIR/restore.log" &
    sudo ./start_restore_inner.sh
    echo "🚀 Restore launched in background. Log: $RESTORE_DIR/restore.log"
    break
  fi
  sleep 0.1
done
