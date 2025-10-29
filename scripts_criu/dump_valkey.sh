#!/bin/bash
set -euo pipefail

# ============================================
# Configuration
# ============================================
BASE_DIR="/fsx/checkpoint1"
PRE1_DIR="$BASE_DIR/pre1"
PRE2_DIR="$BASE_DIR/pre2"
FINAL_DIR="$BASE_DIR/final"

# Page-server configuration
USE_PAGE_SERVER="${USE_PAGE_SERVER:-false}"
DEST_HOST="${DEST_HOST:-ec2-54-87-52-11.compute-1.amazonaws.com}"
PAGE_SERVER_PORT="${PAGE_SERVER_PORT:-6389}"

# ============================================
# Setup
# ============================================
echo "🔧 Configuration:"
echo "  Mode: $([ "$USE_PAGE_SERVER" = "true" ] && echo "Page-Server (network streaming)" || echo "Traditional (FSx storage)")"
if [ "$USE_PAGE_SERVER" = "true" ]; then
  echo "  Destination: $DEST_HOST:$PAGE_SERVER_PORT"
fi
echo

# Create dirs (need root for image ownership typically)
sudo mkdir -p "$PRE1_DIR" "$PRE2_DIR" "$FINAL_DIR"

# Find valkey-server PID
pid=$(pgrep -x valkey-server || true)
if [[ -z "${pid}" ]]; then
  echo "❌ valkey-server not found. Please start it first."
  exit 1
fi
echo "✅ valkey-server PID: ${pid}"
ps -p "$pid" -o pid,cmd,etime

# Helper to run a CRIU phase and time it
run_phase () {
  local desc="$1"; shift
  local log_file=""
  
  # Extract log file from arguments for error reporting
  for arg in "$@"; do
    if [[ "$arg" == *.log ]]; then
      log_file="$arg"
      break
    fi
  done
  
  echo
  echo "🧊 ${desc}..."
  echo "   Command: $*"
  local start end exit_code
  start=$(date +%s.%N)
  # shellcheck disable=SC2068
  "$@"
  exit_code=$?
  end=$(date +%s.%N)
  
  if [ $exit_code -ne 0 ]; then
    echo "❌ ${desc} failed (exit code: $exit_code)"
    if [ -n "$log_file" ] && [ -f "$log_file" ]; then
      echo "📋 Last 30 lines of log file:"
      sudo tail -n 30 "$log_file" 2>/dev/null || echo "   (could not read log)"
    fi
    exit 1
  fi
  
  awk -v s="$start" -v e="$end" 'BEGIN { printf "⏱  %s took %.3f s\n", "", (e - s) }'
}

# Build page-server flags and directory flags based on mode
PAGE_SERVER_FLAGS=""
PRE1_DIR_FLAG="-D $PRE1_DIR"
PRE2_DIR_FLAG="-D $PRE2_DIR"
PRE1_PREV_DIR_FLAG=""
PRE2_PREV_DIR_FLAG="--prev-images-dir $PRE1_DIR"

if [ "$USE_PAGE_SERVER" = "true" ]; then
  PAGE_SERVER_FLAGS="--page-server --address $DEST_HOST --port $PAGE_SERVER_PORT"
  # In page-server mode, pre-dumps don't use -D flag (pages stream to network)
  PRE1_DIR_FLAG=""
  PRE2_DIR_FLAG=""
  # prev-images-dir not needed in page-server mode for pre-dumps
  PRE2_PREV_DIR_FLAG=""
  echo "📡 Page-server mode enabled - will stream pages to $DEST_HOST:$PAGE_SERVER_PORT"
  echo "   Pre-dumps will stream to network, final dump will write to $FINAL_DIR"
fi

# ========= Pre-dump #1 =========
PRE1_LOG="$PRE1_DIR/dump.log"
if [ "$USE_PAGE_SERVER" = "true" ]; then
  run_phase "Pre-dump #1 (streaming to page-server)" \
    sudo criu pre-dump \
      -t "$pid" \
      --track-mem \
      --tcp-close \
      --ext-unix-sk \
      --ghost-limit 8M \
      $PAGE_SERVER_FLAGS \
      -v4 -o "$PRE1_LOG"
else
  run_phase "Pre-dump #1 (track-mem) to $PRE1_DIR" \
    sudo criu pre-dump \
      -t "$pid" \
      -D "$PRE1_DIR" \
      --track-mem \
      --tcp-close \
      --ext-unix-sk \
      --ghost-limit 8M \
      -v0 -o "$PRE1_LOG"
fi

# ========= Pre-dump #2 =========
PRE2_LOG="$PRE2_DIR/dump.log"
if [ "$USE_PAGE_SERVER" = "true" ]; then
  run_phase "Pre-dump #2 (streaming to page-server)" \
    sudo criu pre-dump \
      -t "$pid" \
      --track-mem \
      --tcp-close \
      --ext-unix-sk \
      --ghost-limit 8M \
      $PAGE_SERVER_FLAGS \
      -v4 -o "$PRE2_LOG"
else
  run_phase "Pre-dump #2 (track-mem, delta vs pre1) to $PRE2_DIR" \
    sudo criu pre-dump \
      -t "$pid" \
      -D "$PRE2_DIR" \
      --track-mem \
      --prev-images-dir "$PRE1_DIR" \
      --tcp-close \
      --ext-unix-sk \
      --ghost-limit 8M \
      -v0 -o "$PRE2_LOG"
fi

# ========= Final dump (leave-running) =========
FINAL_LOG="$FINAL_DIR/dump.log"
if [ "$USE_PAGE_SERVER" = "true" ]; then
  run_phase "Final dump (leave-running, delta from page-server) to $FINAL_DIR" \
    sudo criu dump \
      -t "$pid" \
      -D "$FINAL_DIR" \
      --track-mem \
      --leave-running \
      --tcp-close \
      --ext-unix-sk \
      --ghost-limit 8M \
      $PAGE_SERVER_FLAGS \
      -v4 -o "$FINAL_LOG"
else
  run_phase "Final dump (leave-running, delta vs pre2) to $FINAL_DIR" \
    sudo criu dump \
      -t "$pid" \
      -D "$FINAL_DIR" \
      --track-mem \
      --prev-images-dir "$PRE2_DIR" \
      --leave-running \
      --tcp-close \
      --ext-unix-sk \
      --ghost-limit 8M \
      -v0 -o "$FINAL_LOG"
   sudo echo "Dumping finished successfully" > $FINAL_LOG
fi

# Check if dump succeeded by looking for CRIU's success indicator
if sudo grep -q "Notify success" "$FINAL_LOG" 2>/dev/null || [ $? -eq 0 ]; then
  echo "✅ Final dump completed successfully. Log: $FINAL_LOG"
  # Write success marker for restore script
  echo "Dumping finished successfully" | sudo tee -a "$FINAL_LOG" > /dev/null
else
  echo "⚠️  Final dump may have issues. See $FINAL_LOG"
  sudo tail -n 40 "$FINAL_LOG" 2>/dev/null || true
  exit 1
fi

echo
echo "📁 Image sets:"
sudo du -sh "$PRE1_DIR" "$PRE2_DIR" "$FINAL_DIR" 2>/dev/null | sort -h || echo "  (size calculation skipped)"
echo
echo "📝 Tip: restore with:"
if [ "$USE_PAGE_SERVER" = "true" ]; then
  echo "  USE_PAGE_SERVER=true ./auto_restore.sh"
else
  echo "  sudo criu restore -D $FINAL_DIR --tcp-close --ext-unix-sk -v0 -o $FINAL_DIR/restore.log"
fi
