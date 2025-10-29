#!/bin/bash
set -euo pipefail

# ============================================
# Configuration
# ============================================
BASE_DIR="/fsx/checkpoint1"
PRE1_DIR="$BASE_DIR/pre1"
PRE2_DIR="$BASE_DIR/pre2"
FINAL_DIR="$BASE_DIR/final"

# Migration mode configuration
USE_LAZY_PAGES="${USE_LAZY_PAGES:-false}"
DEST_HOST="${DEST_HOST:-54.87.52.11}"
LAZY_PAGES_PORT="${LAZY_PAGES_PORT:-9001}"
SRC_HOST="${SRC_HOST:-0.0.0.0}"

# ============================================
# Setup
# ============================================
echo "🔧 Configuration:"
if [ "$USE_LAZY_PAGES" = "true" ]; then
  echo "  Mode: Lazy Pages (network on-demand)"
  echo "  Destination: $DEST_HOST:$LAZY_PAGES_PORT"
else
  echo "  Mode: Traditional (FSx with pre-dumps)"
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

if [ "$USE_LAZY_PAGES" = "true" ]; then
  echo "📡 Lazy pages mode: Dump + start lazy-pages server (source listens)"
  echo "   Pages will be served on-demand from memory (not written to disk)"

  FINAL_LOG="$FINAL_DIR/dump.log"
run_phase "Dump (leave-running, lazy-pages) to $FINAL_DIR" \
    sudo criu dump \
      --tree "$pid" \
      --images-dir  "$FINAL_DIR" \
      --address "$SRC_HOST"\
      --lazy-pages \
      --port "$LAZY_PAGES_PORT" \
      --tcp-close \
      --ext-unix-sk \
      --ghost-limit 8M \
      -v4 -o "$FINAL_LOG"
  sudo echo "Dumping finished successfully" > $FINAL_LOG
  echo "✅ Dump completed successfully. Log: $FINAL_LOG"

else
  # ============================================
  # TRADITIONAL MODE (FSx with pre-dumps)
  # ============================================
  echo "📁 Traditional mode: Pre-dumps + final dump to FSx"
  
  # ========= Pre-dump #1 =========
  PRE1_LOG="$PRE1_DIR/dump.log"
  run_phase "Pre-dump #1 (track-mem) to $PRE1_DIR" \
    sudo criu pre-dump \
      -t "$pid" \
      -D "$PRE1_DIR" \
      --track-mem \
      --tcp-close \
      --ext-unix-sk \
      --ghost-limit 8M \
      -v0 -o "$PRE1_LOG"
  
  # ========= Pre-dump #2 =========
  PRE2_LOG="$PRE2_DIR/dump.log"
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
  
  # ========= Final dump (leave-running) =========
  FINAL_LOG="$FINAL_DIR/dump.log"
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
  # Check if dump succeeded
  if sudo grep -q "Notify success" "$FINAL_LOG" 2>/dev/null; then
    echo "✅ Final dump completed successfully. Log: $FINAL_LOG"
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
  echo "  sudo ./auto_restore.sh"
fi
