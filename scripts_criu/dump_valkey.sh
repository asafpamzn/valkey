#!/bin/bash
set -euo pipefail

BASE_DIR="/fsx/checkpoint1"
PRE1_DIR="$BASE_DIR/pre1"
PRE2_DIR="$BASE_DIR/pre2"
FINAL_DIR="$BASE_DIR/final"

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
  echo
  echo "🧊 ${desc}..."
  local start end
  start=$(date +%s.%N)
  # shellcheck disable=SC2068
  "$@" || { echo "❌ ${desc} failed"; exit 1; }
  end=$(date +%s.%N)
  awk -v s="$start" -v e="$end" 'BEGIN { printf "⏱  %s took %.3f s\n", "", (e - s) }'
}

# ========= Pre-dump #1 =========
PRE1_LOG="$PRE1_DIR/dump.log"
run_phase "Pre-dump #1 (track-mem) to $PRE1_DIR" \
  sudo criu pre-dump \
    -t "$pid" \
    -D "$PRE1_DIR" \
    --track-mem \
    --tcp-close \
    --ext-unix-sk \
    --ghost-limit 64M \
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
    --ghost-limit 64M \
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
    --ghost-limit 64M \
    -v0 -o "$FINAL_LOG"
sudo echo "Dumping finished successfully" > $FINAL_LOG
if sudo grep -q "Dumping finished successfully" "$FINAL_LOG"; then
  echo "✅ Final dump completed successfully. Log: $FINAL_LOG"
else
  echo "⚠️  Final dump did not report success. See $FINAL_LOG"
  tail -n 40 "$FINAL_LOG" || true
  exit 1
fi

echo
echo "📁 Image sets:"
sudo du -sh "$PRE1_DIR" "$PRE2_DIR" "$FINAL_DIR" | sort -h
echo
echo "📝 Tip: restore with:"
echo "  sudo criu restore -D $FINAL_DIR --tcp-close --ext-unix-sk -v0 -o $FINAL_DIR/restore.log"