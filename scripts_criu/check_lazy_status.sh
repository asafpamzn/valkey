#!/usr/bin/env bash
# check_lazy_status.sh — monitor CRIU lazy migration progress and completion
# Usage:
#   ./check_lazy_status.sh [--lazy-log /var/log/valkey/lazy-pages.log] [--page-log /fsx/checkpoint1/final/page-server.log]
#                          [--valkey-port 6379] [--interval 2]

set -euo pipefail

LAZY_LOG="/var/log/valkey/lazy-pages.log"
PAGE_LOG="/fsx/checkpoint1/final/page-server.log"
VALKEY_PORT=6379
INTERVAL=2

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lazy-log)   LAZY_LOG="$2"; shift 2;;
    --page-log)   PAGE_LOG="$2"; shift 2;;
    --valkey-port) VALKEY_PORT="$2"; shift 2;;
    --interval)   INTERVAL="$2"; shift 2;;
    -h|--help)
      echo "Usage: $0 [--lazy-log FILE] [--page-log FILE] [--valkey-port N] [--interval SECS]"
      exit 0;;
    *) echo "Unknown arg: $1" >&2; exit 1;;
  esac
done

# Helper: tail last line safely
tail_last_line() { sudo tail -n 1 "$1" 2>/dev/null || echo ""; }

echo "🔍 Monitoring lazy migration..."
echo "  Lazy log : $LAZY_LOG"
echo "  Page log : $PAGE_LOG"
echo "  Valkey port: $VALKEY_PORT"
echo

lazy_done=0
page_done=0
valkey_ok=0

while true; do
  sleep "$INTERVAL"

  if [[ -f "$LAZY_LOG" ]]; then
    if grep -qE 'UFFD transferred pages: \([0-9]+/[0-9]+\)' "$LAZY_LOG"; then
      lazy_done=1
    fi
  fi

  if [[ -f "$PAGE_LOG" ]]; then
    if grep -q "page-xfer: Session over" "$PAGE_LOG"; then
      page_done=1
    fi
  fi

  if valkey-cli -p "$VALKEY_PORT" ping >/dev/null 2>&1; then
    valkey_ok=1
  fi

  echo "⏱  $(date '+%H:%M:%S')  Status:"
  echo "    - Lazy pages done : $([[ $lazy_done -eq 1 ]] && echo ✅ || echo ⏳)"
  echo "    - Page server done: $([[ $page_done -eq 1 ]] && echo ✅ || echo ⏳)"
  echo "    - Valkey alive    : $([[ $valkey_ok -eq 1 ]] && echo ✅ || echo ⏳)"
  echo

  if (( lazy_done && page_done && valkey_ok )); then
    echo "✅ Lazy migration fully completed!"
    echo "   Safe to kill CRIU processes:"
    echo "     sudo pkill -f 'criu lazy-pages'"
    echo "     sudo pkill -f 'criu page-server'"
    echo "🏁 Valkey is now fully independent and synced."
    exit 0
  fi
done
