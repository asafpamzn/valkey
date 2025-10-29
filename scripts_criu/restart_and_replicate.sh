#!/bin/bash
set -euo pipefail

# ============================================
# Restart Valkey and Configure Replication
# ============================================
# This script:
# 1. Kills any running valkey-server process (SIGKILL)
# 2. Restarts valkey-server with valkey.conf
# 3. Calls wait_and_replicate.sh to configure replication

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VALKEY_CONF="$SCRIPT_DIR/valkey.conf"
WAIT_REPLICATE_SCRIPT="$SCRIPT_DIR/wait_and_replicate.sh"

echo "🔄 Restarting Valkey and configuring replication..."
echo

# Step 1: Kill valkey-server (SIGKILL)
sudo rm /var/lib/valkey/dump.rdb
echo "🔪 Killing valkey-server..."
pkill -9 valkey-server || true
echo "✅ valkey-server killed"
echo

# Step 2: Start valkey-server with config
echo "🚀 Starting valkey-server with $VALKEY_CONF..."
sudo valkey-server "$VALKEY_CONF" &
echo "✅ valkey-server started"
echo

# Step 3: Configure replication
echo "🔁 Configuring replication..."
"$WAIT_REPLICATE_SCRIPT"

echo
echo "✅ Done! Valkey is running and configured as replica."
