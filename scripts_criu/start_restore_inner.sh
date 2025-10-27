#!/bin/bash
set -euo pipefail

RESTORE_DIR="/fsx/checkpoint1/final"
RESTORE_LOG="$RESTORE_DIR/restore.log"

# Enter a private mount namespace so we can mount our own devpts
# (Requires CAP_SYS_ADMIN / sudo)
sudo unshare -m -- bash -eu -o pipefail <<'EOS'
set -euo pipefail

# Ensure proc and sys are mounted (some environments need this)
mountpoint -q /proc || mount -t proc proc /proc
mountpoint -q /sys  || mount -t sysfs sysfs /sys

# Make sure /dev/pts exists
mkdir -p /dev/pts

# Mount a fresh devpts instance so CRIU can recreate the exact PTY indices.
# Use the 'tty' group if present; fall back to gid 5.
TTY_GID="$(getent group tty | cut -d: -f3 || true)"
TTY_GID="${TTY_GID:-5}"

mount -t devpts devpts /dev/pts -o newinstance,ptmxmode=0666,mode=0620,gid=${TTY_GID}

# Ensure /dev/ptmx points at our new instance’s ptmx
if [ ! -L /dev/ptmx ]; then
  # Some systems have a device node; replace with symlink into /dev/pts
  rm -f /dev/ptmx
  ln -s /dev/pts/ptmx /dev/ptmx
fi

# Optional: avoid managing cgroups during restore on systemd/cgroup v2 hosts
# (prevents the "cgroupd: recv req error" noise)
MANAGE_CGROUPS="--manage-cgroups=none"

# Run the restore
criu restore \
  -D /fsx/checkpoint1/final \
  --tcp-close \
  --shell-job \
  ${MANAGE_CGROUPS} \
  -v4 -o /fsx/checkpoint1/final/restore.log
EOS

echo "✅ Restore attempted. See log: $RESTORE_LOG"

