# Valkey CRIU Live Migration Scripts

These scripts enable live migration of Valkey server between EC2 instances using CRIU (Checkpoint/Restore In Userspace).

## Overview

The scripts support two modes:

1. **Traditional Mode**: Dumps to shared FSx storage (~1 second freeze time)
2. **Page-Server Mode**: Streams memory over network (~50-200ms freeze time) ⚡

## Architecture

- **Source EC2**: `ec2-54-242-40-47.compute-1.amazonaws.com` (runs `dump_valkey.sh`)
- **Destination EC2**: `ec2-54-87-52-11.compute-1.amazonaws.com` (runs `auto_restore.sh`)
- **Shared Storage**: `/fsx/checkpoint1/` (FSx for Lustre)
- **Page-Server Port**: `6389`

## Quick Start

### Traditional Mode (Default)

**On Destination EC2:**
```bash
cd /Users/asafp/work/valkey/scripts_criu
./auto_restore.sh
```

**On Source EC2:**
```bash
cd /Users/asafp/work/valkey/scripts_criu
./dump_valkey.sh
```

### Page-Server Mode (Recommended for Low Latency)

**On Destination EC2 (start first):**
```bash
cd /Users/asafp/work/valkey/scripts_criu
USE_PAGE_SERVER=true ./auto_restore.sh
```

**On Source EC2 (start after destination is ready):**
```bash
cd /Users/asafp/work/valkey/scripts_criu
sudo USE_PAGE_SERVER=true ./dump_valkey.sh
```
```

## Configuration

### Environment Variables

Both scripts support these environment variables:

#### dump_valkey.sh
- `USE_PAGE_SERVER`: Enable page-server mode (`true`/`false`, default: `false`)
- `DEST_HOST`: Destination hostname (default: `ec2-54-87-52-11.compute-1.amazonaws.com`)
- `PAGE_SERVER_PORT`: Port for page-server (default: `6389`)

#### auto_restore.sh
- `USE_PAGE_SERVER`: Enable page-server mode (`true`/`false`, default: `false`)
- `PAGE_SERVER_PORT`: Port to listen on (default: `6389`)

### Example with Custom Configuration

```bash
# On destination
USE_PAGE_SERVER=true PAGE_SERVER_PORT=7000 ./auto_restore.sh

# On source
USE_PAGE_SERVER=true DEST_HOST=10.0.1.100 PAGE_SERVER_PORT=7000 ./dump_valkey.sh
```

## How It Works

### Traditional Mode

1. Source dumps memory to `/fsx/checkpoint1/pre1/`, `/fsx/checkpoint1/pre2/`, `/fsx/checkpoint1/final/`
2. Destination watches for completion marker in log file
3. Destination restores from `/fsx/checkpoint1/final/`

**Pros:**
- Simple setup
- No network configuration needed

**Cons:**
- Slower (~1 second freeze time)
- High I/O on shared storage

### Page-Server Mode

1. Destination starts CRIU page-server listening on port 6389
2. Source connects and **streams memory pages directly over network** during pre-dumps (no FSx writes)
3. Final dump writes only metadata and small delta to `/fsx/checkpoint1/final/`
4. Destination stops page-server and restores from received pages + final metadata

**Pros:**
- Much faster (~50-200ms freeze time) ⚡
- **Dramatically reduced I/O on shared storage** (only final metadata written to FSx)
- Memory pages transferred over network instead of through FSx
- Better for live migration

**Cons:**
- Requires network connectivity on port 6389
- Slightly more complex setup

## Network Requirements

For page-server mode, ensure:

1. **Security Group Rules**: Allow TCP port 6389 from source to destination
2. **Network Connectivity**: Source can reach destination on port 6389
3. **Firewall**: No firewall blocking port 6389

Test connectivity:
```bash
# On source EC2
nc -zv ec2-54-87-52-11.compute-1.amazonaws.com 6389
```

## Performance Comparison

| Mode | Freeze Time | FSx I/O | Network | Memory Transfer | Best For |
|------|-------------|---------|---------|-----------------|----------|
| Traditional | ~1 second | High (all dumps to FSx) | Low | Via FSx | Testing, simple setups |
| Page-Server | ~50-200ms | Minimal (only final metadata) | Medium | Direct network streaming | Production, live migration |

**Key Difference**: In page-server mode, pre-dumps stream memory pages directly over the network to the destination, bypassing FSx entirely. Only the final dump writes minimal metadata to FSx.

## Troubleshooting

### Page-Server Mode Issues

**Problem**: "Connection refused" on source
```bash
# Check if page-server is running on destination
sudo netstat -tlnp | grep 6389

# Check page-server logs
sudo tail -f /fsx/checkpoint1/page-server.log
```

**Problem**: High freeze time even with page-server
- Check network latency between instances
- Verify memory change rate (high churn = longer freeze)
- Consider reducing to single pre-dump

### General Issues

**Problem**: "valkey-server not found"
```bash
# Ensure Valkey is running
pgrep -x valkey-server
ps aux | grep valkey-server
```

**Problem**: Permission errors
```bash
# Ensure proper permissions
sudo chown -R ubuntu:ubuntu /fsx/checkpoint1/
sudo chmod -R 755 /fsx/checkpoint1/
```

## Files

- `dump_valkey.sh`: Checkpoint script (runs on source)
- `auto_restore.sh`: Restore script (runs on destination)
- `valkey.conf`: Valkey configuration
- `wait_and_replicate.sh`: Post-restore replication setup

## Logs

- Dump logs: `/fsx/checkpoint1/{pre1,pre2,final}/dump.log`
- Restore log: `/fsx/checkpoint1/final/restore.log`
- Page-server log: `/fsx/checkpoint1/page-server.log` (page-server mode only)

## Advanced Usage

### Single Pre-dump (Faster)

For even faster checkpoints, modify `dump_valkey.sh` to use only one pre-dump instead of two. This reduces total time but may increase final freeze time slightly.

### Custom Ghost Limit

The scripts use `--ghost-limit 8M`. Adjust if needed:
- Lower (4M): Faster scanning, more files
- Higher (16M): Slower scanning, fewer files

### Verbosity

Scripts use `-v0` (silent) for maximum performance. For debugging, change to `-v4`:
```bash
# In dump_valkey.sh and auto_restore.sh, change:
-v0  # to:
-v4
```

## Best Practices

1. **Always start destination first** when using page-server mode
2. **Test network connectivity** before production migration
3. **Monitor freeze time** and adjust pre-dump frequency if needed
4. **Use page-server mode** for production live migrations
5. **Keep logs** for troubleshooting

## Support

For issues or questions:
1. Check logs in `/fsx/checkpoint1/`
2. Verify network connectivity (page-server mode)
3. Ensure Valkey is running before checkpoint
4. Check CRIU version: `criu --version` (requires 3.15+)
