# Valkey CRIU Live Migration Scripts

These scripts enable live migration of Valkey server between EC2 instances using CRIU (Checkpoint/Restore In Userspace).

## Overview

The scripts support two migration modes:

1. **Traditional Mode** (default): Pre-dumps + final dump to shared FSx storage
2. **Lazy Pages Mode**: Single dump + on-demand page fetching over network

## Architecture

- **Source EC2**: `ec2-54-242-40-47.compute-1.amazonaws.com` (runs `dump_valkey.sh`)
- **Destination EC2**: `ec2-54-87-52-11.compute-1.amazonaws.com` (runs `auto_restore.sh`)
- **Shared Storage**: `/fsx/checkpoint1/` (FSx for Lustre)
- **Lazy Pages Port**: `9001`

## Environment Setup

Before running the migration scripts, you need to set up your EC2 instance with Valkey and the necessary dependencies.

### Prerequisites

- Ubuntu 24.04 LTS (or compatible)
- SSH access to EC2 instance
- Git installed

### Setup Steps

1. **SSH to your EC2 instance:**
   ```bash
   ssh ubuntu@ec2-xx-xxx-xx-xxx.compute-1.amazonaws.com
   ```

2. **Create working directory:**
   ```bash
   mkdir work
   cd work
   ```

3. **Clone the Valkey repository:**
   ```bash
   git clone https://github.com/asafpamzn/valkey.git
   cd valkey
   ```

4. **Checkout the CRIU branch:**
   ```bash
   git checkout criu-sync
   ```

5. **Navigate to scripts directory:**
   ```bash
   cd scripts_criu
   ```

6. **Update package lists:**
   ```bash
   sudo apt update
   ```

7. **Install Valkey:**
   ```bash
   sudo apt install valkey
   ```
   
   This will install:
   - `valkey-server` - The Valkey server
   - `valkey-tools` - CLI tools (valkey-cli, valkey-benchmark, etc.)
   - Dependencies: `libatomic1`, `libjemalloc2`, `liblzf1`

8. **Verify installation:**
   ```bash
   valkey-server --version
   valkey-cli --version
   ```

### CRIU-Specific Configuration

For CRIU to work properly with Valkey, additional configuration is required. CRIU needs the process to run in the same namespace as the user and requires file-based logging instead of systemd journal.

   ```bash

    sudo add-apt-repository ppa:criu/ppa -y
    sudo apt update
    sudo apt install criu -y
    ```

9. **Copy custom Valkey configuration:**
   ```bash
   sudo cp ~/work/valkey/scripts_criu/valkey.conf /etc/valkey/valkey.conf
   ```

10. **Update systemd service file for CRIU compatibility:**
    ```bash
    sudo nano /etc/systemd/system/valkey-server.service
    ```
    
    Replace the content with:
    ```ini
    [Unit]
    Description=Advanced key-value store
    After=network.target
    ConditionPathExists=!/etc/valkey/REDIS_MIGRATION
    Documentation=https://valkey.io/docs/, man:valkey-server(1)

    [Service]
    Type=notify
    ExecStart=/usr/bin/valkey-server /etc/valkey/valkey.conf --supervised systemd --daemonize no
    PIDFile=/run/valkey/valkey-server.pid
    TimeoutStopSec=0
    Restart=always
    User=ubuntu
    Group=ubuntu

    # Redirect output away from systemd journal to avoid socket issues with CRIU
    StandardOutput=file:/var/log/valkey/stdout.log
    StandardError=file:/var/log/valkey/stderr.log

    UMask=007
    LimitNOFILE=65535

    # Namespace isolation DISABLED for same namespace as user (required for CRIU)
    PrivateTmp=no
    PrivateDevices=no
    PrivateUsers=no
    ProtectHome=no
    ProtectSystem=no
    ProtectClock=no
    ProtectControlGroups=no
    ProtectHostname=no
    ProtectKernelLogs=no
    ProtectKernelModules=no
    ProtectKernelTunables=no
    NoNewPrivileges=no
    RestrictNamespaces=no

    # Keep some basic security restrictions
    LockPersonality=true
    RestrictRealtime=true
    RestrictSUIDSGID=true

    [Install]
    WantedBy=multi-user.target
    Alias=valkey.service
    ```
    
    **Key changes for CRIU:**
    - Runs as `ubuntu:ubuntu` user (not root)
    - Logs redirected to files (`/var/log/valkey/`) instead of systemd journal
    - All namespace isolation disabled (CRIU requires same namespace)
    - Basic security restrictions maintained

11. **Set up directories and permissions:**
    ```bash
    # Create necessary directories
    sudo mkdir -p /run/valkey /var/log/valkey
    
    # Set ownership to ubuntu user
    sudo chown ubuntu:ubuntu /run/valkey
    sudo chown ubuntu:ubuntu /etc/valkey
    sudo chown -R ubuntu:ubuntu /var/lib/valkey
    sudo chown -R ubuntu:ubuntu /var/log/valkey
    sudo chown ubuntu:ubuntu /etc/valkey/valkey.conf
    
    # Set permissions
    sudo chmod 755 /run/valkey
    sudo chmod 755 /etc/valkey
    ```

12. **Reload systemd and restart Valkey:**
    ```bash
    sudo systemctl daemon-reload
    sudo systemctl restart valkey-server
    sudo systemctl status valkey-server
    ```
    
    Verify Valkey is running:
    ```bash
    valkey-cli ping
    # Should return: PONG
    ```

Now you're ready to run the migration scripts!

## Quick Start

### Traditional Mode (Default)

**On Destination EC2:**
```bash
cd /path/to/valkey/scripts_criu
sudo ./auto_restore.sh
```

**On Source EC2:**
```bash
cd /path/to/valkey/scripts_criu
sudo ./dump_valkey.sh
```

### Lazy Pages Mode (Recommended for Fastest Startup)

**On Source EC2 (start first):**
```bash
cd /path/to/valkey/scripts_criu
sudo USE_LAZY_PAGES=true ./dump_valkey.sh
# Keep this terminal open - lazy-pages server will run here
```

**On Destination EC2:**
```bash
cd /path/to/valkey/scripts_criu
sudo USE_LAZY_PAGES=true ./auto_restore.sh
# Press Enter when prompted (after source is ready)
```

**After restore completes:**
- Stop the source lazy-pages server with Ctrl+C
- Valkey is now running on destination

## Configuration

### Environment Variables

#### dump_valkey.sh
- `USE_LAZY_PAGES`: Enable lazy pages mode (`true`/`false`, default: `false`)
- `DEST_HOST`: Destination hostname to send pages to (default: `ec2-54-87-52-11.compute-1.amazonaws.com`)
- `LAZY_PAGES_PORT`: Port for lazy-pages server (default: `9001`)

#### auto_restore.sh
- `USE_LAZY_PAGES`: Enable lazy pages mode (`true`/`false`, default: `false`)
- `SOURCE_HOST`: Source hostname to fetch pages from (default: `ec2-54-242-40-47.compute-1.amazonaws.com`)
- `LAZY_PAGES_PORT`: Port to connect to (default: `9001`)

### Example with Custom Configuration

```bash
# On source (sends pages TO destination)
sudo USE_LAZY_PAGES=true DEST_HOST=10.0.1.100 LAZY_PAGES_PORT=9002 ./dump_valkey.sh

# On destination (fetches pages FROM source)
sudo USE_LAZY_PAGES=true SOURCE_HOST=10.0.1.50 LAZY_PAGES_PORT=9002 ./auto_restore.sh
```

## How It Works

### Traditional Mode

1. Source performs two pre-dumps to `/fsx/checkpoint1/pre1/` and `/fsx/checkpoint1/pre2/`
2. Source performs final dump to `/fsx/checkpoint1/final/`
3. Destination watches for completion marker in log file
4. Destination restores from `/fsx/checkpoint1/final/`

**Pros:**
- Simple, reliable
- No network configuration needed
- Well-tested approach

**Cons:**
- High I/O load on FSx
- Slower freeze time (~1-2 seconds)

### Lazy Pages Mode

1. Source performs single dump to `/fsx/checkpoint1/final/`
2. Source starts lazy-pages server on port 9001
3. Destination restores with `--lazy-pages` flag
4. Destination starts immediately, fetches pages on-demand from source
5. Pages transferred over network as needed

**Pros:**
- **Fastest startup** (~100-200ms) ⚡
- Minimal FSx I/O (only metadata)
- True live migration experience
- Pages fetched on-demand over fast network

**Cons:**
- Requires network connectivity on port 9001
- Source must keep lazy-pages server running until pages are loaded
- Slightly more complex coordination

## Network Requirements

For lazy pages mode, ensure:

1. **Security Group Rules**: Allow TCP port 9001 from destination to source
2. **Network Connectivity**: Destination can reach source on port 9001
3. **Firewall**: No firewall blocking port 9001

Test connectivity:
```bash
# On destination EC2
nc -zv ec2-54-242-40-47.compute-1.amazonaws.com 9001
```

## Performance Comparison

| Mode | Freeze Time | Startup Time | FSx I/O | Network | Best For |
|------|-------------|--------------|---------|---------|----------|
| Traditional | ~1-2 seconds | N/A | High (all dumps) | Low | Testing, simple setups |
| Lazy Pages | ~1-2 seconds | ~100-200ms | Minimal (metadata only) | Medium | Production, live migration |

**Key Difference**: Lazy pages mode allows destination to start serving requests almost immediately while fetching memory pages in the background.

## Troubleshooting

### Lazy Pages Mode Issues

**Problem**: "Can't connect to lazy-pages server"
```bash
# Check if lazy-pages server is running on source
sudo netstat -tlnp | grep 9001

# Check lazy-pages logs
sudo tail -f /fsx/checkpoint1/final/lazy-pages.log
```

**Problem**: Slow page fetching
- Check network latency between instances
- Verify network bandwidth is sufficient
- Consider using enhanced networking on EC2 instances

### Traditional Mode Issues

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

### Traditional Mode
- Dump logs: `/fsx/checkpoint1/{pre1,pre2,final}/dump.log`
- Restore log: `/fsx/checkpoint1/final/restore.log`

### Lazy Pages Mode
- Dump log: `/fsx/checkpoint1/final/dump.log`
- Lazy-pages log: `/fsx/checkpoint1/final/lazy-pages.log`
- Restore log: `/fsx/checkpoint1/final/restore.log`

## Advanced Usage

### Verbosity

Scripts use `-v4` (verbose) for debugging. For production, you can modify to `-v0` (silent):
```bash
# In dump_valkey.sh and auto_restore.sh, change:
-v4  # to:
-v0
```

### Custom Ghost Limit

The scripts use `--ghost-limit 8M`. Adjust if needed:
- Lower (4M): Faster scanning, more files
- Higher (16M): Slower scanning, fewer files

## Best Practices

1. **Use lazy pages mode** for production live migrations
2. **Test network connectivity** before migration (especially for lazy pages)
3. **Monitor freeze time** and adjust approach if needed
4. **Keep logs** for troubleshooting
5. **Ensure sufficient network bandwidth** for lazy pages mode

## Workflow Examples

### Traditional Mode Workflow
```bash
# Terminal 1 (Destination)
sudo ./auto_restore.sh

# Terminal 2 (Source)
sudo ./dump_valkey.sh

# Result: Valkey migrated via FSx
```

### Lazy Pages Mode Workflow
```bash
# Terminal 1 (Source) - Start first, keep open
sudo USE_LAZY_PAGES=true ./dump_valkey.sh
# Wait for "Ready to serve pages" message

# Terminal 2 (Destination)
sudo USE_LAZY_PAGES=true ./auto_restore.sh
# Press Enter when prompted
# Valkey starts immediately!

# Terminal 1 (Source) - After restore completes
# Press Ctrl+C to stop lazy-pages server
```

## Support

For issues or questions:
1. Check logs in `/fsx/checkpoint1/`
2. Verify network connectivity (lazy pages mode)
3. Ensure Valkey is running before checkpoint
4. Check CRIU version: `criu --version` (requires 3.15+)
5. Verify lazy-pages support: `criu check --feature lazy_pages`
