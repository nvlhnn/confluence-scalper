# Deployment Guide — TDB Bot (VPS)

## 1. VPS Requirements

### Minimum Specs

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 1 vCPU | 2 vCPU |
| RAM | 512 MB | 1 GB |
| Storage | 5 GB SSD | 10 GB SSD |
| OS | Ubuntu 22.04 LTS | Ubuntu 22.04 LTS |
| Network | Stable connection | Low latency to Binance (Singapore/Tokyo) |

### Recommended VPS Providers

| Provider | Cheapest Plan | Region | Monthly Cost |
|----------|--------------|--------|-------------|
| DigitalOcean | Basic Droplet | Singapore | ~$6/mo |
| Vultr | Cloud Compute | Tokyo | ~$6/mo |
| Hetzner | CX22 | Germany | ~$4/mo |
| Contabo | VPS S | Singapore | ~$5/mo |

> **Tip:** Choose a server in **Singapore or Tokyo** — closest to Binance's primary servers
> for lowest latency.

---

## 2. Server Setup

### Initial Setup

```bash
# 1. Update system
sudo apt update && sudo apt upgrade -y

# 2. Install Python 3.11+
sudo apt install python3.11 python3.11-venv python3-pip -y

# 3. Install system dependencies
sudo apt install git sqlite3 supervisor -y

# 4. Create bot user (don't run as root)
sudo adduser tdb
sudo usermod -aG sudo tdb
su - tdb

# 5. Clone the project
git clone <your-repo-url> ~/tdb
cd ~/tdb

# 6. Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# 7. Install dependencies
pip install -r requirements.txt

# 8. Configure environment
cp .env.example .env
nano .env  # Add your API keys
```

### Security Hardening

```bash
# 1. Firewall
sudo ufw allow OpenSSH
sudo ufw enable

# 2. SSH key authentication only
sudo nano /etc/ssh/sshd_config
# Set: PasswordAuthentication no
sudo systemctl restart sshd

# 3. Fail2ban
sudo apt install fail2ban -y
sudo systemctl enable fail2ban

# 4. Protect .env file
chmod 600 ~/tdb/.env
```

---

## 3. Process Management (Supervisor)

### Supervisor Config

```bash
sudo nano /etc/supervisor/conf.d/tdb.conf
```

```ini
[program:tdb]
command=/home/tdb/tdb/venv/bin/python /home/tdb/tdb/main.py --mode live
directory=/home/tdb/tdb
user=tdb
autostart=true
autorestart=true
startretries=3
startsecs=10
stopwaitsecs=30
stderr_logfile=/home/tdb/tdb/logs/supervisor_err.log
stdout_logfile=/home/tdb/tdb/logs/supervisor_out.log
stdout_logfile_maxbytes=10MB
stdout_logfile_backups=5
environment=PATH="/home/tdb/tdb/venv/bin:%(ENV_PATH)s"
```

### Supervisor Commands

```bash
# Load config
sudo supervisorctl reread
sudo supervisorctl update

# Control
sudo supervisorctl start tdb
sudo supervisorctl stop tdb
sudo supervisorctl restart tdb
sudo supervisorctl status tdb

# View logs
sudo supervisorctl tail tdb stdout
sudo supervisorctl tail tdb stderr
```

---

## 4. Monitoring

### Health Check Script

```bash
# ~/tdb/scripts/health_check.sh
#!/bin/bash

# Check if bot process is running
if ! supervisorctl status tdb | grep -q RUNNING; then
    echo "TDB bot is NOT running!"
    # Send telegram alert
    curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        -d "chat_id=${TELEGRAM_CHAT_ID}" \
        -d "text=🚨 TDB Bot is DOWN! Restarting..." > /dev/null
    supervisorctl restart tdb
fi
```

```bash
# Add to crontab (check every 5 minutes)
crontab -e
*/5 * * * * /home/tdb/tdb/scripts/health_check.sh
```

### Log Rotation

```bash
# ~/tdb/scripts/rotate_logs.sh
#!/bin/bash
find /home/tdb/tdb/logs -name "*.log" -mtime +30 -delete
```

```bash
# Run daily
0 0 * * * /home/tdb/tdb/scripts/rotate_logs.sh
```

---

## 5. Database Backup

```bash
# ~/tdb/scripts/backup_db.sh
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/home/tdb/tdb/data/backups"
mkdir -p $BACKUP_DIR

# Create backup
sqlite3 /home/tdb/tdb/data/tdb.db ".backup '${BACKUP_DIR}/tdb_${DATE}.db'"

# Remove backups older than 7 days
find $BACKUP_DIR -name "tdb_*.db" -mtime +7 -delete

echo "Backup created: tdb_${DATE}.db"
```

```bash
# Run daily at midnight
0 0 * * * /home/tdb/tdb/scripts/backup_db.sh
```

---

## 6. Update Procedure

```bash
# 1. Stop the bot
sudo supervisorctl stop tdb

# 2. Backup current state
bash ~/tdb/scripts/backup_db.sh

# 3. Pull updates
cd ~/tdb
git pull origin main

# 4. Update dependencies (if changed)
source venv/bin/activate
pip install -r requirements.txt

# 5. Restart
sudo supervisorctl start tdb

# 6. Verify
sudo supervisorctl status tdb
tail -f ~/tdb/logs/tdb.log
```

---

## 7. Testnet → Live Transition

### Pre-Live Checklist

```
□ Ran on testnet for minimum 7 days
□ Executed 30+ paper trades
□ Win rate > 50%
□ Profit factor > 1.2
□ No unhandled errors in logs
□ All circuit breakers tested
□ Telegram notifications working
□ Database backups verified
□ Reviewed all risk parameters
□ API key has ONLY futures trading permission (no withdrawal!)
```

### Go Live

```bash
# 1. Update .env
nano ~/tdb/.env
# Change: BINANCE_TESTNET=false
# Change: BOT_MODE=live

# 2. Verify API key permissions
# Go to Binance → API Management
# Ensure: ✅ Enable Futures  ❌ Enable Withdrawals

# 3. Start with reduced risk for first 10 trades
# Temporarily set risk_per_trade_pct: 1.0 in config

# 4. Restart
sudo supervisorctl restart tdb
```

---

## 8. Emergency Procedures

### Bot Producing Losses

```bash
# 1. Stop immediately
sudo supervisorctl stop tdb

# 2. Check open positions on Binance
# Go to Binance Futures → Positions
# Manually close if needed

# 3. Review logs
tail -200 ~/tdb/logs/tdb.log
tail -200 ~/tdb/logs/trades.log

# 4. Do NOT restart until root cause is found
```

### Server Issues

```bash
# If server is unreachable:
# 1. Binance server-side stop losses will protect open positions
# 2. No NEW trades will be opened
# 3. Fix server access, check positions, restart bot
```

### API Key Compromised

```bash
# 1. IMMEDIATELY disable API key on Binance website
# 2. Create new API key
# 3. Update .env on VPS
# 4. Review account for unauthorized trades
# 5. Restart bot
```
