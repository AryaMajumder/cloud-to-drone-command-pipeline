#!/bin/bash
#
# install_step1.sh - Deploy SQLite-gated agent (Step 1)
#
# This script:
# 1. Creates persistent database directory
# 2. Copies agent files to /opt/drone-command/
# 3. Updates systemd service
# 4. Restarts agent
#
# Run as root or with sudo

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}PX4 Agent Step 1 Installation${NC}"
echo -e "${GREEN}SQLite Command Gating${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Error: Please run as root or with sudo${NC}"
    exit 1
fi

# Configuration
INSTALL_DIR="/opt/drone-command"
DB_DIR="/var/lib/px4-agent"
DB_PATH="${DB_DIR}/commands.db"
SERVICE_NAME="px4-agent"

echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Install dir: ${INSTALL_DIR}"
echo "  Database dir: ${DB_DIR}"
echo "  Database path: ${DB_PATH}"
echo "  Service name: ${SERVICE_NAME}"
echo ""

# Step 1: Create persistent database directory
echo -e "${YELLOW}[1/5] Creating database directory...${NC}"
mkdir -p "${DB_DIR}"
chmod 755 "${DB_DIR}"
echo -e "${GREEN}✓ Created ${DB_DIR}${NC}"

# Step 2: Create install directory
echo -e "${YELLOW}[2/5] Creating install directory...${NC}"
mkdir -p "${INSTALL_DIR}"
echo -e "${GREEN}✓ Created ${INSTALL_DIR}${NC}"

# Step 3: Copy agent files
echo -e "${YELLOW}[3/5] Copying agent files...${NC}"

if [ ! -f "agent_db.py" ]; then
    echo -e "${RED}Error: agent_db.py not found in current directory${NC}"
    exit 1
fi

if [ ! -f "px4_agent.py" ]; then
    echo -e "${RED}Error: px4_agent.py not found in current directory${NC}"
    exit 1
fi

cp agent_db.py "${INSTALL_DIR}/"
cp px4_agent.py "${INSTALL_DIR}/"
chmod +x "${INSTALL_DIR}/px4_agent.py"

echo -e "${GREEN}✓ Copied agent files${NC}"

# Step 4: Update systemd service
echo -e "${YELLOW}[4/5] Updating systemd service...${NC}"

cat > "/etc/systemd/system/${SERVICE_NAME}.service" << EOF
[Unit]
Description=PX4 Companion Agent (Step 1 - SQLite Gating)
After=network.target mosquitto.service
Wants=mosquitto.service

[Service]
Type=simple
User=root
WorkingDirectory=${INSTALL_DIR}
ExecStart=${INSTALL_DIR}/venv/bin/python ${INSTALL_DIR}/px4_agent.py

# Environment variables
Environment=DRONE_ID=drone-01
Environment=MQTT_HOST=127.0.0.1
Environment=MQTT_PORT=1883
Environment=PX4_URL=udp:127.0.0.1:14550
Environment=DB_PATH=${DB_PATH}
Environment=HEARTBEAT_TIMEOUT=10
Environment=LOG_LEVEL=INFO
Environment=ENABLE_CLOUD_LINK_CHECK=false

# Restart policy
Restart=always
RestartSec=5

# Security
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
echo -e "${GREEN}✓ Updated systemd service${NC}"

# Step 5: Restart agent
echo -e "${YELLOW}[5/5] Restarting agent...${NC}"

if systemctl is-active --quiet "${SERVICE_NAME}"; then
    systemctl restart "${SERVICE_NAME}"
    echo -e "${GREEN}✓ Agent restarted${NC}"
else
    systemctl start "${SERVICE_NAME}"
    echo -e "${GREEN}✓ Agent started${NC}"
fi

systemctl enable "${SERVICE_NAME}"

# Wait a moment for startup
sleep 2

# Check status
echo ""
echo -e "${YELLOW}Service Status:${NC}"
systemctl status "${SERVICE_NAME}" --no-pager || true

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Installation Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}Database location:${NC} ${DB_PATH}"
echo -e "${YELLOW}Log command:${NC} journalctl -u ${SERVICE_NAME} -f"
echo -e "${YELLOW}Stats command:${NC} sqlite3 ${DB_PATH} 'SELECT state, COUNT(*) FROM commands GROUP BY state'"
echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo "1. Test duplicate command protection"
echo "2. Test agent restart safety"
echo "3. Verify database persistence"
echo ""
echo -e "${GREEN}Ready for Step 1 testing!${NC}"
