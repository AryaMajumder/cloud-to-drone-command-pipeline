#!/bin/bash
################################################################################
# Comprehensive Forwarder Permission Fix with Diagnostics
################################################################################

set -e

INSTALL_DIR="/home/ec2-user/forwarders"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Forwarder Permission Diagnostic & Fix${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

# Stop services
echo -e "${BLUE}[1/6] Stopping services...${NC}"
systemctl stop ack_forwarder 2>/dev/null || true
systemctl stop command_forwarder 2>/dev/null || true
echo -e "${GREEN}✓ Services stopped${NC}"
echo ""

# Check current permissions
echo -e "${BLUE}[2/6] Current permissions:${NC}"
ls -la /home/ec2-user | grep -E "^d" || true
ls -ld /home/ec2-user
ls -ld /home/ec2-user/forwarders || true
ls -ld /home/ec2-user/forwarders/ack || true
ls -ld /home/ec2-user/forwarders/command || true
echo ""

# Check SELinux
echo -e "${BLUE}[3/6] Checking SELinux...${NC}"
if command -v getenforce &> /dev/null; then
    SELINUX_STATUS=$(getenforce)
    echo "SELinux status: $SELINUX_STATUS"
    if [ "$SELINUX_STATUS" = "Enforcing" ]; then
        echo -e "${YELLOW}⚠ SELinux is enforcing - this might be blocking access${NC}"
        echo "Disabling SELinux temporarily..."
        setenforce 0 || echo "Failed to disable SELinux"
    fi
else
    echo "SELinux not found"
fi
echo ""

# Fix permissions comprehensively
echo -e "${BLUE}[4/6] Fixing permissions...${NC}"

# Fix home directory - MUST be at least 711 (rwx--x--x)
echo "Setting /home permissions..."
chmod 755 /home 2>/dev/null || true

echo "Setting /home/ec2-user permissions..."
chmod 755 /home/ec2-user

# Fix forwarders directory tree
echo "Setting forwarders directory permissions..."
chown -R ec2-user:ec2-user "$INSTALL_DIR"

# Set all directories to 755 (rwxr-xr-x)
find "$INSTALL_DIR" -type d -exec chmod 755 {} \;

# Set all files to 644 (rw-r--r--)
find "$INSTALL_DIR" -type f -exec chmod 644 {} \;

# Make Python scripts executable
find "$INSTALL_DIR" -type f -name "*.py" -exec chmod 755 {} \;

# Make venv binaries executable
find "$INSTALL_DIR" -path "*/venv/bin/*" -type f -exec chmod 755 {} \;

# Special: Make sure venv activate scripts are executable
find "$INSTALL_DIR" -path "*/venv/bin/activate*" -type f -exec chmod 755 {} \;

echo -e "${GREEN}✓ Permissions fixed${NC}"
echo ""

# Verify permissions
echo -e "${BLUE}[5/6] Verifying permissions:${NC}"
echo "Home directory:"
ls -ld /home/ec2-user
echo ""
echo "Forwarders directory:"
ls -ld "$INSTALL_DIR"
echo ""
echo "Subdirectories:"
ls -ld "$INSTALL_DIR"/{ack,command}
echo ""
echo "Python executables:"
ls -l "$INSTALL_DIR"/ack/venv/bin/python3
ls -l "$INSTALL_DIR"/command/venv/bin/python3
echo ""

# Test directory access as ec2-user
echo "Testing directory access as ec2-user..."
if su - ec2-user -c "cd $INSTALL_DIR/ack && pwd" &> /dev/null; then
    echo -e "${GREEN}✓ ec2-user can access ACK directory${NC}"
else
    echo -e "${RED}✗ ec2-user CANNOT access ACK directory${NC}"
fi

if su - ec2-user -c "cd $INSTALL_DIR/command && pwd" &> /dev/null; then
    echo -e "${GREEN}✓ ec2-user can access Command directory${NC}"
else
    echo -e "${RED}✗ ec2-user CANNOT access Command directory${NC}"
fi
echo ""

# Start services
echo -e "${BLUE}[6/6] Starting services...${NC}"
systemctl daemon-reload
systemctl start ack_forwarder
systemctl start command_forwarder

sleep 3
echo ""

# Check status
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}Service Status${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""

if systemctl is-active --quiet ack_forwarder; then
    echo -e "${GREEN}✓ ack_forwarder: RUNNING${NC}"
else
    echo -e "${RED}✗ ack_forwarder: FAILED${NC}"
    echo ""
    echo "Recent logs:"
    journalctl -u ack_forwarder -n 10 --no-pager
fi

echo ""

if systemctl is-active --quiet command_forwarder; then
    echo -e "${GREEN}✓ command_forwarder: RUNNING${NC}"
else
    echo -e "${RED}✗ command_forwarder: FAILED${NC}"
    echo ""
    echo "Recent logs:"
    journalctl -u command_forwarder -n 10 --no-pager
fi

echo ""
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}If services are still failing:${NC}"
echo -e "${BLUE}=========================================${NC}"
echo ""
echo "1. Check detailed logs:"
echo "   sudo journalctl -u ack_forwarder -n 50"
echo "   sudo journalctl -u command_forwarder -n 50"
echo ""
echo "2. Try running manually:"
echo "   sudo su - ec2-user"
echo "   cd $INSTALL_DIR/ack"
echo "   source venv/bin/activate"
echo "   python3 ack_forwarder.py"
echo ""
echo "3. Check if there are any AppArmor/SELinux restrictions"
echo ""