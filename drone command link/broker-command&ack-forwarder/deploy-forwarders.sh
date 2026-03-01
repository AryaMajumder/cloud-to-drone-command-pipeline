#!/bin/bash
################################################################################
# Drone C2 Forwarder Deployment Script (Complete Fix)
# Fixes: Python booleans + creates logs directories
################################################################################

set -e

# GitHub URLs
GITHUB_BASE="https://raw.githubusercontent.com/AryaMajumder/cloud-to-drone-command-pipeline/main/drone%20command%20link/broker-command%26ack-forwarder"
COMMAND_FORWARDER_URL="${GITHUB_BASE}/command_forwarder.py"
ACK_FORWARDER_URL="${GITHUB_BASE}/ack_forwarder.py"

# Default values
INSTALL_DIR="/home/ubuntu/forwarders"
AWS_REGION="us-east-1"
CLOUDWATCH_ENABLED="true"
MQTT_BROKER="localhost"
MQTT_PORT="1883"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_header() {
    echo ""
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""
}

show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Required:
  --aws-access-key KEY       AWS Access Key ID
  --aws-secret-key SECRET    AWS Secret Access Key
  --iot-endpoint ENDPOINT    IoT endpoint (xxxxx-ats.iot.us-east-1.amazonaws.com)
  --drone-id ID              Drone identifier (e.g., drone-01)

Optional:
  --aws-region REGION        AWS region (default: us-east-1)
  --install-dir PATH         Install directory (default: /home/ubuntu/forwarders)
  --mqtt-broker HOST         Mosquitto host (default: localhost)
  --mqtt-port PORT           Mosquitto port (default: 1883)
  --mqtt-user USER           Mosquitto username (optional)
  --mqtt-pass PASS           Mosquitto password (optional)
  --cloudwatch BOOL          Enable CloudWatch (default: true)

Example:
  $0 \\
    --aws-access-key "AKIA..." \\
    --aws-secret-key "secret..." \\
    --iot-endpoint "ad7fal60no91u-ats.iot.us-east-1.amazonaws.com" \\
    --drone-id "drone-01"

EOF
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --aws-access-key) AWS_ACCESS_KEY="$2"; shift 2 ;;
        --aws-secret-key) AWS_SECRET_KEY="$2"; shift 2 ;;
        --iot-endpoint) IOT_ENDPOINT="$2"; shift 2 ;;
        --drone-id) DRONE_ID="$2"; shift 2 ;;
        --aws-region) AWS_REGION="$2"; shift 2 ;;
        --install-dir) INSTALL_DIR="$2"; shift 2 ;;
        --mqtt-broker) MQTT_BROKER="$2"; shift 2 ;;
        --mqtt-port) MQTT_PORT="$2"; shift 2 ;;
        --mqtt-user) MQTT_USER="$2"; shift 2 ;;
        --mqtt-pass) MQTT_PASS="$2"; shift 2 ;;
        --cloudwatch) CLOUDWATCH_ENABLED="$2"; shift 2 ;;
        --help) show_usage ;;
        *) print_error "Unknown option: $1"; show_usage ;;
    esac
done

# Validate required parameters
print_header "Validating Parameters"
[[ -z "$AWS_ACCESS_KEY" ]] && { print_error "Missing --aws-access-key"; show_usage; }
[[ -z "$AWS_SECRET_KEY" ]] && { print_error "Missing --aws-secret-key"; show_usage; }
[[ -z "$IOT_ENDPOINT" ]] && { print_error "Missing --iot-endpoint"; show_usage; }
[[ -z "$DRONE_ID" ]] && { print_error "Missing --drone-id"; show_usage; }
print_success "All required parameters provided"

# Convert bash boolean to Python boolean
if [[ "$CLOUDWATCH_ENABLED" == "true" || "$CLOUDWATCH_ENABLED" == "True" || "$CLOUDWATCH_ENABLED" == "TRUE" ]]; then
    PYTHON_CLOUDWATCH="True"
else
    PYTHON_CLOUDWATCH="False"
fi

# Display configuration
print_header "Configuration Summary"
cat << EOF
Installation Dir:     $INSTALL_DIR
AWS Region:           $AWS_REGION
IoT Endpoint:         $IOT_ENDPOINT
Drone ID:             $DRONE_ID
MQTT Broker:          $MQTT_BROKER:$MQTT_PORT
MQTT Auth:            $([ -n "$MQTT_USER" ] && echo "Enabled" || echo "Disabled")
CloudWatch:           $PYTHON_CLOUDWATCH
EOF

echo ""
read -p "Continue? (y/n) " -n 1 -r
echo ""
[[ ! $REPLY =~ ^[Yy]$ ]] && { print_warning "Cancelled"; exit 0; }

# Check prerequisites
print_header "Checking Prerequisites"
for cmd in python3 curl pip3; do
    if ! command -v $cmd &>/dev/null; then
        print_error "$cmd not found"
        exit 1
    fi
    print_success "$cmd found"
done

PYTHON_VERSION=$(python3 --version | awk '{print $2}')
print_info "Python version: $PYTHON_VERSION"

if systemctl is-active --quiet mosquitto; then
    print_success "Mosquitto is running"
else
    print_warning "Mosquitto not running (start with: sudo systemctl start mosquitto)"
fi

# Create directory structure
print_header "Creating Directories"
mkdir -p "$INSTALL_DIR"/{command,ack}
mkdir -p "$INSTALL_DIR/command/logs"
mkdir -p "$INSTALL_DIR/ack/logs"
print_success "Created directories with logs folders"

# Download files from GitHub
print_header "Downloading Files from GitHub"

print_info "Downloading command_forwarder.py..."
if curl -fsSL "$COMMAND_FORWARDER_URL" -o "$INSTALL_DIR/command/command_forwarder.py"; then
    SIZE=$(stat -c%s "$INSTALL_DIR/command/command_forwarder.py" 2>/dev/null || stat -f%z "$INSTALL_DIR/command/command_forwarder.py" 2>/dev/null)
    print_success "Downloaded command_forwarder.py ($SIZE bytes)"
else
    print_error "Failed to download command_forwarder.py"
    exit 1
fi

print_info "Downloading ack_forwarder.py..."
if curl -fsSL "$ACK_FORWARDER_URL" -o "$INSTALL_DIR/ack/ack_forwarder.py"; then
    SIZE=$(stat -c%s "$INSTALL_DIR/ack/ack_forwarder.py" 2>/dev/null || stat -f%z "$INSTALL_DIR/ack/ack_forwarder.py" 2>/dev/null)
    print_success "Downloaded ack_forwarder.py ($SIZE bytes)"
else
    print_error "Failed to download ack_forwarder.py"
    exit 1
fi

# Create configuration files
print_header "Creating Configuration Files"

# Command forwarder config
print_info "Creating command forwarder config..."
cat > "$INSTALL_DIR/command/config.py" << EOF
# Command Forwarder Configuration
config = {
    'aws_access_key_id': '$AWS_ACCESS_KEY',
    'aws_secret_access_key': '$AWS_SECRET_KEY',
    'aws_region': '$AWS_REGION',
    'iot_endpoint': '$IOT_ENDPOINT',
    'mqtt_broker': '$MQTT_BROKER',
    'mqtt_port': $MQTT_PORT,
EOF

if [[ -n "$MQTT_USER" ]]; then
    cat >> "$INSTALL_DIR/command/config.py" << EOF
    'mqtt_user': '$MQTT_USER',
    'mqtt_pass': '$MQTT_PASS',
EOF
else
    cat >> "$INSTALL_DIR/command/config.py" << EOF
    'mqtt_user': None,
    'mqtt_pass': None,
EOF
fi

cat >> "$INSTALL_DIR/command/config.py" << EOF
    'cloudwatch_enabled': $PYTHON_CLOUDWATCH,
    'cloudwatch_namespace': 'DroneC2/Command'
}
EOF
print_success "Created command config"

# ACK forwarder config
print_info "Creating ACK forwarder config..."
cat > "$INSTALL_DIR/ack/config.py" << EOF
# ACK Forwarder Configuration
config = {
    'aws_access_key_id': '$AWS_ACCESS_KEY',
    'aws_secret_access_key': '$AWS_SECRET_KEY',
    'aws_region': '$AWS_REGION',
    'iot_endpoint': '$IOT_ENDPOINT',
    'mqtt_broker': '$MQTT_BROKER',
    'mqtt_port': $MQTT_PORT,
EOF

if [[ -n "$MQTT_USER" ]]; then
    cat >> "$INSTALL_DIR/ack/config.py" << EOF
    'mqtt_user': '$MQTT_USER',
    'mqtt_pass': '$MQTT_PASS',
EOF
else
    cat >> "$INSTALL_DIR/ack/config.py" << EOF
    'mqtt_user': None,
    'mqtt_pass': None,
EOF
fi

cat >> "$INSTALL_DIR/ack/config.py" << EOF
    'cloudwatch_enabled': $PYTHON_CLOUDWATCH,
    'cloudwatch_namespace': 'DroneC2/ACK',
    'drone_id': '$DRONE_ID'
}
EOF
print_success "Created ACK config"

# Create wrapper scripts
print_header "Creating Wrapper Scripts"

# Command forwarder wrapper
cat > "$INSTALL_DIR/command/run.py" << 'EOF'
#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import config
from command_forwarder import CommandForwarder

if __name__ == "__main__":
    forwarder = CommandForwarder(config)
    exit(forwarder.run())
EOF
chmod +x "$INSTALL_DIR/command/run.py"
print_success "Created command wrapper"

# ACK forwarder wrapper
cat > "$INSTALL_DIR/ack/run.py" << 'EOF'
#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import config
from ack_forwarder import AckForwarder

if __name__ == "__main__":
    forwarder = AckForwarder(config)
    exit(forwarder.run())
EOF
chmod +x "$INSTALL_DIR/ack/run.py"
print_success "Created ACK wrapper"

# Create virtual environments
print_header "Creating Virtual Environments"

print_info "Creating command forwarder venv..."
cd "$INSTALL_DIR/command"
python3 -m venv venv
source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet boto3 paho-mqtt awsiotsdk
deactivate
print_success "Command venv ready (boto3, paho-mqtt, awsiotsdk)"

print_info "Creating ACK forwarder venv..."
cd "$INSTALL_DIR/ack"
python3 -m venv venv
source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet boto3 paho-mqtt awsiotsdk
deactivate
print_success "ACK venv ready (boto3, paho-mqtt, awsiotsdk)"

# Test forwarders
print_header "Testing Forwarders"

print_info "Testing command forwarder (3 seconds)..."
cd "$INSTALL_DIR/command"
timeout 3 venv/bin/python3 run.py &> /tmp/cmd_test.log || true
if grep -qi "connect\|forwarder\|starting" /tmp/cmd_test.log; then
    print_success "Command forwarder initialized"
else
    print_warning "Command test inconclusive. Last 3 lines:"
    tail -3 /tmp/cmd_test.log
fi

print_info "Testing ACK forwarder (3 seconds)..."
cd "$INSTALL_DIR/ack"
timeout 3 venv/bin/python3 run.py &> /tmp/ack_test.log || true
if grep -qi "connect\|forwarder\|starting" /tmp/ack_test.log; then
    print_success "ACK forwarder initialized"
else
    print_warning "ACK test inconclusive. Last 3 lines:"
    tail -3 /tmp/ack_test.log
fi

# Create systemd services
print_header "Creating Systemd Services"

CURRENT_USER=$(whoami)

# Command forwarder service
print_info "Creating command_forwarder.service..."
sudo tee /etc/systemd/system/command_forwarder.service > /dev/null << EOF
[Unit]
Description=Drone C2 Command Forwarder (IoT Core)
After=network.target mosquitto.service
Wants=mosquitto.service

[Service]
Type=simple
User=$CURRENT_USER
WorkingDirectory=$INSTALL_DIR/command
ExecStart=$INSTALL_DIR/command/venv/bin/python3 $INSTALL_DIR/command/run.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
print_success "Created command_forwarder.service"

# ACK forwarder service
print_info "Creating ack_forwarder.service..."
sudo tee /etc/systemd/system/ack_forwarder.service > /dev/null << EOF
[Unit]
Description=Drone C2 ACK Forwarder
After=network.target mosquitto.service
Wants=mosquitto.service

[Service]
Type=simple
User=$CURRENT_USER
WorkingDirectory=$INSTALL_DIR/ack
ExecStart=$INSTALL_DIR/ack/venv/bin/python3 $INSTALL_DIR/ack/run.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
print_success "Created ack_forwarder.service"

# Enable and start services
print_header "Enabling and Starting Services"

sudo systemctl daemon-reload
sudo systemctl enable command_forwarder ack_forwarder
print_success "Services enabled"

sudo systemctl start command_forwarder ack_forwarder
print_success "Services started"

sleep 3

# Verify services
print_header "Verifying Services"

if systemctl is-active --quiet command_forwarder; then
    print_success "command_forwarder is running"
else
    print_error "command_forwarder failed to start"
    print_info "Recent logs:"
    sudo journalctl -u command_forwarder -n 15 --no-pager
fi

if systemctl is-active --quiet ack_forwarder; then
    print_success "ack_forwarder is running"
else
    print_error "ack_forwarder failed to start"
    print_info "Recent logs:"
    sudo journalctl -u ack_forwarder -n 15 --no-pager
fi

# Final summary
print_header "Installation Complete!"

cat << EOF
${GREEN}✓${NC} Downloaded forwarders from GitHub
${GREEN}✓${NC} Created configurations (with Python booleans)
${GREEN}✓${NC} Created logs directories
${GREEN}✓${NC} Created virtual environments
${GREEN}✓${NC} Created systemd services

${BLUE}Installation:${NC}
  Directory:  $INSTALL_DIR
  Command:    $INSTALL_DIR/command/
  ACK:        $INSTALL_DIR/ack/

${BLUE}Service Status:${NC}
EOF

systemctl is-active command_forwarder &>/dev/null && \
    echo -e "  ${GREEN}✓${NC} command_forwarder: running" || \
    echo -e "  ${RED}✗${NC} command_forwarder: not running"

systemctl is-active ack_forwarder &>/dev/null && \
    echo -e "  ${GREEN}✓${NC} ack_forwarder: running" || \
    echo -e "  ${RED}✗${NC} ack_forwarder: not running"

cat << EOF

${BLUE}Log Files:${NC}
  $INSTALL_DIR/command/logs/command_forwarder.log
  $INSTALL_DIR/ack/logs/ack_forwarder.log

${BLUE}Systemd Logs:${NC}
  sudo journalctl -u command_forwarder -u ack_forwarder -f

${BLUE}Test Command:${NC}
  aws lambda invoke --function-name minimal-command-system \\
    --cli-binary-format raw-in-base64-out \\
    --payload '{"payload":{"target_id":"${DRONE_ID}","action":"rtl"}}' \\
    response.json

EOF

print_success "Deployment complete!"
exit 0
