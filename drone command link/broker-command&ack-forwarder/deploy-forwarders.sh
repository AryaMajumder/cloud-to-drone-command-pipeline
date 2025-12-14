#!/bin/bash
################################################################################
# Drone C2 Forwarder Setup Script
# 
# Description: Automates setup of ACK and Command forwarders from GitHub repo
# 
# Usage:
#   ./setup_forwarders.sh \
#     --aws-access-key "AKIA..." \
#     --aws-secret-key "secret..." \
#     --aws-region "us-east-1" \
#     --iot-endpoint "xxxxx.iot.us-east-1.amazonaws.com" \
#     --drone-id "drone_001" \
#     --sqs-queue-url "https://sqs.us-east-1.amazonaws.com/123/queue"
#
################################################################################

set -e  # Exit on error

# Hardcoded GitHub raw file URLs
GITHUB_BASE_URL="https://raw.githubusercontent.com/AryaMajumder/cloud-to-drone-command-pipeline/main/drone%20command%20link/broker-command%26ack-forwarder"
FORWARDER_LIB_URL="${GITHUB_BASE_URL}/forwarder_lib.py"
ACK_FORWARDER_URL="${GITHUB_BASE_URL}/ack_forwarder.py"
COMMAND_FORWARDER_URL="${GITHUB_BASE_URL}/command_forwarder.py"

# Default values
INSTALL_DIR="/home/ubuntu/forwarders"
AWS_REGION="us-east-1"
CLOUDWATCH_ENABLED="true"
MQTT_BROKER="localhost"
MQTT_PORT="1883"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

################################################################################
# Helper Functions
################################################################################

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

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

Required Options:
  --aws-access-key KEY       AWS Access Key ID
  --aws-secret-key SECRET    AWS Secret Access Key
  --iot-endpoint ENDPOINT    AWS IoT Core endpoint (without https://)
  --drone-id ID              Drone identifier (e.g., drone_001)
  --sqs-queue-url URL        SQS queue URL for commands

Optional Options:
  --aws-region REGION        AWS region (default: us-east-1)
  --install-dir PATH         Installation directory (default: /home/ubuntu/forwarders)
  --cloudwatch-enabled BOOL  Enable CloudWatch metrics (default: true)
  --mqtt-broker HOST         Mosquitto broker host (default: localhost)
  --mqtt-port PORT           Mosquitto broker port (default: 1883)
  --mqtt-user USER           Mosquitto username (optional)
  --mqtt-pass PASS           Mosquitto password (optional)
  --help                     Show this help message

GitHub Files (hardcoded):
  Base URL: $GITHUB_BASE_URL
  - forwarder_lib.py
  - ack_forwarder.py
  - command_forwarder.py

Example:
  $0 \\
    --aws-access-key "AKIAIOSFODNN7EXAMPLE" \\
    --aws-secret-key "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \\
    --iot-endpoint "a1b2c3d4e5f6g7.iot.us-east-1.amazonaws.com" \\
    --drone-id "drone_001" \\
    --sqs-queue-url "https://sqs.us-east-1.amazonaws.com/123456789012/drone-commands"

EOF
    exit 1
}

################################################################################
# Parse Command Line Arguments
################################################################################

while [[ $# -gt 0 ]]; do
    case $1 in
        --aws-access-key)
            AWS_ACCESS_KEY="$2"
            shift 2
            ;;
        --aws-secret-key)
            AWS_SECRET_KEY="$2"
            shift 2
            ;;
        --aws-region)
            AWS_REGION="$2"
            shift 2
            ;;
        --iot-endpoint)
            IOT_ENDPOINT="$2"
            shift 2
            ;;
        --drone-id)
            DRONE_ID="$2"
            shift 2
            ;;
        --sqs-queue-url)
            SQS_QUEUE_URL="$2"
            shift 2
            ;;
        --install-dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --cloudwatch-enabled)
            CLOUDWATCH_ENABLED="$2"
            shift 2
            ;;
        --mqtt-broker)
            MQTT_BROKER="$2"
            shift 2
            ;;
        --mqtt-port)
            MQTT_PORT="$2"
            shift 2
            ;;
        --mqtt-user)
            MQTT_USER="$2"
            shift 2
            ;;
        --mqtt-pass)
            MQTT_PASS="$2"
            shift 2
            ;;
        --help)
            show_usage
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            ;;
    esac
done

################################################################################
# Validate Required Parameters
################################################################################

print_header "Validating Parameters"

if [[ -z "$AWS_ACCESS_KEY" ]]; then
    print_error "Missing required parameter: --aws-access-key"
    show_usage
fi

if [[ -z "$AWS_SECRET_KEY" ]]; then
    print_error "Missing required parameter: --aws-secret-key"
    show_usage
fi

if [[ -z "$IOT_ENDPOINT" ]]; then
    print_error "Missing required parameter: --iot-endpoint"
    show_usage
fi

if [[ -z "$DRONE_ID" ]]; then
    print_error "Missing required parameter: --drone-id"
    show_usage
fi

if [[ -z "$SQS_QUEUE_URL" ]]; then
    print_error "Missing required parameter: --sqs-queue-url"
    show_usage
fi

print_success "All required parameters validated"

################################################################################
# Display Configuration
################################################################################

print_header "Configuration Summary"

cat << EOF
GitHub Base URL:      $GITHUB_BASE_URL
Installation Dir:     $INSTALL_DIR
AWS Region:           $AWS_REGION
IoT Endpoint:         $IOT_ENDPOINT
Drone ID:             $DRONE_ID
SQS Queue URL:        $SQS_QUEUE_URL
MQTT Broker:          $MQTT_BROKER:$MQTT_PORT
CloudWatch:           $CLOUDWATCH_ENABLED
EOF

echo ""
read -p "Continue with installation? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_warning "Installation cancelled by user"
    exit 0
fi

################################################################################
# Check Prerequisites
################################################################################

print_header "Checking Prerequisites"

# Check for required commands
for cmd in python3 curl pip3; do
    if ! command -v $cmd &> /dev/null; then
        print_error "$cmd is not installed"
        exit 1
    fi
    print_success "$cmd found"
done

# Check Python version
PYTHON_VERSION=$(python3 --version | awk '{print $2}')
print_info "Python version: $PYTHON_VERSION"

# Check if Mosquitto is running
if systemctl is-active --quiet mosquitto; then
    print_success "Mosquitto is running"
else
    print_warning "Mosquitto is not running. Start it with: sudo systemctl start mosquitto"
fi

################################################################################
# Create Directory Structure
################################################################################

print_header "Creating Directory Structure"

mkdir -p "$INSTALL_DIR"/{ack,command}
print_success "Created directories"

################################################################################
# Download Files from GitHub
################################################################################

print_header "Downloading Files from GitHub"

# Download forwarder_lib.py
print_info "Downloading forwarder_lib.py..."
if curl -fsSL "$FORWARDER_LIB_URL" -o "$INSTALL_DIR/forwarder_lib.py"; then
    print_success "Downloaded forwarder_lib.py"
else
    print_error "Failed to download forwarder_lib.py"
    print_error "URL: $FORWARDER_LIB_URL"
    exit 1
fi

# Download ack_forwarder.py
print_info "Downloading ack_forwarder.py..."
if curl -fsSL "$ACK_FORWARDER_URL" -o "$INSTALL_DIR/ack_forwarder_template.py"; then
    print_success "Downloaded ack_forwarder.py"
else
    print_error "Failed to download ack_forwarder.py"
    print_error "URL: $ACK_FORWARDER_URL"
    exit 1
fi

# Download command_forwarder.py
print_info "Downloading command_forwarder.py..."
if curl -fsSL "$COMMAND_FORWARDER_URL" -o "$INSTALL_DIR/command_forwarder_template.py"; then
    print_success "Downloaded command_forwarder.py"
else
    print_error "Failed to download command_forwarder.py"
    print_error "URL: $COMMAND_FORWARDER_URL"
    exit 1
fi

# Copy forwarder_lib to both directories
cp "$INSTALL_DIR/forwarder_lib.py" "$INSTALL_DIR/ack/"
cp "$INSTALL_DIR/forwarder_lib.py" "$INSTALL_DIR/command/"
print_success "Copied forwarder_lib.py to both directories"

################################################################################
# Update Configuration in Python Files
################################################################################

print_header "Updating Configuration"

# Function to update config in Python file
update_config() {
    local template_file=$1
    local output_file=$2
    local forwarder_type=$3
    
    print_info "Creating configured $output_file"
    
    # Build MQTT auth config
    local mqtt_auth=""
    if [[ -n "$MQTT_USER" ]]; then
        mqtt_auth="        'mqtt_user': '$MQTT_USER',
        'mqtt_pass': '$MQTT_PASS',"
    fi
    
    # Build SQS config for command forwarder
    local sqs_config=""
    if [[ "$forwarder_type" == "Command" ]]; then
        sqs_config="        'sqs_queue_url': '$SQS_QUEUE_URL',"
    fi
    
    # Read the template and replace config section
    # Extract everything before "if __name__"
    sed -n '1,/if __name__/p' "$template_file" | sed '$ d' > "$output_file"
    
    # Add the new config
    cat >> "$output_file" << EOF

if __name__ == "__main__":
    config = ForwarderConfig({
        'aws_access_key_id': '$AWS_ACCESS_KEY',
        'aws_secret_access_key': '$AWS_SECRET_KEY',
        'aws_region': '$AWS_REGION',
        'iot_endpoint': '$IOT_ENDPOINT',
        'mqtt_broker': '$MQTT_BROKER',
        'mqtt_port': $MQTT_PORT,
$mqtt_auth
        'cloudwatch_namespace': 'DroneC2/$forwarder_type',
        'cloudwatch_enabled': $CLOUDWATCH_ENABLED,
        'drone_id': '$DRONE_ID',
$sqs_config
    })
    
    forwarder = ${forwarder_type}Forwarder(config)
    exit(forwarder.run())
EOF

    print_success "Created configured $output_file"
}

# Update ACK forwarder config
update_config "$INSTALL_DIR/ack_forwarder_template.py" "$INSTALL_DIR/ack/ack_forwarder.py" "Ack"

# Update Command forwarder config
update_config "$INSTALL_DIR/command_forwarder_template.py" "$INSTALL_DIR/command/command_forwarder.py" "Command"

# Clean up templates
rm -f "$INSTALL_DIR/ack_forwarder_template.py" "$INSTALL_DIR/command_forwarder_template.py"

################################################################################
# Create Virtual Environments
################################################################################

print_header "Creating Virtual Environments"

# ACK forwarder venv
print_info "Creating venv for ACK forwarder..."
cd "$INSTALL_DIR/ack"
python3 -m venv venv
source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet boto3 paho-mqtt
deactivate
print_success "ACK forwarder venv created"

# Command forwarder venv
print_info "Creating venv for Command forwarder..."
cd "$INSTALL_DIR/command"
python3 -m venv venv
source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet boto3 paho-mqtt
deactivate
print_success "Command forwarder venv created"

################################################################################
# Test Forwarders
################################################################################

print_header "Testing Forwarders"

# Test ACK forwarder
print_info "Testing ACK forwarder (5 seconds)..."
cd "$INSTALL_DIR/ack"
timeout 5 venv/bin/python3 ack_forwarder.py &> /tmp/ack_test.log || true

if grep -q "Forwarder running\|Connected to Mosquitto" /tmp/ack_test.log; then
    print_success "ACK forwarder test passed"
else
    print_warning "ACK forwarder test inconclusive. Check logs:"
    tail -10 /tmp/ack_test.log
fi

# Test Command forwarder
print_info "Testing Command forwarder (5 seconds)..."
cd "$INSTALL_DIR/command"
timeout 5 venv/bin/python3 command_forwarder.py &> /tmp/command_test.log || true

if grep -q "Forwarder running\|Connected to Mosquitto" /tmp/command_test.log; then
    print_success "Command forwarder test passed"
else
    print_warning "Command forwarder test inconclusive. Check logs:"
    tail -10 /tmp/command_test.log
fi

################################################################################
# Create Systemd Services
################################################################################

print_header "Creating Systemd Services"

# Get current user
CURRENT_USER=$(whoami)

# ACK forwarder service
print_info "Creating ack_forwarder.service"
sudo tee /etc/systemd/system/ack_forwarder.service > /dev/null << EOF
[Unit]
Description=Drone C2 ACK Forwarder
After=network.target mosquitto.service
Wants=mosquitto.service

[Service]
Type=simple
User=$CURRENT_USER
WorkingDirectory=$INSTALL_DIR/ack
ExecStart=$INSTALL_DIR/ack/venv/bin/python3 ack_forwarder.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
print_success "Created ack_forwarder.service"

# Command forwarder service
print_info "Creating command_forwarder.service"
sudo tee /etc/systemd/system/command_forwarder.service > /dev/null << EOF
[Unit]
Description=Drone C2 Command Forwarder
After=network.target mosquitto.service
Wants=mosquitto.service

[Service]
Type=simple
User=$CURRENT_USER
WorkingDirectory=$INSTALL_DIR/command
ExecStart=$INSTALL_DIR/command/venv/bin/python3 command_forwarder.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
print_success "Created command_forwarder.service"

################################################################################
# Enable and Start Services
################################################################################

print_header "Enabling and Starting Services"

sudo systemctl daemon-reload

# Enable services
sudo systemctl enable ack_forwarder
sudo systemctl enable command_forwarder
print_success "Services enabled"

# Start services
sudo systemctl start ack_forwarder
sudo systemctl start command_forwarder
print_success "Services started"

# Wait a moment for services to initialize
sleep 3

################################################################################
# Verify Services
################################################################################

print_header "Verifying Services"

# Check ACK forwarder
if systemctl is-active --quiet ack_forwarder; then
    print_success "ACK forwarder is running"
else
    print_error "ACK forwarder failed to start"
    print_info "Recent logs:"
    sudo journalctl -u ack_forwarder -n 20 --no-pager
fi

# Check Command forwarder
if systemctl is-active --quiet command_forwarder; then
    print_success "Command forwarder is running"
else
    print_error "Command forwarder failed to start"
    print_info "Recent logs:"
    sudo journalctl -u command_forwarder -n 20 --no-pager
fi

################################################################################
# Final Summary
################################################################################

print_header "Installation Complete!"

cat << EOF
${GREEN}✓${NC} Downloaded files from GitHub
${GREEN}✓${NC} Created virtual environments
${GREEN}✓${NC} Configured forwarders
${GREEN}✓${NC} Created systemd services

Installation Directory: $INSTALL_DIR

${BLUE}Service Status:${NC}
EOF

systemctl is-active ack_forwarder && echo -e "  ${GREEN}✓${NC} ack_forwarder: running" || echo -e "  ${RED}✗${NC} ack_forwarder: not running"
systemctl is-active command_forwarder && echo -e "  ${GREEN}✓${NC} command_forwarder: running" || echo -e "  ${RED}✗${NC} command_forwarder: not running"

cat << EOF

${BLUE}Useful Commands:${NC}

Check service status:
  sudo systemctl status ack_forwarder
  sudo systemctl status command_forwarder

View logs:
  sudo journalctl -u ack_forwarder -f
  sudo journalctl -u command_forwarder -f

Restart services:
  sudo systemctl restart ack_forwarder
  sudo systemctl restart command_forwarder

Stop services:
  sudo systemctl stop ack_forwarder
  sudo systemctl stop command_forwarder

${BLUE}Next Steps:${NC}

1. Verify agent is running: sudo systemctl status px4_agent
2. Test command flow: Send test command from AWS IoT Core
3. Monitor all logs: sudo journalctl -u ack_forwarder -u command_forwarder -u px4_agent -f

EOF

print_success "Setup script completed successfully!"

exit 0