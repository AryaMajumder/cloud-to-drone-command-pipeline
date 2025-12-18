#!/bin/bash
################################################################################
# Forwarder Deployment Script - Downloads from GitHub
# 
# Downloads ACK forwarder from GitHub (keeps the logic)
# Modifies forwarder_lib.py to support SigV4 (access keys)
# Creates new command forwarder with IoT Core subscription
#
# Usage:
#   sudo bash deploy_forwarders_github_sigv4.sh \
#     --aws-access-key "AKIA..." \
#     --aws-secret-key "secret..." \
#     --iot-endpoint "xxxxx.iot.us-east-1.amazonaws.com" \
#     --drone-id "drone001"
#
################################################################################

set -e

# GitHub URLs
GITHUB_BASE="https://raw.githubusercontent.com/AryaMajumder/cloud-to-drone-command-pipeline/main/drone%20command%20link/broker-command%26ack-forwarder"
ACK_FORWARDER_URL="${GITHUB_BASE}/ack_forwarder.py"
FORWARDER_LIB_URL="${GITHUB_BASE}/forwarder_lib.py"

# Default values
INSTALL_DIR="/home/ec2-user/forwarders"
AWS_REGION="us-east-1"
CLOUDWATCH_ENABLED="true"
MQTT_BROKER="localhost"
MQTT_PORT="1883"
MQTT_USER=""
MQTT_PASS=""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

This script:
  1. Downloads ack_forwarder.py from GitHub (keeps GitHub logic)
  2. Downloads forwarder_lib.py from GitHub
  3. Modifies forwarder_lib.py to support SigV4 (access keys)
  4. Creates new command_forwarder.py (IoT Core subscription, not SQS!)

Required Options:
  --aws-access-key KEY       AWS Access Key ID
  --aws-secret-key SECRET    AWS Secret Access Key
  --iot-endpoint ENDPOINT    AWS IoT Core endpoint
  --drone-id ID              Drone identifier

Optional Options:
  --aws-region REGION        AWS region (default: us-east-1)
  --install-dir PATH         Installation directory (default: /home/ec2-user/forwarders)
  --cloudwatch-enabled BOOL  Enable CloudWatch metrics (default: true)
  --mqtt-broker HOST         Mosquitto broker host (default: localhost)
  --mqtt-port PORT           Mosquitto broker port (default: 1883)
  --mqtt-user USER           Mosquitto username (optional)
  --mqtt-pass PASS           Mosquitto password (optional)
  --help                     Show this help message

GitHub Repository:
  https://github.com/AryaMajumder/cloud-to-drone-command-pipeline

Example:
  $0 \\
    --aws-access-key "AKIAIOSFODNN7EXAMPLE" \\
    --aws-secret-key "wJalrXUtnFEMI/K7MDENG/..." \\
    --iot-endpoint "xxxxx-ats.iot.us-east-1.amazonaws.com" \\
    --drone-id "drone001"
EOF
    exit 1
}

################################################################################
# Parse Arguments
################################################################################

while [[ $# -gt 0 ]]; do
    case $1 in
        --aws-access-key) AWS_ACCESS_KEY="$2"; shift 2 ;;
        --aws-secret-key) AWS_SECRET_KEY="$2"; shift 2 ;;
        --aws-region) AWS_REGION="$2"; shift 2 ;;
        --iot-endpoint) IOT_ENDPOINT="$2"; shift 2 ;;
        --drone-id) DRONE_ID="$2"; shift 2 ;;
        --install-dir) INSTALL_DIR="$2"; shift 2 ;;
        --cloudwatch-enabled) CLOUDWATCH_ENABLED="$2"; shift 2 ;;
        --mqtt-broker) MQTT_BROKER="$2"; shift 2 ;;
        --mqtt-port) MQTT_PORT="$2"; shift 2 ;;
        --mqtt-user) MQTT_USER="$2"; shift 2 ;;
        --mqtt-pass) MQTT_PASS="$2"; shift 2 ;;
        --help) show_usage ;;
        *) print_error "Unknown option: $1"; show_usage ;;
    esac
done

# Validate required parameters
if [[ -z "$AWS_ACCESS_KEY" ]] || [[ -z "$AWS_SECRET_KEY" ]] || [[ -z "$IOT_ENDPOINT" ]] || [[ -z "$DRONE_ID" ]]; then
    print_error "Missing required parameters"
    show_usage
fi

################################################################################
# Display Configuration
################################################################################

print_header "Forwarder Deployment Configuration"

cat << EOF
${BLUE}AWS Configuration:${NC}
  Access Key: ${AWS_ACCESS_KEY:0:10}...
  Region: $AWS_REGION
  IoT Endpoint: $IOT_ENDPOINT

${BLUE}Mosquitto Configuration:${NC}
  Broker: $MQTT_BROKER
  Port: $MQTT_PORT

${BLUE}Drone Configuration:${NC}
  Drone ID: $DRONE_ID

${BLUE}Source:${NC}
  GitHub Repo: cloud-to-drone-command-pipeline
  ACK Forwarder: Downloaded from GitHub
  Command Forwarder: Created new (IoT subscription)
  Forwarder Lib: Downloaded and modified for SigV4

${BLUE}Installation:${NC}
  Directory: $INSTALL_DIR
EOF

read -p "Continue with deployment? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_info "Deployment cancelled"
    exit 0
fi

################################################################################
# Check Prerequisites
################################################################################

print_header "Checking Prerequisites"

if [[ $EUID -ne 0 ]]; then
   print_error "This script must be run with sudo"
   exit 1
fi

ACTUAL_USER="${SUDO_USER:-$USER}"
print_info "Running as: $ACTUAL_USER"

for cmd in python3 pip3 systemctl curl; do
    if ! command -v $cmd &> /dev/null; then
        print_error "$cmd is not installed"
        exit 1
    fi
    print_success "$cmd found"
done

PYTHON_VERSION=$(python3 --version | awk '{print $2}')
print_info "Python version: $PYTHON_VERSION"

if systemctl is-active --quiet mosquitto; then
    print_success "Mosquitto is running"
else
    print_warning "Mosquitto is not running"
fi

################################################################################
# Create Directory Structure
################################################################################

print_header "Creating Directory Structure"

mkdir -p "$INSTALL_DIR"/{ack,command,logs}
print_success "Created directories"

################################################################################
# Download Files from GitHub
################################################################################

print_header "Downloading Files from GitHub"

# Download ACK forwarder
print_info "Downloading ack_forwarder.py from GitHub..."
if curl -fsSL "$ACK_FORWARDER_URL" -o "$INSTALL_DIR/ack_forwarder_github.py"; then
    print_success "Downloaded ack_forwarder.py"
else
    print_error "Failed to download ack_forwarder.py"
    print_error "URL: $ACK_FORWARDER_URL"
    exit 1
fi

# Download forwarder_lib
print_info "Downloading forwarder_lib.py from GitHub..."
if curl -fsSL "$FORWARDER_LIB_URL" -o "$INSTALL_DIR/forwarder_lib_github.py"; then
    print_success "Downloaded forwarder_lib.py"
else
    print_error "Failed to download forwarder_lib.py"
    print_error "URL: $FORWARDER_LIB_URL"
    exit 1
fi

################################################################################
# Modify forwarder_lib for SigV4 Support
################################################################################

print_header "Modifying forwarder_lib for SigV4 Support"

print_info "The GitHub forwarder_lib uses X.509 certificates."
print_info "Creating SigV4-compatible version..."

# Create modified forwarder_lib with SigV4 support
cat > "$INSTALL_DIR/forwarder_lib.py" << 'FORWARDER_LIB_EOF'
#!/usr/bin/env python3
"""
Forwarder Library - Modified for SigV4 Support
Based on GitHub version but adapted for access keys
"""

import json
import logging
import time
from datetime import datetime
import paho.mqtt.client as mqtt
import boto3

try:
    from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False

class ForwarderConfig:
    def __init__(self, config_dict):
        # AWS Credentials (for SigV4)
        self.aws_access_key_id = config_dict.get('aws_access_key_id')
        self.aws_secret_access_key = config_dict.get('aws_secret_access_key')
        self.aws_region = config_dict.get('aws_region', 'us-east-1')
        
        # AWS IoT Core
        self.iot_endpoint = config_dict['iot_endpoint']
        
        # Local Mosquitto
        self.mqtt_broker = config_dict.get('mqtt_broker', 'localhost')
        self.mqtt_port = config_dict.get('mqtt_port', 1883)
        self.mqtt_user = config_dict.get('mqtt_user')
        self.mqtt_pass = config_dict.get('mqtt_pass')
        
        # CloudWatch
        self.cloudwatch_namespace = config_dict.get('cloudwatch_namespace')
        self.cloudwatch_enabled = config_dict.get('cloudwatch_enabled', True)
        
        # Drone ID
        self.drone_id = config_dict.get('drone_id', 'drone_001')

class ForwarderBase:
    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.mqtt_client = None
        self.iot_client = None
        self.mqtt_connected = False
        self.iot_connected = False
        self.logger = logging.getLogger(name)
        
        # Setup boto3 session
        if config.aws_access_key_id and config.aws_secret_access_key:
            self.session = boto3.Session(
                aws_access_key_id=config.aws_access_key_id,
                aws_secret_access_key=config.aws_secret_access_key,
                region_name=config.aws_region
            )
        else:
            self.session = boto3.Session(region_name=config.aws_region)
        
        # Setup CloudWatch
        if config.cloudwatch_enabled and config.cloudwatch_namespace:
            try:
                self.cloudwatch = self.session.client('cloudwatch')
            except:
                self.cloudwatch = None
        else:
            self.cloudwatch = None
    
    def publish_metric(self, metric_name, value, unit='Count'):
        if not self.cloudwatch:
            return
        try:
            self.cloudwatch.put_metric_data(
                Namespace=self.config.cloudwatch_namespace,
                MetricData=[{
                    'MetricName': metric_name,
                    'Value': value,
                    'Unit': unit,
                    'Timestamp': datetime.utcnow()
                }]
            )
        except Exception as e:
            self.logger.error(f"CloudWatch publish failed: {e}")
    
    def connect_mosquitto(self, on_connect_callback, on_message_callback):
        client = mqtt.Client(client_id=f"{self.name}_forwarder_{int(time.time())}")
        
        def wrapped_on_connect(client, userdata, flags, rc):
            self.mqtt_connected = (rc == 0)
            on_connect_callback(client, userdata, flags, rc)
        
        def wrapped_on_disconnect(client, userdata, rc):
            self.mqtt_connected = False
            if rc != 0:
                self.logger.warning(f"Unexpected disconnect from Mosquitto: {rc}")
        
        client.on_connect = wrapped_on_connect
        client.on_disconnect = wrapped_on_disconnect
        if on_message_callback:
            client.on_message = on_message_callback
        if self.config.mqtt_user:
            client.username_pw_set(self.config.mqtt_user, self.config.mqtt_pass)
        
        self.logger.info(f"Connecting to Mosquitto at {self.config.mqtt_broker}:{self.config.mqtt_port}")
        client.connect(self.config.mqtt_broker, self.config.mqtt_port, keepalive=60)
        client.loop_start()
        return client
    
    def connect_iot_sigv4(self):
        """Connect to AWS IoT Core using SigV4 (WebSocket)"""
        if not SDK_AVAILABLE:
            raise Exception("AWSIoTPythonSDK not installed. Run: pip install AWSIoTPythonSDK")
        
        self.logger.info(f"Connecting to AWS IoT Core at {self.config.iot_endpoint} (SigV4)")
        credentials = self.session.get_credentials()
        if credentials is None:
            raise Exception("No AWS credentials found")
        frozen_creds = credentials.get_frozen_credentials()
        
        self.iot_client = AWSIoTMQTTClient(f"{self.name}_forwarder", useWebsocket=True)
        self.iot_client.configureEndpoint(self.config.iot_endpoint, 443)
        self.iot_client.configureIAMCredentials(frozen_creds.access_key, frozen_creds.secret_key, frozen_creds.token)
        self.iot_client.configureAutoReconnectBackoffTime(1, 32, 20)
        self.iot_client.configureOfflinePublishQueueing(-1)
        self.iot_client.configureDrainingFrequency(2)
        self.iot_client.configureConnectDisconnectTimeout(10)
        self.iot_client.configureMQTTOperationTimeout(5)
        
        if self.iot_client.connect():
            self.iot_connected = True
            self.logger.info("Connected to AWS IoT Core")
        else:
            raise Exception("Failed to connect to IoT Core")
        return self.iot_client
    
    def publish_to_iot(self, topic, payload, qos=1):
        if not self.iot_connected or not self.iot_client:
            self.logger.error("Cannot publish to IoT: not connected")
            return False
        try:
            if isinstance(payload, dict):
                payload = json.dumps(payload)
            elif isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            self.iot_client.publish(topic, payload, qos)
            self.logger.debug(f"Published to IoT: {topic}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to publish to IoT: {e}")
            return False
    
    def subscribe_to_iot(self, topic, callback, qos=1):
        if not self.iot_connected or not self.iot_client:
            raise Exception("Cannot subscribe: not connected to IoT Core")
        def wrapped_callback(client, userdata, message):
            try:
                callback(message.topic, message.payload)
            except Exception as e:
                self.logger.error(f"Error in IoT callback: {e}")
        self.iot_client.subscribe(topic, qos, wrapped_callback)
        self.logger.info(f"Subscribed to IoT topic: {topic}")
FORWARDER_LIB_EOF

print_success "Created SigV4-compatible forwarder_lib.py"

################################################################################
# Modify ACK Forwarder for SigV4
################################################################################

print_info "Modifying ACK forwarder to use SigV4..."

# Modify the ACK forwarder to call connect_iot_sigv4 instead of connect_iot
sed 's/self.connect_iot()/self.connect_iot_sigv4()/g' "$INSTALL_DIR/ack_forwarder_github.py" > "$INSTALL_DIR/ack/ack_forwarder.py"

# Update the config section
cat >> "$INSTALL_DIR/ack/ack_forwarder.py" << 'ACK_CONFIG_EOF'

if __name__ == "__main__":
    config = ForwarderConfig({
        'aws_access_key_id': '__AWS_ACCESS_KEY__',
        'aws_secret_access_key': '__AWS_SECRET_KEY__',
        'aws_region': '__AWS_REGION__',
        'iot_endpoint': '__IOT_ENDPOINT__',
        'cloudwatch_namespace': 'DroneC2/ACK',
        'cloudwatch_enabled': __CLOUDWATCH_ENABLED__,
        'mqtt_broker': '__MQTT_BROKER__',
        'mqtt_port': __MQTT_PORT__,
        'mqtt_user': __MQTT_USER__,
        'mqtt_pass': __MQTT_PASS__,
        'drone_id': '__DRONE_ID__'
    })
    forwarder = AckForwarder(config)
    exit(forwarder.run())
ACK_CONFIG_EOF

print_success "Modified ACK forwarder"

################################################################################
# Create New Command Forwarder
################################################################################

print_info "Creating new command forwarder (IoT subscription)..."

cat > "$INSTALL_DIR/command/command_forwarder.py" << 'COMMAND_FORWARDER_EOF'
#!/usr/bin/env python3
"""Command Forwarder: Cloud → Agent (IoT Core Subscription)"""

import json
import time
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from forwarder_lib import ForwarderBase, ForwarderConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class CommandForwarder(ForwarderBase):
    def __init__(self, config):
        super().__init__("command", config)
        
    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("✓ Connected to Mosquitto")
        else:
            self.logger.error(f"Mosquitto connection failed: {rc}")
    
    def on_iot_command(self, topic, payload):
        try:
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            data = json.loads(payload)
            cmd_id = data.get('cmd_id', 'unknown')
            cmd = data.get('cmd', 'unknown')
            drone_id = data.get('drone_id', 'unknown')
            
            self.logger.info(f"Command from IoT: cmd_id={cmd_id}, cmd={cmd}, drone_id={drone_id}")
            
            if not self._validate_command(data):
                self.logger.error(f"Invalid command: {cmd_id}")
                self.publish_metric('CommandValidationFailed', 1)
                return
            
            mosquitto_topic = f"drone/{drone_id}/cmd"
            result = self.mqtt_client.publish(mosquitto_topic, json.dumps(data), qos=1)
            
            if result.rc == 0:
                self.publish_metric('CommandForwarded', 1)
                self.logger.info(f"✓ Forwarded to agent: {cmd_id}")
            else:
                self.publish_metric('CommandForwardFailed', 1)
                self.logger.error(f"✗ Mosquitto publish failed: {result.rc}")
        except Exception as e:
            self.logger.error(f"Error processing command: {e}")
            self.publish_metric('CommandForwardFailed', 1)
    
    def _validate_command(self, cmd_data):
        required_fields = ['cmd_id', 'cmd', 'drone_id']
        for field in required_fields:
            if field not in cmd_data:
                return False
        if cmd_data['drone_id'] != self.config.drone_id:
            return False
        return True
    
    def run(self):
        self.logger.info("=" * 60)
        self.logger.info("Command Forwarder starting (IoT Core subscription)...")
        self.logger.info("=" * 60)
        
        try:
            self.logger.info("Connecting to AWS IoT Core...")
            self.connect_iot_sigv4()
            
            cmd_topic = f"drone/{self.config.drone_id}/cmd"
            self.subscribe_to_iot(cmd_topic, self.on_iot_command, qos=1)
            self.logger.info(f"✓ Subscribed to IoT: {cmd_topic}")
            
            self.logger.info("Connecting to Mosquitto...")
            self.mqtt_client = self.connect_mosquitto(self.on_mqtt_connect, None)
            time.sleep(2)
            
            if not self.mqtt_connected:
                self.logger.error("Failed to connect to Mosquitto")
                return 1
            
            self.logger.info("=" * 60)
            self.logger.info("✓ Command Forwarder running")
            self.logger.info(f"  Listening on IoT: drone/{self.config.drone_id}/cmd")
            self.logger.info(f"  Publishing to Mosquitto: localhost:{self.config.mqtt_port}")
            self.logger.info("=" * 60)
            
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
            return 0
        except Exception as e:
            self.logger.error(f"Fatal error: {e}", exc_info=True)
            return 1

if __name__ == "__main__":
    config = ForwarderConfig({
        'aws_access_key_id': '__AWS_ACCESS_KEY__',
        'aws_secret_access_key': '__AWS_SECRET_KEY__',
        'aws_region': '__AWS_REGION__',
        'iot_endpoint': '__IOT_ENDPOINT__',
        'mqtt_broker': '__MQTT_BROKER__',
        'mqtt_port': __MQTT_PORT__,
        'mqtt_user': __MQTT_USER__,
        'mqtt_pass': __MQTT_PASS__',
        'cloudwatch_namespace': 'DroneC2/Command',
        'cloudwatch_enabled': __CLOUDWATCH_ENABLED__,
        'drone_id': '__DRONE_ID__'
    })
    
    forwarder = CommandForwarder(config)
    exit(forwarder.run())
COMMAND_FORWARDER_EOF

print_success "Created command forwarder"

################################################################################
# Configure Files
################################################################################

print_header "Configuring Files"

# Handle empty MQTT user/pass
if [[ -z "$MQTT_USER" ]]; then
    MQTT_USER_VALUE="None"
    MQTT_PASS_VALUE="None"
else
    MQTT_USER_VALUE="'${MQTT_USER}'"
    MQTT_PASS_VALUE="'${MQTT_PASS}'"
fi

# Configure ACK forwarder
sed -i "s|__AWS_ACCESS_KEY__|${AWS_ACCESS_KEY}|g" "$INSTALL_DIR/ack/ack_forwarder.py"
sed -i "s|__AWS_SECRET_KEY__|${AWS_SECRET_KEY}|g" "$INSTALL_DIR/ack/ack_forwarder.py"
sed -i "s|__AWS_REGION__|${AWS_REGION}|g" "$INSTALL_DIR/ack/ack_forwarder.py"
sed -i "s|__IOT_ENDPOINT__|${IOT_ENDPOINT}|g" "$INSTALL_DIR/ack/ack_forwarder.py"
sed -i "s|__MQTT_BROKER__|${MQTT_BROKER}|g" "$INSTALL_DIR/ack/ack_forwarder.py"
sed -i "s|__MQTT_PORT__|${MQTT_PORT}|g" "$INSTALL_DIR/ack/ack_forwarder.py"
sed -i "s|__MQTT_USER__|${MQTT_USER_VALUE}|g" "$INSTALL_DIR/ack/ack_forwarder.py"
sed -i "s|__MQTT_PASS__|${MQTT_PASS_VALUE}|g" "$INSTALL_DIR/ack/ack_forwarder.py"
sed -i "s|__CLOUDWATCH_ENABLED__|${CLOUDWATCH_ENABLED}|g" "$INSTALL_DIR/ack/ack_forwarder.py"
sed -i "s|__DRONE_ID__|${DRONE_ID}|g" "$INSTALL_DIR/ack/ack_forwarder.py"

# Configure Command forwarder
sed -i "s|__AWS_ACCESS_KEY__|${AWS_ACCESS_KEY}|g" "$INSTALL_DIR/command/command_forwarder.py"
sed -i "s|__AWS_SECRET_KEY__|${AWS_SECRET_KEY}|g" "$INSTALL_DIR/command/command_forwarder.py"
sed -i "s|__AWS_REGION__|${AWS_REGION}|g" "$INSTALL_DIR/command/command_forwarder.py"
sed -i "s|__IOT_ENDPOINT__|${IOT_ENDPOINT}|g" "$INSTALL_DIR/command/command_forwarder.py"
sed -i "s|__MQTT_BROKER__|${MQTT_BROKER}|g" "$INSTALL_DIR/command/command_forwarder.py"
sed -i "s|__MQTT_PORT__|${MQTT_PORT}|g" "$INSTALL_DIR/command/command_forwarder.py"
sed -i "s|__MQTT_USER__|${MQTT_USER_VALUE}|g" "$INSTALL_DIR/command/command_forwarder.py"
sed -i "s|__MQTT_PASS__|${MQTT_PASS_VALUE}|g" "$INSTALL_DIR/command/command_forwarder.py"
sed -i "s|__CLOUDWATCH_ENABLED__|${CLOUDWATCH_ENABLED}|g" "$INSTALL_DIR/command/command_forwarder.py"
sed -i "s|__DRONE_ID__|${DRONE_ID}|g" "$INSTALL_DIR/command/command_forwarder.py"

# Copy forwarder_lib to both directories
cp "$INSTALL_DIR/forwarder_lib.py" "$INSTALL_DIR/ack/"
cp "$INSTALL_DIR/forwarder_lib.py" "$INSTALL_DIR/command/"

print_success "Configuration complete"

################################################################################
# Create Virtual Environments
################################################################################

print_header "Creating Virtual Environments"

print_info "Creating venv for ACK forwarder..."
cd "$INSTALL_DIR/ack"
python3 -m venv venv
source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet boto3 paho-mqtt AWSIoTPythonSDK
deactivate
print_success "ACK forwarder venv created"

print_info "Creating venv for Command forwarder..."
cd "$INSTALL_DIR/command"
python3 -m venv venv
source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet boto3 paho-mqtt AWSIoTPythonSDK
deactivate
print_success "Command forwarder venv created"

################################################################################
# Create Systemd Services
################################################################################

print_header "Creating Systemd Services"

cat > /etc/systemd/system/ack_forwarder.service << EOF
[Unit]
Description=Drone C2 ACK Forwarder (GitHub + SigV4)
After=network.target mosquitto.service
Wants=mosquitto.service

[Service]
Type=simple
User=$ACTUAL_USER
WorkingDirectory=$INSTALL_DIR/ack
ExecStart=$INSTALL_DIR/ack/venv/bin/python3 ack_forwarder.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/command_forwarder.service << EOF
[Unit]
Description=Drone C2 Command Forwarder (IoT Subscription + SigV4)
After=network.target mosquitto.service
Wants=mosquitto.service

[Service]
Type=simple
User=$ACTUAL_USER
WorkingDirectory=$INSTALL_DIR/command
ExecStart=$INSTALL_DIR/command/venv/bin/python3 command_forwarder.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

print_success "Created systemd services"

################################################################################
# Set Permissions
################################################################################

print_header "Setting Permissions"

# CRITICAL: Fix home directory permissions for systemd to access
print_info "Fixing home directory permissions for systemd..."
chmod 755 /home/$ACTUAL_USER

# Fix forwarders directory permissions
chown -R $ACTUAL_USER:$ACTUAL_USER "$INSTALL_DIR"
chmod -R 755 "$INSTALL_DIR"

# Make sure all directories are executable (for systemd to traverse)
find "$INSTALL_DIR" -type d -exec chmod 755 {} \;

# Make Python files readable
find "$INSTALL_DIR" -type f -name "*.py" -exec chmod 644 {} \;

# Make venv binaries executable
find "$INSTALL_DIR" -path "*/venv/bin/*" -type f -exec chmod 755 {} \;

print_success "Permissions set"

################################################################################
# Enable and Start Services
################################################################################

print_header "Enabling and Starting Services"

systemctl daemon-reload
systemctl enable ack_forwarder
systemctl enable command_forwarder
systemctl start ack_forwarder
systemctl start command_forwarder

sleep 3

################################################################################
# Verify Services
################################################################################

print_header "Verifying Services"

if systemctl is-active --quiet ack_forwarder; then
    print_success "ACK forwarder is running"
else
    print_error "ACK forwarder failed to start"
    journalctl -u ack_forwarder -n 20 --no-pager
fi

if systemctl is-active --quiet command_forwarder; then
    print_success "Command forwarder is running"
else
    print_error "Command forwarder failed to start"
    journalctl -u command_forwarder -n 20 --no-pager
fi

################################################################################
# Final Summary
################################################################################

print_header "Deployment Complete!"

cat << EOF
${GREEN}✓${NC} Downloaded ack_forwarder.py from GitHub
${GREEN}✓${NC} Downloaded forwarder_lib.py from GitHub
${GREEN}✓${NC} Modified forwarder_lib for SigV4 support
${GREEN}✓${NC} Created new command forwarder (IoT subscription)
${GREEN}✓${NC} Created virtual environments
${GREEN}✓${NC} Configured forwarders
${GREEN}✓${NC} Created systemd services

${BLUE}Installation Directory:${NC} $INSTALL_DIR

${BLUE}GitHub Source:${NC}
  Repository: cloud-to-drone-command-pipeline
  ACK Forwarder: From GitHub (logic preserved)
  Forwarder Lib: From GitHub (modified for SigV4)
  Command Forwarder: New (IoT subscription)

${BLUE}Service Status:${NC}
EOF

systemctl is-active ack_forwarder && echo -e "  ${GREEN}✓${NC} ack_forwarder: running" || echo -e "  ${RED}✗${NC} ack_forwarder: not running"
systemctl is-active command_forwarder && echo -e "  ${GREEN}✓${NC} command_forwarder: running" || echo -e "  ${RED}✗${NC} command_forwarder: not running"

cat << EOF

${BLUE}Architecture:${NC}
  ACK Flow:     Mosquitto → ACK Forwarder → AWS IoT Core
  Command Flow: AWS IoT Core → Command Forwarder → Mosquitto

${BLUE}Authentication:${NC}
  Method: SigV4 (WebSocket)
  Access Key: ${AWS_ACCESS_KEY:0:10}...

${BLUE}Useful Commands:${NC}
  sudo systemctl status ack_forwarder
  sudo journalctl -u ack_forwarder -f
  sudo systemctl restart ack_forwarder

EOF

print_success "Deployment script completed successfully!"
exit 0