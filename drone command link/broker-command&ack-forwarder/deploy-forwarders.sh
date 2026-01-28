#!/bin/bash
set -e

echo "============================================================"
echo "Drone C2 Forwarders Deployment (GitHub Architecture)"
echo "============================================================"
echo ""

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then 
    echo "[ERROR] This script must be run with sudo"
    exit 1
fi

# Get the actual user who invoked sudo
ACTUAL_USER="${SUDO_USER}"
if [ -z "$ACTUAL_USER" ]; then
    echo "[ERROR] Could not detect user. Please run with: sudo ./script.sh"
    exit 1
fi

# Set installation directory based on actual user
INSTALL_DIR="/home/$ACTUAL_USER/forwarders"

echo "[INFO] Detected user: $ACTUAL_USER"
echo "[INFO] Installation directory: $INSTALL_DIR"

# Configuration - UPDATE THESE VALUES
AWS_REGION="us-east-1"
DRONE_ID="drone_001"
SQS_QUEUE_URL=$1  # User must provide this
MQTT_BROKER="localhost"
MQTT_PORT="1883"

# Verify AWS credentials exist
if [ ! -f "/home/$ACTUAL_USER/.aws/credentials" ]; then
    echo "[ERROR] AWS credentials not found"
    echo "Please configure AWS credentials first with: aws configure"
    exit 1
fi

# Check for required parameters
if [ -z "$SQS_QUEUE_URL" ]; then
    echo "[ERROR] SQS_QUEUE_URL not set"
    echo "Please edit this script and set SQS_QUEUE_URL variable"
    echo "Example: https://sqs.us-east-1.amazonaws.com/123456789012/drone-commands-queue"
    exit 1
fi

echo ""
echo "============================================================"
echo "Creating Directory Structure"
echo "============================================================"

# Create directories as the actual user
sudo -u $ACTUAL_USER mkdir -p "$INSTALL_DIR"/{lib,ack,command}/{venv,logs}

# Set proper ownership and permissions
chown -R $ACTUAL_USER:$ACTUAL_USER "$INSTALL_DIR"
chmod -R 755 "$INSTALL_DIR"

echo "[OK] Directory structure created"

echo ""
echo "============================================================"
echo "Installing System Dependencies"
echo "============================================================"

# Install Python3 if needed
if ! command -v python3 &> /dev/null; then
    echo "[INFO] Installing Python3..."
    yum install -y python3 python3-pip
else
    echo "[OK] Python3 already installed"
fi

# Install Mosquitto if needed
if ! command -v mosquitto &> /dev/null; then
    echo "[INFO] Installing Mosquitto..."
    yum install -y mosquitto mosquitto-clients
    systemctl enable mosquitto
    systemctl start mosquitto
else
    echo "[OK] Mosquitto already installed"
fi

# Ensure Mosquitto is running
if ! systemctl is-active --quiet mosquitto; then
    echo "[INFO] Starting Mosquitto..."
    systemctl start mosquitto
fi

echo ""
echo "============================================================"
echo "Creating Virtual Environments"
echo "============================================================"

echo "[INFO] Creating ACK forwarder venv..."
sudo -u $ACTUAL_USER python3 -m venv "$INSTALL_DIR/ack/venv"

echo "[INFO] Creating Command forwarder venv..."
sudo -u $ACTUAL_USER python3 -m venv "$INSTALL_DIR/command/venv"

echo ""
echo "============================================================"
echo "Installing Python Dependencies"
echo "============================================================"

echo "[INFO] Installing ACK forwarder dependencies..."
sudo -u $ACTUAL_USER "$INSTALL_DIR/ack/venv/bin/pip" install --upgrade pip
sudo -u $ACTUAL_USER "$INSTALL_DIR/ack/venv/bin/pip" install paho-mqtt boto3 requests

echo "[INFO] Installing Command forwarder dependencies..."
sudo -u $ACTUAL_USER "$INSTALL_DIR/command/venv/bin/pip" install --upgrade pip
sudo -u $ACTUAL_USER "$INSTALL_DIR/command/venv/bin/pip" install paho-mqtt boto3 requests

echo ""
echo "============================================================"
echo "Creating Forwarder Library (forwarder_lib.py)"
echo "============================================================"

# Create shared library
cat > "$INSTALL_DIR/lib/forwarder_lib.py" << 'EOFLIB'
#!/usr/bin/env python3
"""
Shared library for all forwarders
Provides common connection logic and utilities
"""

import logging
import paho.mqtt.client as mqtt
import boto3

class ForwarderConfig:
    """Configuration holder for forwarders"""
    
    def __init__(self, config_dict):
        # AWS
        self.aws_access_key_id = config_dict.get('aws_access_key_id')
        self.aws_secret_access_key = config_dict.get('aws_secret_access_key')
        self.aws_region = config_dict.get('aws_region', 'us-east-1')
        self.iot_endpoint = config_dict.get('iot_endpoint')
        
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
        
        # SQS (for command forwarder)
        self.sqs_queue_url = config_dict.get('sqs_queue_url')

class ForwarderBase:
    """Base class for all forwarders"""
    
    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.mqtt_client = None
        self.mqtt_connected = False
        
        # Setup logging
        self.logger = logging.getLogger(name)
        
        # Setup AWS session
        if config.aws_access_key_id:
            self.session = boto3.Session(
                aws_access_key_id=config.aws_access_key_id,
                aws_secret_access_key=config.aws_secret_access_key,
                region_name=config.aws_region
            )
        else:
            # Use default credentials
            self.session = boto3.Session(region_name=config.aws_region)
        
        # Setup CloudWatch
        if config.cloudwatch_enabled and config.cloudwatch_namespace:
            self.cloudwatch = boto3.client('cloudwatch', region_name=config.aws_region)
        else:
            self.cloudwatch = None
    
    def connect_mosquitto(self, on_connect_callback, on_message_callback):
        """Connect to local Mosquitto broker"""
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        
        def wrapped_on_connect(client, userdata, flags, rc, properties):
            self.mqtt_connected = (rc == 0)
            if on_connect_callback:
                on_connect_callback(client, userdata, flags, rc)
        
        def wrapped_on_disconnect(client, userdata, rc, properties):
            self.mqtt_connected = False
            if rc != 0:
                self.logger.warning(f"Unexpected disconnect from Mosquitto: {rc}")
        
        client.on_connect = wrapped_on_connect
        client.on_disconnect = wrapped_on_disconnect
        
        if on_message_callback:
            client.on_message = on_message_callback
        
        # Set auth if provided
        if self.config.mqtt_user:
            client.username_pw_set(self.config.mqtt_user, self.config.mqtt_pass)
        
        # Connect
        self.logger.info(f"Connecting to Mosquitto at {self.config.mqtt_broker}:{self.config.mqtt_port}")
        client.connect(self.config.mqtt_broker, self.config.mqtt_port, keepalive=60)
        client.loop_start()
        
        return client
EOFLIB

chown $ACTUAL_USER:$ACTUAL_USER "$INSTALL_DIR/lib/forwarder_lib.py"
chmod 644 "$INSTALL_DIR/lib/forwarder_lib.py"

echo "[OK] Created forwarder_lib.py"

echo ""
echo "============================================================"
echo "Creating ACK Forwarder Script"
echo "============================================================"

cat > "$INSTALL_DIR/ack/ack_forwarder.py" << 'EOFACK'
#!/usr/bin/env python3
"""
ACK Forwarder: Agent → Cloud
Subscribes to local Mosquitto and forwards ACKs to AWS IoT Core
"""

import json
import time
import logging
import sys
import os
import signal

# Add lib directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lib'))

import paho.mqtt.client as mqtt
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from forwarder_lib import ForwarderBase, ForwarderConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.expanduser('~/forwarders/ack/logs/ack_forwarder.log')),
        logging.StreamHandler(sys.stdout)
    ]
)

class AckForwarder(ForwarderBase):
    def __init__(self, config):
        super().__init__("ack", config)
        self.iot_endpoint = config.iot_endpoint
        self.running = True
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, sig, frame):
        self.logger.info("Shutdown signal received")
        self.running = False
        if self.mqtt_client:
            self.mqtt_client.disconnect()
        sys.exit(0)
    
    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("✓ Connected to Mosquitto")
            # Subscribe to ACK topic
            client.subscribe(f"drone/+/cmd_ack", qos=1)
            self.logger.info("✓ Subscribed to drone/+/cmd_ack")
        else:
            self.logger.error(f"Mosquitto connection failed: {rc}")
    
    def on_mqtt_message(self, client, userdata, msg):
        """Forward ACK to AWS IoT Core"""
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            
            self.logger.info(f"Received ACK from {topic}")
            
            # Validate JSON
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                self.logger.error(f"Invalid JSON payload: {payload}")
                return
            
            # Publish to AWS IoT Core using HTTPS + SigV4
            url = f"https://{self.iot_endpoint}/topics/{topic}"
            
            request = AWSRequest(
                method='POST',
                url=url,
                data=payload,
                headers={'Content-Type': 'application/json'}
            )
            
            # Sign with SigV4
            credentials = self.session.get_credentials()
            SigV4Auth(credentials, 'iotdevicegateway', self.config.aws_region).add_auth(request)
            
            # Send
            response = requests.post(
                url,
                headers=dict(request.headers),
                data=payload
            )
            
            if response.status_code == 200:
                self.logger.info(f"✓ Forwarded ACK to IoT Core: {topic}")
            else:
                self.logger.error(f"IoT publish failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            self.logger.error(f"Error processing ACK: {e}", exc_info=True)
    
    def run(self):
        self.logger.info("=" * 60)
        self.logger.info("ACK Forwarder starting...")
        self.logger.info(f"  IoT Endpoint: {self.iot_endpoint}")
        self.logger.info("=" * 60)
        
        try:
            # Connect to Mosquitto
            self.mqtt_client = self.connect_mosquitto(
                self.on_mqtt_connect,
                self.on_mqtt_message
            )
            
            # Wait for connection
            time.sleep(2)
            
            if not self.mqtt_connected:
                self.logger.error("Failed to connect to Mosquitto")
                return 1
            
            self.logger.info("=" * 60)
            self.logger.info("✓ ACK Forwarder running")
            self.logger.info("=" * 60)
            
            # Keep alive
            heartbeat_counter = 0
            while self.running:
                time.sleep(1)
                heartbeat_counter += 1
                if heartbeat_counter >= 60:
                    self.logger.info("ACK Forwarder running...")
                    heartbeat_counter = 0
                    
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
            return 0
        except Exception as e:
            self.logger.error(f"Fatal error: {e}", exc_info=True)
            return 1

if __name__ == "__main__":
    # Read config from environment or use defaults
    config = ForwarderConfig({
        'aws_region': os.environ.get('AWS_REGION', 'us-east-1'),
        'iot_endpoint': os.environ.get('IOT_ENDPOINT'),
        'mqtt_broker': os.environ.get('MQTT_BROKER', 'localhost'),
        'mqtt_port': int(os.environ.get('MQTT_PORT', 1883)),
        'cloudwatch_namespace': 'DroneC2/ACK',
        'cloudwatch_enabled': True,
        'drone_id': os.environ.get('DRONE_ID', 'drone_001')
    })
    
    forwarder = AckForwarder(config)
    exit(forwarder.run())
EOFACK

chown $ACTUAL_USER:$ACTUAL_USER "$INSTALL_DIR/ack/ack_forwarder.py"
chmod +x "$INSTALL_DIR/ack/ack_forwarder.py"

echo "[OK] Created ack_forwarder.py"

echo ""
echo "============================================================"
echo "Creating Command Forwarder Script"
echo "============================================================"

cat > "$INSTALL_DIR/command/command_forwarder.py" << 'EOFCMD'
#!/usr/bin/env python3
"""
Command Forwarder: Cloud → Agent (via SQS)
Polls SQS for commands and forwards to local Mosquitto
"""

import json
import time
import logging
import sys
import os
import signal

# Add lib directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lib'))

import paho.mqtt.client as mqtt
from forwarder_lib import ForwarderBase, ForwarderConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.expanduser('~/forwarders/command/logs/command_forwarder.log')),
        logging.StreamHandler(sys.stdout)
    ]
)

class CommandForwarder(ForwarderBase):
    def __init__(self, config):
        super().__init__("command", config)
        self.sqs_queue_url = config.sqs_queue_url
        self.sqs_client = self.session.client('sqs')
        self.running = True
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, sig, frame):
        self.logger.info("Shutdown signal received")
        self.running = False
        if self.mqtt_client:
            self.mqtt_client.disconnect()
        sys.exit(0)
    
    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("✓ Connected to Mosquitto")
        else:
            self.logger.error(f"Mosquitto connection failed: {rc}")
    
    def poll_commands(self):
        """Poll SQS for commands from cloud"""
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.sqs_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,  # Long polling
                VisibilityTimeout=30
            )
            
            if 'Messages' not in response:
                return
            
            for message in response['Messages']:
                try:
                    # Parse message body
                    body = json.loads(message['Body'])
                    
                    cmd_id = body.get('cmd_id', 'unknown')
                    cmd = body.get('cmd', 'unknown')
                    drone_id = body.get('drone_id', 'unknown')
                    params = body.get('params', {})
                    
                    self.logger.info(f"Command from SQS: cmd_id={cmd_id}, cmd={cmd}, drone={drone_id}")
                    
                    # Forward to local Mosquitto
                    topic = f"drone/{drone_id}/cmd"
                    payload = json.dumps(body)
                    
                    result = self.mqtt_client.publish(topic, payload, qos=1)
                    result.wait_for_publish()
                    
                    self.logger.info(f"✓ Forwarded command to Mosquitto: {topic}")
                    
                    # Delete message from queue
                    self.sqs_client.delete_message(
                        QueueUrl=self.sqs_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    self.logger.info(f"✓ Deleted message from SQS")
                    
                except Exception as e:
                    self.logger.error(f"Error processing SQS message: {e}", exc_info=True)
                    
        except Exception as e:
            self.logger.error(f"Error polling SQS: {e}")
            time.sleep(5)  # Back off on error
    
    def run(self):
        self.logger.info("=" * 60)
        self.logger.info("Command Forwarder starting...")
        self.logger.info(f"  SQS Queue: {self.sqs_queue_url}")
        self.logger.info("=" * 60)
        
        try:
            # Connect to Mosquitto
            self.mqtt_client = self.connect_mosquitto(self.on_mqtt_connect, None)
            time.sleep(2)
            
            if not self.mqtt_connected:
                self.logger.error("Failed to connect to Mosquitto")
                return 1
            
            self.logger.info("=" * 60)
            self.logger.info("✓ Command Forwarder running")
            self.logger.info("=" * 60)
            
            # Main polling loop
            while self.running:
                self.poll_commands()
                
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
            return 0
        except Exception as e:
            self.logger.error(f"Fatal error: {e}", exc_info=True)
            return 1

if __name__ == "__main__":
    # Read config from environment
    config = ForwarderConfig({
        'aws_region': os.environ.get('AWS_REGION', 'us-east-1'),
        'mqtt_broker': os.environ.get('MQTT_BROKER', 'localhost'),
        'mqtt_port': int(os.environ.get('MQTT_PORT', 1883)),
        'cloudwatch_namespace': 'DroneC2/Command',
        'cloudwatch_enabled': True,
        'drone_id': os.environ.get('DRONE_ID', 'drone_001'),
        'sqs_queue_url': os.environ.get('SQS_QUEUE_URL')
    })
    
    forwarder = CommandForwarder(config)
    exit(forwarder.run())
EOFCMD

chown $ACTUAL_USER:$ACTUAL_USER "$INSTALL_DIR/command/command_forwarder.py"
chmod +x "$INSTALL_DIR/command/command_forwarder.py"

echo "[OK] Created command_forwarder.py"

echo ""
echo "============================================================"
echo "Creating Environment Configuration"
echo "============================================================"

# Get IoT endpoint
echo "[INFO] Fetching IoT endpoint..."
IOT_ENDPOINT=$(aws iot describe-endpoint --endpoint-type iot:Data-ATS --region $AWS_REGION --query 'endpointAddress' --output text)
echo "[INFO] IoT Endpoint: $IOT_ENDPOINT"

# Create config file
cat > "$INSTALL_DIR/forwarder.env" << EOF
# AWS Configuration
AWS_REGION=$AWS_REGION
IOT_ENDPOINT=$IOT_ENDPOINT

# Mosquitto Configuration
MQTT_BROKER=$MQTT_BROKER
MQTT_PORT=$MQTT_PORT

# Drone Configuration
DRONE_ID=$DRONE_ID

# SQS Configuration (for command forwarder)
SQS_QUEUE_URL=$SQS_QUEUE_URL
EOF

chown $ACTUAL_USER:$ACTUAL_USER "$INSTALL_DIR/forwarder.env"
chmod 600 "$INSTALL_DIR/forwarder.env"

echo "[OK] Created forwarder.env"

echo ""
echo "============================================================"
echo "Creating Systemd Services"
echo "============================================================"

# ACK Forwarder Service
cat > /etc/systemd/system/ack_forwarder.service << EOFSERVICE
[Unit]
Description=Drone C2 ACK Forwarder (Mosquitto to AWS IoT Core)
After=network.target mosquitto.service
Wants=mosquitto.service

[Service]
Type=simple
User=$ACTUAL_USER
WorkingDirectory=$INSTALL_DIR/ack
EnvironmentFile=$INSTALL_DIR/forwarder.env
Environment="PATH=$INSTALL_DIR/ack/venv/bin:/usr/local/bin:/usr/bin:/bin"
Environment="HOME=/home/$ACTUAL_USER"
Environment="PYTHONPATH=$INSTALL_DIR/lib"
ExecStart=$INSTALL_DIR/ack/venv/bin/python3 $INSTALL_DIR/ack/ack_forwarder.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ack_forwarder

[Install]
WantedBy=multi-user.target
EOFSERVICE

# Command Forwarder Service
cat > /etc/systemd/system/command_forwarder.service << EOFSERVICE
[Unit]
Description=Drone C2 Command Forwarder (SQS to Mosquitto)
After=network.target mosquitto.service
Wants=mosquitto.service

[Service]
Type=simple
User=$ACTUAL_USER
WorkingDirectory=$INSTALL_DIR/command
EnvironmentFile=$INSTALL_DIR/forwarder.env
Environment="PATH=$INSTALL_DIR/command/venv/bin:/usr/local/bin:/usr/bin:/bin"
Environment="HOME=/home/$ACTUAL_USER"
Environment="PYTHONPATH=$INSTALL_DIR/lib"
ExecStart=$INSTALL_DIR/command/venv/bin/python3 $INSTALL_DIR/command/command_forwarder.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=command_forwarder

[Install]
WantedBy=multi-user.target
EOFSERVICE

echo "[OK] Created systemd services"

echo ""
echo "============================================================"
echo "Enabling and Starting Services"
echo "============================================================"

systemctl daemon-reload

systemctl enable ack_forwarder.service
systemctl enable command_forwarder.service

systemctl restart ack_forwarder.service
systemctl restart command_forwarder.service

sleep 2

echo ""
echo "============================================================"
echo "Verifying Services"
echo "============================================================"

ACK_STATUS=$(systemctl is-active ack_forwarder.service)
CMD_STATUS=$(systemctl is-active command_forwarder.service)

if [ "$ACK_STATUS" = "active" ]; then
    echo "[OK] ACK forwarder is running"
else
    echo "[ERROR] ACK forwarder failed to start"
    journalctl -u ack_forwarder.service -n 20 --no-pager
fi

if [ "$CMD_STATUS" = "active" ]; then
    echo "[OK] Command forwarder is running"
else
    echo "[ERROR] Command forwarder failed to start"
    journalctl -u command_forwarder.service -n 20 --no-pager
fi

echo ""
echo "============================================================"
echo "Deployment Complete"
echo "============================================================"
echo ""
echo "Installation directory: $INSTALL_DIR"
echo ""
echo "Configuration file: $INSTALL_DIR/forwarder.env"
echo "  - Update this file and restart services to change config"
echo ""
echo "Services:"
echo "  - ack_forwarder.service (Mosquitto → IoT Core)"
echo "  - command_forwarder.service (SQS → Mosquitto)"
echo ""
echo "Logs:"
echo "  sudo journalctl -u ack_forwarder -f"
echo "  sudo journalctl -u command_forwarder -f"
echo "  tail -f $INSTALL_DIR/ack/logs/ack_forwarder.log"
echo "  tail -f $INSTALL_DIR/command/logs/command_forwarder.log"
echo ""
echo "IMPORTANT: Update SQS_QUEUE_URL in $INSTALL_DIR/forwarder.env"
echo "           Then run: sudo systemctl restart command_forwarder"
echo ""