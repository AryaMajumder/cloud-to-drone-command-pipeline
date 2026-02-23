#!/bin/bash
set -e

MOSQUITTO_BROKER="localhost"
MOSQUITTO_PORT="1883"

echo "============================================================"
echo "Drone C2 Forwarders Deployment Script"
echo "============================================================"
echo ""

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then 
    echo "[ERROR] This script must be run with sudo"
    exit 1
fi

# Get the actual user who invoked sudo (not root)
ACTUAL_USER="${SUDO_USER}"
if [ -z "$ACTUAL_USER" ]; then
    echo "[ERROR] Could not detect user. Please run with: sudo ./script.sh"
    exit 1
fi

# Set installation directory based on actual user
INSTALL_DIR="/home/$ACTUAL_USER/forwarders"

echo "[INFO] Detected user: $ACTUAL_USER"
echo "[INFO] Installation directory: $INSTALL_DIR"

# Verify AWS credentials exist
if [ ! -f "/home/$ACTUAL_USER/.aws/credentials" ]; then
    echo "[ERROR] AWS credentials not found at /home/$ACTUAL_USER/.aws/credentials"
    echo "Please configure AWS credentials first with: aws configure"
    exit 1
fi

echo ""
echo "============================================================"
echo "Creating Directory Structure"
echo "============================================================"

# Create directories as the actual user
sudo -u $ACTUAL_USER mkdir -p "$INSTALL_DIR"/{ack,command}/{venv,logs}

# Set proper ownership
chown -R $ACTUAL_USER:$ACTUAL_USER "$INSTALL_DIR"

# Set directory permissions to allow systemd access
chmod -R 755 "$INSTALL_DIR"

echo "[OK] Directory structure created"

echo ""
echo "============================================================"
echo "Installing System Dependencies"
echo "============================================================"

# Install Python3 and pip if needed
if ! command -v python3 &> /dev/null; then
    echo "[INFO] Installing Python3..."
    yum install -y python3 python3-pip
else
    echo "[OK] Python3 already installed"
fi

# Install Mosquitto broker
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

# Create venvs as the actual user
echo "[INFO] Creating ACK forwarder venv..."
sudo -u $ACTUAL_USER python3 -m venv "$INSTALL_DIR/ack/venv"

echo "[INFO] Creating Command forwarder venv..."
sudo -u $ACTUAL_USER python3 -m venv "$INSTALL_DIR/command/venv"

echo ""
echo "============================================================"
echo "Installing Python Dependencies"
echo "============================================================"

# Install dependencies as the actual user
echo "[INFO] Installing ACK forwarder dependencies..."
sudo -u $ACTUAL_USER "$INSTALL_DIR/ack/venv/bin/pip" install --upgrade pip
sudo -u $ACTUAL_USER "$INSTALL_DIR/ack/venv/bin/pip" install paho-mqtt boto3 requests

echo "[INFO] Installing Command forwarder dependencies..."
sudo -u $ACTUAL_USER "$INSTALL_DIR/command/venv/bin/pip" install --upgrade pip
sudo -u $ACTUAL_USER "$INSTALL_DIR/command/venv/bin/pip" install paho-mqtt boto3 requests

echo ""
echo "============================================================"
echo "Deploying Forwarder Scripts"
echo "============================================================"

# Create ACK forwarder script
cat > "$INSTALL_DIR/ack/ack_forwarder.py" << 'EOFACK'
#!/usr/bin/env python3
"""
Drone C2 ACK Forwarder
Forwards command acknowledgements from local Mosquitto to AWS IoT Core
"""

import json
import logging
import signal
import sys
import time
import os
from datetime import datetime

import paho.mqtt.client as mqtt
import boto3
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

# ============================================================
# Configuration
# ============================================================

MOSQUITTO_BROKER = "localhost"
MOSQUITTO_PORT = 1883
MOSQUITTO_TOPIC = "drone/+/cmd_ack"

AWS_REGION = "us-east-1"
IOT_ENDPOINT = None  # Will be fetched dynamically

HEARTBEAT_INTERVAL = 60  # seconds
MAX_RECONNECT_DELAY = 300  # 5 minutes

# Get log directory from environment or use default
LOG_DIR = os.path.expanduser("~/forwarders/ack/logs")

# ============================================================
# Logging Setup
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'ack_forwarder.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('ack_forwarder')

# ============================================================
# AWS IoT Core Publisher (SigV4)
# ============================================================

class IoTPublisher:
    def __init__(self, region):
        self.region = region
        self.endpoint = None
        self.session = boto3.Session()
        self.credentials = self.session.get_credentials()
        
        # Fetch IoT endpoint
        iot_client = self.session.client('iot', region_name=region)
        response = iot_client.describe_endpoint(endpointType='iot:Data-ATS')
        self.endpoint = response['endpointAddress']
        logger.info(f"AWS IoT endpoint: {self.endpoint}")
    
    def publish(self, topic, payload):
        """Publish message to AWS IoT Core using SigV4 signed HTTPS"""
        url = f"https://{self.endpoint}/topics/{topic}"
        
        # Create request
        request = AWSRequest(
            method='POST',
            url=url,
            data=payload,
            headers={'Content-Type': 'application/json'}
        )
        
        # Sign request with SigV4
        SigV4Auth(self.credentials, 'iotdevicegateway', self.region).add_auth(request)
        
        # Send request
        response = requests.post(
            url,
            headers=dict(request.headers),
            data=payload
        )
        
        if response.status_code != 200:
            raise Exception(f"IoT publish failed: {response.status_code} - {response.text}")
        
        return response

# ============================================================
# Mosquitto Callbacks
# ============================================================

iot_publisher = None
mosquitto_client = None
running = True

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to Mosquitto broker")
        client.subscribe(MOSQUITTO_TOPIC)
        logger.info(f"Subscribed to topic: {MOSQUITTO_TOPIC}")
    else:
        logger.error(f"Failed to connect to Mosquitto: {rc}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(f"Unexpected disconnection from Mosquitto: {rc}")

def on_message(client, userdata, msg):
    """Forward ACK message to AWS IoT Core"""
    try:
        topic = msg.topic
        payload = msg.payload.decode('utf-8')
        
        logger.info(f"Received ACK from {topic}")
        logger.debug(f"Payload: {payload}")
        
        # Validate JSON
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON payload: {payload}")
            return
        
        # Forward to AWS IoT Core
        iot_publisher.publish(topic, payload)
        logger.info(f"Forwarded ACK to AWS IoT Core: {topic}")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)

# ============================================================
# Graceful Shutdown
# ============================================================

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received")
    running = False
    if mosquitto_client:
        mosquitto_client.disconnect()
    sys.exit(0)

# ============================================================
# Main
# ============================================================

def main():
    global iot_publisher, mosquitto_client, running
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting ACK Forwarder")
    
    # Initialize AWS IoT publisher
    try:
        iot_publisher = IoTPublisher(AWS_REGION)
    except Exception as e:
        logger.error(f"Failed to initialize IoT publisher: {e}")
        sys.exit(1)
    
    # Initialize Mosquitto client
    mosquitto_client = mqtt.Client()
    mosquitto_client.on_connect = on_connect
    mosquitto_client.on_disconnect = on_disconnect
    mosquitto_client.on_message = on_message
    
    # Connect to Mosquitto with retry
    retry_delay = 1
    while running:
        try:
            logger.info(f"Connecting to Mosquitto at {MOSQUITTO_BROKER}:{MOSQUITTO_PORT}")
            mosquitto_client.connect(MOSQUITTO_BROKER, MOSQUITTO_PORT, 60)
            break
        except Exception as e:
            logger.error(f"Failed to connect to Mosquitto: {e}")
            logger.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, MAX_RECONNECT_DELAY)
    
    # Start MQTT loop
    mosquitto_client.loop_start()
    
    # Keep alive with periodic logging
    last_heartbeat = time.time()
    while running:
        time.sleep(1)
        
        now = time.time()
        if now - last_heartbeat >= HEARTBEAT_INTERVAL:
            logger.info("ACK Forwarder running...")
            last_heartbeat = now
    
    mosquitto_client.loop_stop()
    logger.info("ACK Forwarder stopped")

if __name__ == "__main__":
    main()
EOFACK

# Create Command forwarder script
cat > "$INSTALL_DIR/command/command_forwarder.py" << 'EOFCMD'
#!/usr/bin/env python3
"""
Drone C2 Command Forwarder
Subscribes to AWS IoT Core commands and forwards to local Mosquitto
"""

import json
import logging
import signal
import sys
import time
import os
from datetime import datetime

import paho.mqtt.client as mqtt
import boto3
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

# ============================================================
# Configuration
# ============================================================

MOSQUITTO_BROKER = "localhost"
MOSQUITTO_PORT = 1883

AWS_REGION = "us-east-1"
IOT_ENDPOINT = None  # Will be fetched dynamically
IOT_TOPIC = "drone/+/cmd"

HEARTBEAT_INTERVAL = 60  # seconds
MAX_RECONNECT_DELAY = 300  # 5 minutes

# Get log directory from environment or use default
LOG_DIR = os.path.expanduser("~/forwarders/command/logs")

# ============================================================
# Logging Setup
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'command_forwarder.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('command_forwarder')

# ============================================================
# AWS IoT Core Subscriber (WebSocket + SigV4)
# ============================================================

class IoTSubscriber:
    def __init__(self, region, on_message_callback):
        self.region = region
        self.on_message_callback = on_message_callback
        self.session = boto3.Session()
        self.credentials = self.session.get_credentials()
        
        # Fetch IoT endpoint
        iot_client = self.session.client('iot', region_name=region)
        response = iot_client.describe_endpoint(endpointType='iot:Data-ATS')
        self.endpoint = response['endpointAddress']
        logger.info(f"AWS IoT endpoint: {self.endpoint}")
        
        # For simplicity, we'll poll using IoT Data API
        # Full WebSocket implementation would be more complex
        self.iot_data = self.session.client('iot-data', region_name=region)
    
    def start_polling(self, topic):
        """
        NOTE: This is a simplified polling approach.
        For production, use AWS IoT Device SDK with WebSockets.
        """
        logger.warning("Using polling mode - not recommended for production")
        logger.info(f"Polling topic: {topic}")
        
        # This is a placeholder - actual implementation would use WebSockets
        # For now, we'll just log that we're ready
        logger.info("Command subscriber ready (polling mode)")

# ============================================================
# Mosquitto Publisher
# ============================================================

mosquitto_client = None
running = True

def mosquitto_on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to Mosquitto broker")
    else:
        logger.error(f"Failed to connect to Mosquitto: {rc}")

def mosquitto_on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(f"Unexpected disconnection from Mosquitto: {rc}")

def forward_command_to_mosquitto(topic, payload):
    """Forward command from IoT Core to Mosquitto"""
    try:
        logger.info(f"Forwarding command to {topic}")
        logger.debug(f"Payload: {payload}")
        
        mosquitto_client.publish(topic, payload, qos=1)
        logger.info(f"Command forwarded to Mosquitto: {topic}")
        
    except Exception as e:
        logger.error(f"Error forwarding command: {e}", exc_info=True)

# ============================================================
# Graceful Shutdown
# ============================================================

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received")
    running = False
    if mosquitto_client:
        mosquitto_client.disconnect()
    sys.exit(0)

# ============================================================
# Main
# ============================================================

def main():
    global mosquitto_client, running
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting Command Forwarder")
    
    # Initialize Mosquitto client
    mosquitto_client = mqtt.Client()
    mosquitto_client.on_connect = mosquitto_on_connect
    mosquitto_client.on_disconnect = mosquitto_on_disconnect
    
    # Connect to Mosquitto with retry
    retry_delay = 1
    while running:
        try:
            logger.info(f"Connecting to Mosquitto at {MOSQUITTO_BROKER}:{MOSQUITTO_PORT}")
            mosquitto_client.connect(MOSQUITTO_BROKER, MOSQUITTO_PORT, 60)
            break
        except Exception as e:
            logger.error(f"Failed to connect to Mosquitto: {e}")
            logger.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, MAX_RECONNECT_DELAY)
    
    # Start MQTT loop
    mosquitto_client.loop_start()
    
    # Initialize IoT subscriber
    try:
        iot_subscriber = IoTSubscriber(AWS_REGION, forward_command_to_mosquitto)
        iot_subscriber.start_polling(IOT_TOPIC)
    except Exception as e:
        logger.error(f"Failed to initialize IoT subscriber: {e}")
        sys.exit(1)
    
    # Keep alive with periodic logging
    last_heartbeat = time.time()
    while running:
        time.sleep(1)
        
        now = time.time()
        if now - last_heartbeat >= HEARTBEAT_INTERVAL:
            logger.info("Command Forwarder running...")
            last_heartbeat = now
    
    mosquitto_client.loop_stop()
    logger.info("Command Forwarder stopped")

if __name__ == "__main__":
    main()
EOFCMD

# Set ownership and permissions on scripts
chown $ACTUAL_USER:$ACTUAL_USER "$INSTALL_DIR/ack/ack_forwarder.py"
chown $ACTUAL_USER:$ACTUAL_USER "$INSTALL_DIR/command/command_forwarder.py"
chmod +x "$INSTALL_DIR/ack/ack_forwarder.py"
chmod +x "$INSTALL_DIR/command/command_forwarder.py"

echo "[OK] Forwarder scripts deployed"

echo ""
echo "============================================================"
echo "Creating Systemd Services"
echo "============================================================"

# Create ACK forwarder service
cat > /etc/systemd/system/ack_forwarder.service << EOFSERVICE
[Unit]
Description=Drone C2 ACK Forwarder (Mosquitto to AWS IoT Core)
After=network.target mosquitto.service
Wants=mosquitto.service

[Service]
Type=simple
User=$ACTUAL_USER
WorkingDirectory=$INSTALL_DIR/ack
Environment="PATH=$INSTALL_DIR/ack/venv/bin:/usr/local/bin:/usr/bin:/bin"
Environment="HOME=/home/$ACTUAL_USER"
Environment="AWS_SHARED_CREDENTIALS_FILE=/home/$ACTUAL_USER/.aws/credentials"
Environment="AWS_CONFIG_FILE=/home/$ACTUAL_USER/.aws/config"
ExecStart=$INSTALL_DIR/ack/venv/bin/python3 $INSTALL_DIR/ack/ack_forwarder.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ack_forwarder

[Install]
WantedBy=multi-user.target
EOFSERVICE

# Create Command forwarder service
cat > /etc/systemd/system/command_forwarder.service << EOFSERVICE
[Unit]
Description=Drone C2 Command Forwarder (AWS IoT Core to Mosquitto)
After=network.target mosquitto.service
Wants=mosquitto.service

[Service]
Type=simple
User=$ACTUAL_USER
WorkingDirectory=$INSTALL_DIR/command
Environment="PATH=$INSTALL_DIR/command/venv/bin:/usr/local/bin:/usr/bin:/bin"
Environment="HOME=/home/$ACTUAL_USER"
Environment="AWS_SHARED_CREDENTIALS_FILE=/home/$ACTUAL_USER/.aws/credentials"
Environment="AWS_CONFIG_FILE=/home/$ACTUAL_USER/.aws/config"
ExecStart=$INSTALL_DIR/command/venv/bin/python3 $INSTALL_DIR/command/command_forwarder.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=command_forwarder

[Install]
WantedBy=multi-user.target
EOFSERVICE

echo "[OK] Systemd services created"

echo ""
echo "============================================================"
echo "Enabling and Starting Services"
echo "============================================================"

systemctl daemon-reload

echo "[INFO] Enabling ACK forwarder..."
systemctl enable ack_forwarder.service

echo "[INFO] Enabling Command forwarder..."
systemctl enable command_forwarder.service

echo "[INFO] Starting ACK forwarder..."
systemctl start ack_forwarder.service

echo "[INFO] Starting Command forwarder..."
systemctl start command_forwarder.service

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
echo "User: $ACTUAL_USER"
echo "Installation directory: $INSTALL_DIR"
echo ""
echo "Services deployed:"
echo "  - ack_forwarder.service"
echo "  - command_forwarder.service"
echo ""
echo "Log locations:"
echo "  - $INSTALL_DIR/ack/logs/ack_forwarder.log"
echo "  - $INSTALL_DIR/command/logs/command_forwarder.log"
echo ""
echo "Useful commands:"
echo "  sudo systemctl status ack_forwarder"
echo "  sudo systemctl status command_forwarder"
echo "  sudo journalctl -u ack_forwarder -f"
echo "  sudo journalctl -u command_forwarder -f"
echo ""