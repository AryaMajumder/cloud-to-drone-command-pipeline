#!/usr/bin/env python3
"""
Command Forwarder: Cloud → Agent (via SQS)
Polls SQS for commands and forwards to local Mosquitto broker

FIXED VERSION - Standalone (no ForwarderBase inheritance)
This version does NOT require IoT Core credentials/certificates
Only needs: SQS queue URL, MQTT broker, AWS region
"""

import json
import time
import logging
import sys
import os
import signal
import paho.mqtt.client as mqtt
import boto3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class CommandForwarder:
    """
    Standalone Command Forwarder
    Polls SQS for commands and publishes to local MQTT broker
    """
    
    def __init__(self, sqs_queue_url, mqtt_broker='localhost', mqtt_port=1883,
                 aws_region='us-east-1', drone_id='drone_001', 
                 mqtt_user=None, mqtt_pass=None):
        """
        Initialize command forwarder
        
        Args:
            sqs_queue_url: SQS queue URL to poll for commands
            mqtt_broker: Local MQTT broker hostname (default: localhost)
            mqtt_port: Local MQTT broker port (default: 1883)
            aws_region: AWS region (default: us-east-1)
            drone_id: Drone identifier for MQTT topics (default: drone_001)
            mqtt_user: Optional MQTT username
            mqtt_pass: Optional MQTT password
        """
        self.sqs_queue_url = sqs_queue_url
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.aws_region = aws_region
        self.drone_id = drone_id
        self.mqtt_user = mqtt_user
        self.mqtt_pass = mqtt_pass
        
        # Setup logging
        self.logger = logging.getLogger("command_forwarder")
        
        # Initialize AWS SQS client
        self.sqs = boto3.client('sqs', region_name=self.aws_region)
        
        # MQTT client (will be initialized in run())
        self.mqtt_client = None
        self.mqtt_connected = False
        
        # Running flag for graceful shutdown
        self.running = True
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        """Handle shutdown signals"""
        self.logger.info("Shutdown signal received")
        self.running = False
        if self.mqtt_client:
            self.mqtt_client.disconnect()
        sys.exit(0)
    
    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """Callback when MQTT connection is established"""
        if rc == 0:
            self.mqtt_connected = True
            self.logger.info("✓ Connected to Mosquitto broker")
        else:
            self.mqtt_connected = False
            self.logger.error(f"✗ Mosquitto connection failed with code: {rc}")
    
    def _on_mqtt_disconnect(self, client, userdata, rc):
        """Callback when MQTT connection is lost"""
        self.mqtt_connected = False
        if rc != 0:
            self.logger.warning(f"Unexpected MQTT disconnection (rc={rc})")
    
    def _connect_mqtt(self):
        """Connect to local MQTT broker"""
        self.mqtt_client = mqtt.Client()
        
        # Set callbacks
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
        
        # Set credentials if provided
        if self.mqtt_user and self.mqtt_pass:
            self.mqtt_client.username_pw_set(self.mqtt_user, self.mqtt_pass)
        
        try:
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, keepalive=60)
            self.mqtt_client.loop_start()
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to MQTT broker: {e}")
            return False
    
    def _poll_sqs(self):
        """
        Poll SQS for commands and forward to MQTT
        Uses long polling (20 seconds) to reduce API calls
        """
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.sqs_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,  # Long polling
                VisibilityTimeout=30,
                AttributeNames=['All'],
                MessageAttributeNames=['All']
            )
            
            # Check if any messages received
            if 'Messages' not in response:
                return
            
            for message in response['Messages']:
                try:
                    # Parse command from message body
                    body = json.loads(message['Body'])
                    
                    cmd_id = body.get('cmd_id', 'unknown')
                    cmd = body.get('cmd', 'unknown')
                    drone_id = body.get('drone_id', self.drone_id)
                    params = body.get('params', {})
                    
                    self.logger.info(f"📥 Received command: cmd_id={cmd_id}, cmd={cmd}, drone={drone_id}")
                    
                    # Publish to local MQTT broker
                    topic = f"drone/{drone_id}/cmd"
                    payload = json.dumps(body)
                    
                    result = self.mqtt_client.publish(topic, payload, qos=1)
                    
                    if result.rc == mqtt.MQTT_ERR_SUCCESS:
                        self.logger.info(f"✓ Forwarded to MQTT: {topic}")
                        
                        # Delete message from SQS only after successful forward
                        self.sqs.delete_message(
                            QueueUrl=self.sqs_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        self.logger.debug(f"✓ Deleted from SQS: {cmd_id}")
                    else:
                        self.logger.error(f"✗ MQTT publish failed (rc={result.rc})")
                        # Message will become visible again after VisibilityTimeout
                        
                except json.JSONDecodeError as e:
                    self.logger.error(f"Invalid JSON in SQS message: {e}")
                    # Delete malformed message
                    self.sqs.delete_message(
                        QueueUrl=self.sqs_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                except KeyError as e:
                    self.logger.error(f"Missing required field in message: {e}")
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}", exc_info=True)
                    # Leave in queue for retry
                    
        except Exception as e:
            self.logger.error(f"Error polling SQS: {e}", exc_info=True)
            time.sleep(5)  # Back off on errors
    
    def run(self):
        """
        Main run loop
        Connects to MQTT broker and continuously polls SQS for commands
        """
        self.logger.info("=" * 60)
        self.logger.info("Command Forwarder starting...")
        self.logger.info(f"  SQS Queue: {self.sqs_queue_url}")
        self.logger.info(f"  AWS Region: {self.aws_region}")
        self.logger.info(f"  MQTT Broker: {self.mqtt_broker}:{self.mqtt_port}")
        self.logger.info(f"  Drone ID: {self.drone_id}")
        self.logger.info("=" * 60)
        
        try:
            # Connect to MQTT broker
            if not self._connect_mqtt():
                self.logger.error("Failed to connect to MQTT broker")
                return 1
            
            # Wait for MQTT connection to be established
            time.sleep(2)
            
            if not self.mqtt_connected:
                self.logger.error("MQTT connection not established")
                return 1
            
            self.logger.info("=" * 60)
            self.logger.info("✓ Command Forwarder running")
            self.logger.info("  Polling SQS every 20 seconds...")
            self.logger.info("=" * 60)
            
            # Main polling loop
            heartbeat_counter = 0
            while self.running:
                self._poll_sqs()
                
                # Heartbeat log every ~60 seconds (3 polls × 20s each)
                heartbeat_counter += 1
                if heartbeat_counter >= 3:
                    self.logger.info("💓 Command Forwarder running...")
                    heartbeat_counter = 0
            
            return 0
            
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received, shutting down...")
            return 0
        except Exception as e:
            self.logger.error(f"Fatal error in main loop: {e}", exc_info=True)
            return 1
        finally:
            # Cleanup
            if self.mqtt_client:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
                self.logger.info("Disconnected from MQTT broker")


def main():
    """
    Entry point when run as script
    Reads configuration from environment variables
    """
    # Required configuration
    sqs_queue_url = os.environ.get('SQS_QUEUE_URL') or os.environ.get('sqs_queue_url')
    
    if not sqs_queue_url:
        print("ERROR: SQS_QUEUE_URL environment variable is required", file=sys.stderr)
        print("Set it in your environment or forwarder.env file", file=sys.stderr)
        sys.exit(1)
    
    # Optional configuration with defaults
    mqtt_broker = os.environ.get('MQTT_BROKER', os.environ.get('mqtt_broker', 'localhost'))
    mqtt_port = int(os.environ.get('MQTT_PORT', os.environ.get('mqtt_port', 1883)))
    aws_region = os.environ.get('AWS_REGION', os.environ.get('aws_region', 'us-east-1'))
    drone_id = os.environ.get('DRONE_ID', os.environ.get('drone_id', 'drone_001'))
    mqtt_user = os.environ.get('MQTT_USER', os.environ.get('mqtt_user'))
    mqtt_pass = os.environ.get('MQTT_PASS', os.environ.get('mqtt_pass'))
    
    # Create and run forwarder
    forwarder = CommandForwarder(
        sqs_queue_url=sqs_queue_url,
        mqtt_broker=mqtt_broker,
        mqtt_port=mqtt_port,
        aws_region=aws_region,
        drone_id=drone_id,
        mqtt_user=mqtt_user,
        mqtt_pass=mqtt_pass
    )
    
    exit_code = forwarder.run()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
