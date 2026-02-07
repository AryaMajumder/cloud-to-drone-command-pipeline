#!/usr/bin/env python3
"""
Command Forwarder: Cloud → Agent
Polls SQS and forwards commands to local Mosquitto

FIXED VERSION for GitHub
Path: drone command link/broker-command&ack-forwarder/Command-forwarder.py

Key fixes:
- Uses CommandConfig (not ForwarderConfig) to avoid iot_endpoint requirement
- Reads sqs_queue_url from config or environment
- Resilient to config structure changes
- Works standalone or with ForwarderBase
"""

import json
import time
import logging
import sys
import os
import signal

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(__file__))

import paho.mqtt.client as mqtt
import boto3

# Try to import ForwarderBase from forwarder_lib
try:
    from forwarder_lib import ForwarderBase
    USE_FORWARDER_BASE = True
except ImportError:
    USE_FORWARDER_BASE = False
    print("Warning: forwarder_lib not available, running in standalone mode")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.expanduser('~/forwarders/command/logs/command_forwarder.log')),
        logging.StreamHandler(sys.stdout)
    ]
)

class CommandConfig:
    """
    Configuration class for Command Forwarder
    
    Does NOT require iot_endpoint (only needed for ACK forwarder)
    Reads from config dict or falls back to environment variables
    """
    def __init__(self, config_dict):
        self.aws_region = config_dict.get('aws_region', 'us-east-1')
        self.mqtt_broker = config_dict.get('mqtt_broker', 'localhost')
        self.mqtt_port = config_dict.get('mqtt_port', 1883)
        self.drone_id = config_dict.get('drone_id', 'drone_001')
        self.cloudwatch_enabled = config_dict.get('cloudwatch_enabled', True)
        self.cloudwatch_namespace = config_dict.get('cloudwatch_namespace', 'DroneC2/Command')
        
        # Get SQS queue URL from config or environment
        self.sqs_queue_url = config_dict.get('sqs_queue_url')
        if not self.sqs_queue_url:
            self.sqs_queue_url = (
                os.environ.get('sqs_queue_url') or 
                os.environ.get('SQS_QUEUE_URL') or
                os.environ.get('queue_url') or
                os.environ.get('QUEUE_URL')
            )
        
        # Validate required config
        if not self.sqs_queue_url:
            raise ValueError(
                "sqs_queue_url is required. Set it in config dict or as "
                "sqs_queue_url/SQS_QUEUE_URL environment variable"
            )

if USE_FORWARDER_BASE:
    class CommandForwarder(ForwarderBase):
        """
        Command Forwarder with ForwarderBase
        
        Inherits MQTT connection handling and base functionality
        from ForwarderBase in forwarder_lib.py
        """
        def __init__(self, config):
            """
            Initialize command forwarder
            
            Args:
                config: CommandConfig instance with forwarder settings
            """
            super().__init__("command", config)
            
            # SQS queue URL already validated in CommandConfig
            self.sqs_queue_url = config.sqs_queue_url
            
            # Initialize AWS SQS client
            self.sqs = boto3.client('sqs', region_name=self.config.aws_region)
            self.running = True

            # Setup signal handlers for graceful shutdown
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)

        def signal_handler(self, sig, frame):
            """Handle SIGINT and SIGTERM for graceful shutdown"""
            self.logger.info("Shutdown signal received")
            self.running = False
            if self.mqtt_client:
                self.mqtt_client.disconnect()
            sys.exit(0)

        def on_mqtt_connect(self, client, userdata, flags, rc):
            """Callback when MQTT connection is established"""
            if rc == 0:
                self.logger.info("✓ Connected to Mosquitto")
            else:
                self.logger.error(f"Mosquitto connection failed with code: {rc}")

        def poll_sqs(self):
            """
            Poll SQS for commands and forward to MQTT
            
            Uses long polling (20 seconds) to reduce API calls and latency
            Deletes messages from SQS only after successful MQTT forward
            """
            try:
                response = self.sqs.receive_message(
                    QueueUrl=self.sqs_queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20,  # Long polling to reduce API calls
                    AttributeNames=['All'],
                    MessageAttributeNames=['All']
                )

                # Check if any messages were received
                if 'Messages' not in response:
                    return  # No messages available

                for message in response['Messages']:
                    try:
                        # Parse command JSON
                        body = json.loads(message['Body'])
                        cmd_id = body.get('cmd_id', 'unknown')
                        self.logger.info(f"📥 Received command from SQS: {cmd_id}")

                        # Extract drone_id (may differ from forwarder's configured drone_id)
                        drone_id = body.get('drone_id', self.config.drone_id)
                        topic = f"drone/{drone_id}/cmd"

                        # Forward command to MQTT
                        result = self.mqtt_client.publish(topic, json.dumps(body), qos=1)
                        
                        if result.rc == 0:
                            self.logger.info(f"✓ Forwarded to MQTT topic: {topic}")
                            
                            # Delete from SQS only after successful forward
                            self.sqs.delete_message(
                                QueueUrl=self.sqs_queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                            self.logger.info(f"✓ Deleted from SQS: {cmd_id}")
                        else:
                            self.logger.error(f"✗ Failed to publish to MQTT (rc={result.rc})")

                    except json.JSONDecodeError as e:
                        self.logger.error(f"Invalid JSON in SQS message: {e}")
                    except KeyError as e:
                        self.logger.error(f"Missing required field in message: {e}")
                    except Exception as e:
                        self.logger.error(f"Error processing message: {e}", exc_info=True)

            except Exception as e:
                self.logger.error(f"Error polling SQS: {e}", exc_info=True)
                time.sleep(5)  # Back off before retrying

        def run(self):
            """
            Main run loop
            
            Connects to MQTT broker and continuously polls SQS for commands
            Returns exit code (0 = success, 1 = failure)
            """
            self.logger.info("=" * 60)
            self.logger.info("Command Forwarder starting...")
            self.logger.info(f"  SQS Queue: {self.sqs_queue_url}")
            self.logger.info(f"  AWS Region: {self.config.aws_region}")
            self.logger.info(f"  MQTT Broker: {self.config.mqtt_broker}:{self.config.mqtt_port}")
            self.logger.info(f"  Drone ID: {self.config.drone_id}")
            self.logger.info("=" * 60)

            try:
                # Connect to Mosquitto MQTT broker
                self.mqtt_client = self.connect_mosquitto(
                    self.on_mqtt_connect,
                    None  # No message handler (we only publish, not subscribe)
                )

                # Wait for MQTT connection to establish
                time.sleep(2)

                if not self.mqtt_connected:
                    self.logger.error("Failed to connect to Mosquitto")
                    return 1

                self.logger.info("=" * 60)
                self.logger.info("✓ Command Forwarder running")
                self.logger.info("  Polling SQS every 20 seconds...")
                self.logger.info("=" * 60)

                # Main polling loop
                heartbeat_counter = 0
                while self.running:
                    self.poll_sqs()
                    
                    # Heartbeat log every ~60 seconds (3 polls × 20s = 60s)
                    heartbeat_counter += 1
                    if heartbeat_counter >= 3:
                        self.logger.info("💓 Command Forwarder running...")
                        heartbeat_counter = 0

            except KeyboardInterrupt:
                self.logger.info("Keyboard interrupt received, shutting down...")
                return 0
            except Exception as e:
                self.logger.error(f"Fatal error in main loop: {e}", exc_info=True)
                return 1
            finally:
                # Cleanup
                if self.mqtt_client:
                    self.mqtt_client.disconnect()
                    self.logger.info("Disconnected from MQTT")

else:
    # Standalone implementation without ForwarderBase
    # Used when forwarder_lib.py is not available
    class CommandForwarder:
        """
        Standalone Command Forwarder
        
        Implements basic functionality without ForwarderBase dependency
        Useful for testing or when forwarder_lib is unavailable
        """
        def __init__(self, config):
            self.config = config
            self.logger = logging.getLogger("command")
            self.sqs_queue_url = config.sqs_queue_url
            self.sqs = boto3.client('sqs', region_name=config.aws_region)
            self.running = True
            self.mqtt_client = None
            self.mqtt_connected = False
            
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)
            
        def signal_handler(self, sig, frame):
            self.logger.info("Shutdown signal received")
            self.running = False
            if self.mqtt_client:
                self.mqtt_client.disconnect()
            sys.exit(0)
            
        def on_connect(self, client, userdata, flags, rc):
            if rc == 0:
                self.mqtt_connected = True
                self.logger.info("✓ Connected to Mosquitto")
            else:
                self.logger.error(f"Connection failed: {rc}")
                
        def connect_mqtt(self):
            """Connect to MQTT broker"""
            client = mqtt.Client()
            client.on_connect = self.on_connect
            client.connect(self.config.mqtt_broker, self.config.mqtt_port, 60)
            client.loop_start()
            return client
            
        def poll_sqs(self):
            """Poll SQS and forward commands"""
            try:
                response = self.sqs.receive_message(
                    QueueUrl=self.sqs_queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20
                )
                
                if 'Messages' in response:
                    for message in response['Messages']:
                        body = json.loads(message['Body'])
                        cmd_id = body.get('cmd_id', 'unknown')
                        drone_id = body.get('drone_id', self.config.drone_id)
                        topic = f"drone/{drone_id}/cmd"
                        
                        result = self.mqtt_client.publish(topic, json.dumps(body), qos=1)
                        if result.rc == 0:
                            self.logger.info(f"✓ Forwarded: {cmd_id} to {topic}")
                            
                            self.sqs.delete_message(
                                QueueUrl=self.sqs_queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                        else:
                            self.logger.error(f"✗ MQTT publish failed: {result.rc}")
                            
            except Exception as e:
                self.logger.error(f"Error: {e}", exc_info=True)
                time.sleep(5)
                
        def run(self):
            """Main run loop"""
            self.logger.info("=" * 60)
            self.logger.info("Command Forwarder starting (standalone mode)...")
            self.logger.info(f"  SQS Queue: {self.sqs_queue_url}")
            self.logger.info("=" * 60)
            
            try:
                self.mqtt_client = self.connect_mqtt()
                time.sleep(2)
                
                if not self.mqtt_connected:
                    self.logger.error("Failed to connect to MQTT")
                    return 1
                    
                self.logger.info("✓ Command Forwarder running")
                
                heartbeat_counter = 0
                while self.running:
                    self.poll_sqs()
                    
                    heartbeat_counter += 1
                    if heartbeat_counter >= 3:
                        self.logger.info("💓 Command Forwarder running...")
                        heartbeat_counter = 0
                    
                return 0
                
            except Exception as e:
                self.logger.error(f"Fatal error: {e}", exc_info=True)
                return 1
            finally:
                if self.mqtt_client:
                    self.mqtt_client.disconnect()

if __name__ == "__main__":
    """
    Entry point when run as script
    
    Reads configuration from environment variables:
    - aws_region / AWS_REGION
    - sqs_queue_url / SQS_QUEUE_URL (required)
    - mqtt_broker / MQTT_BROKER
    - mqtt_port / MQTT_PORT
    - drone_id / DRONE_ID
    """
    
    # Create config from environment variables
    # Checks both lowercase and UPPERCASE for compatibility
    config = CommandConfig({
        'aws_region': os.environ.get('aws_region') or os.environ.get('AWS_REGION', 'us-east-1'),
        'sqs_queue_url': os.environ.get('sqs_queue_url') or os.environ.get('SQS_QUEUE_URL'),
        'mqtt_broker': os.environ.get('mqtt_broker') or os.environ.get('MQTT_BROKER', 'localhost'),
        'mqtt_port': int(os.environ.get('mqtt_port') or os.environ.get('MQTT_PORT', 1883)),
        'drone_id': os.environ.get('drone_id') or os.environ.get('DRONE_ID', 'drone_001'),
        'cloudwatch_enabled': os.environ.get('cloudwatch_enabled', 'true').lower() == 'true',
        'cloudwatch_namespace': 'DroneC2/Command'
    })

    # Create and run forwarder
    forwarder = CommandForwarder(config)
    exit_code = forwarder.run()
    sys.exit(exit_code)
