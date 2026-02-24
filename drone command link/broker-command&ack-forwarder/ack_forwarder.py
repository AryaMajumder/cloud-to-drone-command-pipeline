#!/usr/bin/env python3
"""
ACK Forwarder: Agent → Cloud (via IoT Core)
Subscribes to local Mosquitto and forwards ACKs to IoT Core
"""

import json
import time
import logging
import os
from datetime import datetime
import paho.mqtt.client as mqtt
import boto3
from awscrt import io, mqtt as iot_mqtt, auth
from awsiot import mqtt_connection_builder

# Logging setup
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'ack_forwarder.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("ack_forwarder")

class AckForwarder:
    def __init__(self, config):
        self.config = config
        self.logger = logger
        
        # MQTT clients
        self.mosquitto_client = None
        self.iot_connection = None
        
        # Connection state
        self.mosquitto_connected = False
        self.iot_connected = False
        
        # Metrics
        self.cloudwatch = None
        if config.get('cloudwatch_enabled'):
            self.cloudwatch = boto3.client(
                'cloudwatch',
                aws_access_key_id=config['aws_access_key_id'],
                aws_secret_access_key=config['aws_secret_access_key'],
                region_name=config['aws_region']
            )
    
    def publish_metric(self, metric_name, value):
        """Publish metric to CloudWatch"""
        if not self.cloudwatch:
            return
        
        try:
            self.cloudwatch.put_metric_data(
                Namespace=self.config.get('cloudwatch_namespace', 'DroneC2/ACK'),
                MetricData=[{
                    'MetricName': metric_name,
                    'Value': value,
                    'Unit': 'Count',
                    'Timestamp': datetime.utcnow()
                }]
            )
        except Exception as e:
            self.logger.error(f"CloudWatch publish failed: {e}")
    
    def on_mosquitto_connect(self, client, userdata, flags, rc):
        """Mosquitto connection callback"""
        if rc == 0:
            self.mosquitto_connected = True
            topic = f"drone/{self.config['drone_id']}/cmd_ack"
            client.subscribe(topic, qos=1)
            self.logger.info(f"✓ Connected to Mosquitto")
            self.logger.info(f"✓ Subscribed to {topic}")
        else:
            self.mosquitto_connected = False
            self.logger.error(f"Mosquitto connection failed: {rc}")
    
    def on_mosquitto_disconnect(self, client, userdata, rc):
        """Mosquitto disconnection callback"""
        self.mosquitto_connected = False
        if rc != 0:
            self.logger.warning(f"Unexpected Mosquitto disconnect: {rc}")
    
    def on_mosquitto_message(self, client, userdata, msg):
        """
        Callback when ACK received from local Mosquitto
        
        Args:
            msg: MQTT message from agent
        """
        try:
            topic = msg.topic
            payload_str = msg.payload.decode('utf-8')
            data = json.loads(payload_str)
            
            # Extract ACK details
            cmd_id = data.get('cmd_id', 'unknown')
            status = data.get('status', 'unknown')
            
            self.logger.info(f"ACK: cmd_id={cmd_id}, status={status}")
            
            # Forward to IoT Core
            if self.iot_connected:
                future = self.iot_connection.publish(
                    topic=topic,
                    payload=payload_str,
                    qos=iot_mqtt.QoS.AT_LEAST_ONCE
                )
                future.result()  # Wait for publish to complete
                
                self.publish_metric('AckForwarded', 1)
                self.logger.info(f"✓ ACK forwarded: {cmd_id}")
            else:
                self.logger.error("✗ IoT not connected, dropping ACK")
                self.publish_metric('AckDropped', 1)
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON from agent: {e}")
            self.publish_metric('InvalidAck', 1)
        except Exception as e:
            self.logger.error(f"Error forwarding ACK: {e}")
            self.publish_metric('AckForwardFailed', 1)
    
    def connect_mosquitto(self):
        """Connect to local Mosquitto broker"""
        self.logger.info(f"Connecting to Mosquitto at {self.config['mqtt_broker']}:{self.config['mqtt_port']}")
        
        client = mqtt.Client(client_id="ack_forwarder")
        client.on_connect = self.on_mosquitto_connect
        client.on_disconnect = self.on_mosquitto_disconnect
        client.on_message = self.on_mosquitto_message
        
        # Set credentials if provided
        mqtt_user = self.config.get('mqtt_user')
        mqtt_pass = self.config.get('mqtt_pass')
        if mqtt_user and mqtt_pass:
            client.username_pw_set(mqtt_user, mqtt_pass)
        
        # Connect
        client.connect(
            self.config['mqtt_broker'],
            self.config['mqtt_port'],
            keepalive=60
        )
        
        # Start background loop
        client.loop_start()
        
        self.mosquitto_client = client
        
        # Wait for connection
        for i in range(10):
            if self.mosquitto_connected:
                return True
            time.sleep(0.5)
        
        return False
    
    def connect_iot(self):
        """Connect to AWS IoT Core using AWS credentials (SigV4)"""
        self.logger.info(f"Connecting to AWS IoT Core at {self.config['iot_endpoint']}")
        
        # Create event loop
        event_loop_group = io.EventLoopGroup(1)
        host_resolver = io.DefaultHostResolver(event_loop_group)
        client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
        
        # Create credentials provider from access key
        credentials_provider = auth.AwsCredentialsProvider.new_static(
            access_key_id=self.config['aws_access_key_id'],
            secret_access_key=self.config['aws_secret_access_key']
        )
        
        # Build connection with SigV4 auth
        self.iot_connection = mqtt_connection_builder.websockets_with_default_aws_signing(
            endpoint=self.config['iot_endpoint'],
            client_bootstrap=client_bootstrap,
            region=self.config['aws_region'],
            credentials_provider=credentials_provider,
            client_id="ack_forwarder",
            clean_session=True,
            keep_alive_secs=30,
            on_connection_interrupted=self.on_iot_interrupted,
            on_connection_resumed=self.on_iot_resumed
        )
        
        # Connect
        connect_future = self.iot_connection.connect()
        connect_future.result()  # Wait for connection
        
        self.iot_connected = True
        self.logger.info("✓ Connected to AWS IoT Core")
        
        return True
    
    def on_iot_interrupted(self, connection, error, **kwargs):
        """IoT connection interrupted"""
        self.iot_connected = False
        self.logger.warning(f"IoT connection interrupted: {error}")
    
    def on_iot_resumed(self, connection, return_code, session_present, **kwargs):
        """IoT connection resumed"""
        self.iot_connected = True
        self.logger.info(f"IoT connection resumed: {return_code}")
    
    def run(self):
        """Main run loop"""
        self.logger.info("=" * 60)
        self.logger.info("ACK Forwarder starting...")
        self.logger.info("=" * 60)
        
        try:
            # Connect to Mosquitto
            if not self.connect_mosquitto():
                self.logger.error("Failed to connect to Mosquitto")
                return 1
            
            # Connect to IoT Core
            if not self.connect_iot():
                self.logger.error("Failed to connect to IoT Core")
                return 1
            
            self.logger.info("=" * 60)
            self.logger.info("✓ ACK Forwarder running")
            self.logger.info(f"  Mosquitto: {self.config['mqtt_broker']}:{self.config['mqtt_port']}")
            self.logger.info(f"  Subscribed: drone/{self.config['drone_id']}/cmd_ack")
            self.logger.info(f"  IoT Core: {self.config['iot_endpoint']}")
            self.logger.info("=" * 60)
            
            # Keep running
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
            return 0
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
            return 1
        finally:
            # Cleanup
            if self.mosquitto_client:
                self.mosquitto_client.loop_stop()
                self.mosquitto_client.disconnect()
            if self.iot_connection:
                disconnect_future = self.iot_connection.disconnect()
                disconnect_future.result()
