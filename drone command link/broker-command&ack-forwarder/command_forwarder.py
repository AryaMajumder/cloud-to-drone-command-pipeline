#!/usr/bin/env python3
"""
Command Forwarder: Cloud → Agent (via IoT Core)
Subscribes to IoT Core and forwards commands to local Mosquitto
"""

import json
import time
import logging
import paho.mqtt.client as mqtt
import boto3
from awscrt import io, mqtt as iot_mqtt, auth, http
from awsiot import mqtt_connection_builder

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class CommandForwarder:
    def __init__(self, config):
        self.logger = logging.getLogger("command_forwarder")
        self.config = config
        
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
            from datetime import datetime
            self.cloudwatch.put_metric_data(
                Namespace=self.config.get('cloudwatch_namespace', 'DroneC2/Command'),
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
            self.logger.info("✓ Connected to local Mosquitto")
        else:
            self.mosquitto_connected = False
            self.logger.error(f"Mosquitto connection failed: {rc}")
    
    def on_mosquitto_disconnect(self, client, userdata, rc):
        """Mosquitto disconnection callback"""
        self.mosquitto_connected = False
        if rc != 0:
            self.logger.warning(f"Unexpected Mosquitto disconnect: {rc}")
    
    def on_iot_command(self, topic, payload, **kwargs):
        """
        Callback when command received from IoT Core
        
        Args:
            topic: MQTT topic (e.g., 'drone/drone-01/cmd')
            payload: Command payload (bytes)
        """
        try:
            # Decode payload
            payload_str = payload.decode('utf-8')
            data = json.loads(payload_str)
            
            # Extract drone ID from topic
            # Topic format: drone/{drone_id}/cmd
            parts = topic.split('/')
            if len(parts) >= 3:
                drone_id = parts[1]
            else:
                drone_id = data.get('target_id', 'unknown')
            
            # Log command received
            action = data.get('action', 'unknown')
            self.logger.info(f"Command from IoT: drone_id={drone_id}, action={action}")
            
            # Forward to local Mosquitto
            if self.mosquitto_connected:
                result = self.mosquitto_client.publish(
                    topic,
                    payload_str,
                    qos=1
                )
                
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    self.publish_metric('CommandForwarded', 1)
                    self.logger.info(f"✓ Forwarded to agent: {topic}")
                else:
                    self.publish_metric('CommandForwardFailed', 1)
                    self.logger.error(f"✗ Mosquitto publish failed: {result.rc}")
            else:
                self.logger.error("✗ Mosquitto not connected, dropping command")
                self.publish_metric('CommandDropped', 1)
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON from IoT: {e}")
            self.publish_metric('InvalidCommand', 1)
        except Exception as e:
            self.logger.error(f"Error processing command: {e}")
            self.publish_metric('CommandForwardFailed', 1)
    
    def connect_mosquitto(self):
        """Connect to local Mosquitto broker"""
        self.logger.info(f"Connecting to Mosquitto at {self.config['mqtt_broker']}:{self.config['mqtt_port']}")
        
        client = mqtt.Client(client_id="command_forwarder")
        client.on_connect = self.on_mosquitto_connect
        client.on_disconnect = self.on_mosquitto_disconnect
        
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
            client_id="command_forwarder",
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
        
        # Re-subscribe to command topics
        self.subscribe_to_commands()
    
    def subscribe_to_commands(self):
        """Subscribe to command topics on IoT Core"""
        # Subscribe to wildcard topic for all drones
        topic = "drone/+/cmd"
        
        self.logger.info(f"Subscribing to IoT topic: {topic}")
        
        subscribe_future, _ = self.iot_connection.subscribe(
            topic=topic,
            qos=iot_mqtt.QoS.AT_LEAST_ONCE,
            callback=self.on_iot_command
        )
        
        # Wait for subscription to complete
        subscribe_future.result()
        self.logger.info(f"✓ Subscribed to {topic}")
    
    def run(self):
        """Main run loop"""
        self.logger.info("=" * 60)
        self.logger.info("Command Forwarder starting...")
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
            
            # Subscribe to command topics
            self.subscribe_to_commands()
            
            self.logger.info("=" * 60)
            self.logger.info("✓ Command Forwarder running")
            self.logger.info(f"  IoT Core: {self.config['iot_endpoint']}")
            self.logger.info(f"  Subscribed: drone/+/cmd")
            self.logger.info(f"  Forwarding to: Mosquitto (localhost)")
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


if __name__ == "__main__":
    # Configuration
    config = {
        'aws_access_key_id': 'YOUR_ACCESS_KEY',
        'aws_secret_access_key': 'YOUR_SECRET_KEY',
        'aws_region': 'us-east-1',
        'iot_endpoint': 'xxxxx-ats.iot.us-east-1.amazonaws.com',
        'mqtt_broker': 'localhost',
        'mqtt_port': 1883,
        'mqtt_user': None,  # Optional
        'mqtt_pass': None,  # Optional
        'cloudwatch_enabled': True,
        'cloudwatch_namespace': 'DroneC2/Command'
    }
    
    forwarder = CommandForwarder(config)
    exit(forwarder.run())
