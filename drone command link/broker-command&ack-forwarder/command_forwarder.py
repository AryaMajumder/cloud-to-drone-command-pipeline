#!/usr/bin/env python3
"""
Command Forwarder: Cloud → Agent (Direct IoT Core Subscription)
Subscribes to AWS IoT Core and forwards commands to Mosquitto
"""

import json
import time
import logging
from forwarder_lib import ForwarderBase, ForwarderConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class CommandForwarder(ForwarderBase):
    def __init__(self, config):
        super().__init__("command", config)
        
    def on_mqtt_connect(self, client, userdata, flags, rc):
        """Callback when connected to Mosquitto"""
        if rc == 0:
            self.logger.info("✓ Connected to Mosquitto")
        else:
            self.logger.error(f"Mosquitto connection failed: {rc}")
    
    def on_iot_command(self, topic, payload):
        """Callback when command received from IoT Core"""
        try:
            # Decode payload
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            
            data = json.loads(payload)
            cmd_id = data.get('cmd_id', 'unknown')
            cmd = data.get('cmd', 'unknown')
            drone_id = data.get('drone_id', 'unknown')
            
            self.logger.info(f"Command from IoT: cmd_id={cmd_id}, cmd={cmd}, drone_id={drone_id}")
            
            # Validate command
            if not self._validate_command(data):
                self.logger.error(f"Invalid command: {cmd_id}")
                self.publish_metric('CommandValidationFailed', 1)
                return
            
            # Forward to Mosquitto
            mosquitto_topic = f"drone/{drone_id}/cmd"
            result = self.mqtt_client.publish(mosquitto_topic, json.dumps(data), qos=1)
            
            if result.rc == 0:  # MQTT_ERR_SUCCESS
                self.publish_metric('CommandForwarded', 1)
                self.logger.info(f"✓ Forwarded to agent: {cmd_id}")
            else:
                self.publish_metric('CommandForwardFailed', 1)
                self.logger.error(f"✗ Mosquitto publish failed: {result.rc}")
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in command: {e}")
            self.publish_metric('CommandForwardFailed', 1)
        except Exception as e:
            self.logger.error(f"Error processing command: {e}")
            self.publish_metric('CommandForwardFailed', 1)
    
    def _validate_command(self, cmd_data):
        """Validate command structure"""
        required_fields = ['cmd_id', 'cmd', 'drone_id']
        
        for field in required_fields:
            if field not in cmd_data:
                self.logger.error(f"Missing required field: {field}")
                return False
        
        # Check if command is for this drone
        if cmd_data['drone_id'] != self.config.drone_id:
            self.logger.warning(f"Command for different drone: {cmd_data['drone_id']}")
            return False
        
        return True
    
    def run(self):
        self.logger.info("=" * 60)
        self.logger.info("Command Forwarder starting (IoT Core subscription)...")
        self.logger.info("=" * 60)
        
        try:
            # Connect to AWS IoT Core first
            self.logger.info("Connecting to AWS IoT Core...")
            self.connect_iot_sigv4()
            
            # Subscribe to command topic
            cmd_topic = f"drone/{self.config.drone_id}/cmd"
            self.subscribe_to_iot(cmd_topic, self.on_iot_command, qos=1)
            self.logger.info(f"✓ Subscribed to IoT: {cmd_topic}")
            
            # Connect to Mosquitto
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
            
            # Keep running
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
        'aws_access_key_id': 'YOUR_ACCESS_KEY',
        'aws_secret_access_key': 'YOUR_SECRET_KEY',
        'aws_region': 'us-east-1',
        'iot_endpoint': 'YOUR_ENDPOINT.iot.us-east-1.amazonaws.com',
        'mqtt_broker': 'localhost',
        'mqtt_port': 1883,
        'cloudwatch_namespace': 'DroneC2/Command',
        'cloudwatch_enabled': True,
        'drone_id': 'drone_001'
    })
    
    forwarder = CommandForwarder(config)
    exit(forwarder.run())