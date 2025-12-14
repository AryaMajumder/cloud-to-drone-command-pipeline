#!/usr/bin/env python3
"""ACK Forwarder: Agent → Cloud"""

import json
import time
import logging
from forwarder_lib import ForwarderBase, ForwarderConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class AckForwarder(ForwarderBase):
    def __init__(self, config):
        super().__init__("ack", config)
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("✓ Connected to Mosquitto")
            topic = f"drone/{self.config.drone_id}/cmd_ack"
            client.subscribe(topic, qos=1)
            self.logger.info(f"✓ Subscribed to {topic}")
        else:
            self.logger.error(f"Connection failed: {rc}")
    
    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode('utf-8')
        try:
            data = json.loads(payload)
            cmd_id = data.get('cmd_id', 'unknown')
            status = data.get('status', 'unknown')
            self.logger.info(f"ACK: cmd_id={cmd_id}, status={status}")
            
            if self.publish_to_iot(topic, payload, qos=1):
                self.publish_metric('AckForwarded', 1)
                self.logger.info(f"✓ ACK forwarded: {cmd_id}")
            else:
                self.publish_metric('AckForwardFailed', 1)
        except Exception as e:
            self.logger.error(f"Error: {e}")
            self.publish_metric('AckForwardFailed', 1)
    
    def run(self):
        self.logger.info("ACK Forwarder starting...")
        try:
            self.logger.info(f"✓ IoT configured for {self.config.iot_endpoint}")
            self.mqtt_client = self.connect_mosquitto(self.on_connect, self.on_message)
            time.sleep(2)
            if not self.mqtt_connected:
                return 1
            self.logger.info("✓ ACK Forwarder running")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            return 0

if __name__ == "__main__":
    config = ForwarderConfig({
        'aws_access_key_id': 'YOUR_ACCESS_KEY',
        'aws_secret_access_key': 'YOUR_SECRET_KEY',
        'aws_region': 'us-east-1',
        'iot_endpoint': 'YOUR_ENDPOINT.iot.us-east-1.amazonaws.com',
        'cloudwatch_namespace': 'DroneC2/ACK',
        'drone_id': 'drone_001'
    })
    forwarder = AckForwarder(config)
    exit(forwarder.run())