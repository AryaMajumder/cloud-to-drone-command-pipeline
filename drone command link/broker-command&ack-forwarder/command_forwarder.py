#!/usr/bin/env python3
"""
Command Forwarder: Cloud → Agent (via SQS)
Polls SQS for commands from IoT Core and forwards to Mosquitto
"""

import json
import time
import logging
import paho.mqtt.client as mqtt
from forwarder_lib import ForwarderBase, ForwarderConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class CommandForwarder(ForwarderBase):
    def __init__(self, config):
        super().__init__("command", config)
        self.sqs_queue_url = config.sqs_queue_url
        self.sqs_client = self.session.client('sqs')
        
    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("✓ Connected to Mosquitto")
        else:
            self.logger.error(f"Mosquitto connection failed: {rc}")
    
    def poll_commands(self):
        """Poll SQS for commands from IoT Core"""
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
                    
                    self.logger.info(f"Command from SQS: cmd_id={cmd_id}, cmd={cmd}, drone_id={drone_id}")
                    
                    # Publish to Mosquitto
                    topic = f"drone/{drone_id}/cmd"
                    result = self.mqtt_client.publish(topic, json.dumps(body), qos=1)
                    
                    if result.rc == mqtt.MQTT_ERR_SUCCESS:
                        self.publish_metric('CommandForwarded', 1)
                        self.logger.info(f"✓ Forwarded to agent: {cmd_id}")
                        
                        # Delete message from SQS (only after successful forward)
                        self.sqs_client.delete_message(
                            QueueUrl=self.sqs_queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        self.logger.debug(f"✓ Deleted from SQS: {cmd_id}")
                    else:
                        self.publish_metric('CommandForwardFailed', 1)
                        self.logger.error(f"✗ Mosquitto publish failed: {result.rc}")
                        # Message will become visible again after VisibilityTimeout
                        
                except json.JSONDecodeError as e:
                    self.logger.error(f"Invalid JSON in SQS message: {e}")
                    # Delete malformed message
                    self.sqs_client.delete_message(
                        QueueUrl=self.sqs_queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    # Leave in queue for retry
                    
        except Exception as e:
            self.logger.error(f"SQS poll error: {e}")
            time.sleep(5)  # Back off on error
    
    def run(self):
        self.logger.info("=" * 60)
        self.logger.info("Command Forwarder starting...")
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
            self.logger.info(f"  Polling: {self.sqs_queue_url}")
            self.logger.info(f"  Publishing to: Mosquitto (localhost)")
            self.logger.info("=" * 60)
            
            # Main polling loop
            while True:
                self.poll_commands()
                
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
            return 0
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
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
        'drone_id': 'drone_001',
        'sqs_queue_url': 'https://sqs.us-east-1.amazonaws.com/123456789012/drone-commands-queue'
    })
    
    forwarder = CommandForwarder(config)
    exit(forwarder.run())