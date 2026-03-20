#!/usr/bin/env python3
"""ACK Forwarder: Agent → Cloud (via IoT Core) — boto3 version"""

import json
import time
import logging
import os
from datetime import datetime
import paho.mqtt.client as mqtt
import boto3

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'ack_forwarder.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ack_forwarder")


class AckForwarder:
    def __init__(self, config):
        self.config = config
        self.logger = logger
        self.mosquitto_client = None
        self.iot_client = None
        self.mosquitto_connected = False
        self.iot_connected = False
        self.cloudwatch = None
        if config.get('cloudwatch_enabled'):
            try:
                self.cloudwatch = boto3.Session(
                    aws_access_key_id=config['aws_access_key_id'],
                    aws_secret_access_key=config['aws_secret_access_key'],
                    region_name=config['aws_region']
                ).client('cloudwatch')
            except Exception as e:
                self.logger.warning(f"CloudWatch init failed: {e}")

    def publish_metric(self, metric_name, value):
        if not self.cloudwatch:
            return
        try:
            self.cloudwatch.put_metric_data(
                Namespace=self.config.get('cloudwatch_namespace', 'DroneC2/ACK'),
                MetricData=[{'MetricName': metric_name, 'Value': value,
                             'Unit': 'Count', 'Timestamp': datetime.utcnow()}]
            )
        except Exception as e:
            self.logger.error(f"CloudWatch publish failed: {e}")

    def connect_iot_boto3(self):
        self.logger.info(f"Connecting to IoT Core at {self.config['iot_endpoint']} (boto3)")
        session = boto3.Session(
            aws_access_key_id=self.config['aws_access_key_id'],
            aws_secret_access_key=self.config['aws_secret_access_key'],
            region_name=self.config['aws_region']
        )
        self.iot_client = session.client(
            'iot-data',
            endpoint_url=f"https://{self.config['iot_endpoint']}"
        )
        self.iot_connected = True
        self.logger.info("✓ Connected to IoT Core")
        return True

    def on_mosquitto_connect(self, client, userdata, flags, rc):
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
        self.mosquitto_connected = False
        if rc != 0:
            self.logger.warning(f"Unexpected Mosquitto disconnect: {rc}")

    def on_mosquitto_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload_str = msg.payload.decode('utf-8')
            data = json.loads(payload_str)
            cmd_id = data.get('cmd_id', 'unknown')
            status = data.get('status', 'unknown')
            self.logger.info(f"ACK: cmd_id={cmd_id}, status={status}")
            if self.iot_connected and self.iot_client:
                self.iot_client.publish(topic=topic, qos=1, payload=payload_str)
                self.publish_metric('AckForwarded', 1)
                self.logger.info(f"✓ ACK forwarded: {cmd_id}")
            else:
                self.logger.error("✗ IoT not connected, dropping ACK")
                self.publish_metric('AckDropped', 1)
        except Exception as e:
            self.logger.error(f"Error forwarding ACK: {e}")
            self.publish_metric('AckForwardFailed', 1)

    def connect_mosquitto(self):
        client = mqtt.Client(client_id="ack_forwarder")
        client.on_connect = self.on_mosquitto_connect
        client.on_disconnect = self.on_mosquitto_disconnect
        client.on_message = self.on_mosquitto_message
        if self.config.get('mqtt_user') and self.config.get('mqtt_pass'):
            client.username_pw_set(self.config['mqtt_user'], self.config['mqtt_pass'])
        client.connect(self.config['mqtt_broker'], self.config['mqtt_port'], keepalive=60)
        client.loop_start()
        self.mosquitto_client = client
        for _ in range(10):
            if self.mosquitto_connected:
                return True
            time.sleep(0.5)
        return False

    def run(self):
        self.logger.info("=" * 60)
        self.logger.info("ACK Forwarder starting...")
        self.logger.info("=" * 60)
        try:
            if not self.connect_mosquitto():
                self.logger.error("Failed to connect to Mosquitto")
                return 1
            if not self.connect_iot_boto3():
                self.logger.error("Failed to connect to IoT Core")
                return 1
            self.logger.info("✓ ACK Forwarder running")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            return 0
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
            return 1
        finally:
            if self.mosquitto_client:
                self.mosquitto_client.loop_stop()
                self.mosquitto_client.disconnect()
