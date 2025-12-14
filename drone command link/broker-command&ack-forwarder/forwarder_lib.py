#!/usr/bin/env python3
"""
Shared library for all forwarders
Provides common connection logic, metrics, and utilities
"""

import json
import logging
import hmac
import hashlib
from datetime import datetime
from awscrt import mqtt as aws_mqtt
from awsiot import mqtt_connection_builder
import paho.mqtt.client as mqtt
import boto3

class ForwarderConfig:
    """Configuration holder for forwarders"""
    
    def __init__(self, config_dict):
        # AWS IoT Core
        self.iot_endpoint = config_dict['iot_endpoint']
        self.iot_cert = config_dict['iot_cert']
        self.iot_key = config_dict['iot_key']
        self.iot_ca = config_dict['iot_ca']
        
        # Local Mosquitto
        self.mqtt_broker = config_dict.get('mqtt_broker', 'localhost')
        self.mqtt_port = config_dict.get('mqtt_port', 1883)
        self.mqtt_user = config_dict.get('mqtt_user')
        self.mqtt_pass = config_dict.get('mqtt_pass')
        
        # CloudWatch
        self.cloudwatch_namespace = config_dict.get('cloudwatch_namespace')
        self.cloudwatch_enabled = config_dict.get('cloudwatch_enabled', True)
        
        # Signing (for telemetry)
        self.signing_key = config_dict.get('signing_key')
        
        # Drone ID
        self.drone_id = config_dict.get('drone_id', 'drone_001')

class ForwarderBase:
    """Base class for all forwarders"""
    
    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.mqtt_client = None
        self.iot_connection = None
        self.mqtt_connected = False
        self.iot_connected = False
        
        # Setup logging
        self.logger = logging.getLogger(name)
        
        # Setup CloudWatch
        if config.cloudwatch_enabled and config.cloudwatch_namespace:
            self.cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')
        else:
            self.cloudwatch = None
    
    def publish_metric(self, metric_name, value, unit='Count'):
        """Publish metric to CloudWatch"""
        if not self.cloudwatch:
            return
        
        try:
            self.cloudwatch.put_metric_data(
                Namespace=self.config.cloudwatch_namespace,
                MetricData=[{
                    'MetricName': metric_name,
                    'Value': value,
                    'Unit': unit,
                    'Timestamp': datetime.utcnow()
                }]
            )
        except Exception as e:
            self.logger.error(f"CloudWatch publish failed: {e}")
    
    def sign_message(self, payload):
        """Sign message with HMAC (for telemetry)"""
        if not self.config.signing_key:
            return payload
        
        try:
            signature = hmac.new(
                self.config.signing_key.encode(),
                payload.encode(),
                hashlib.sha256
            ).hexdigest()
            
            data = json.loads(payload)
            data['signature'] = signature
            data['forwarder_ts'] = datetime.utcnow().isoformat()
            
            return json.dumps(data)
        except Exception as e:
            self.logger.error(f"Signing failed: {e}")
            return payload
    
    def connect_mosquitto(self, on_connect_callback, on_message_callback):
        """Connect to local Mosquitto broker"""
        client = mqtt.Client(client_id=f"{self.name}_forwarder")
        
        # Set callbacks
        def wrapped_on_connect(client, userdata, flags, rc):
            if rc == 0:
                self.mqtt_connected = True
            else:
                self.mqtt_connected = False
            on_connect_callback(client, userdata, flags, rc)
        
        def wrapped_on_disconnect(client, userdata, rc):
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
    
    def connect_iot(self):
        """Connect to AWS IoT Core"""
        self.logger.info(f"Connecting to AWS IoT Core at {self.config.iot_endpoint}")
        
        def on_connection_interrupted(connection, error, **kwargs):
            self.iot_connected = False
            self.logger.warning(f"IoT connection interrupted: {error}")
        
        def on_connection_resumed(connection, return_code, session_present, **kwargs):
            self.iot_connected = True
            self.logger.info(f"IoT connection resumed: {return_code}")
        
        connection = mqtt_connection_builder.mtls_from_path(
            endpoint=self.config.iot_endpoint,
            cert_filepath=self.config.iot_cert,
            pri_key_filepath=self.config.iot_key,
            ca_filepath=self.config.iot_ca,
            client_id=f"{self.name}-forwarder",
            clean_session=False,
            keep_alive_secs=30,
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed
        )
        
        connect_future = connection.connect()
        connect_future.result()
        self.iot_connected = True
        
        return connection