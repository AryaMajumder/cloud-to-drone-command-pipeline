#!/usr/bin/env python3
"""
Forwarder Library - SigV4 Version
Compatible with existing GitHub forwarder structure but uses access keys
"""

import json
import logging
import time
from datetime import datetime
from typing import Optional, Callable
import paho.mqtt.client as mqtt
import boto3
from botocore.exceptions import ClientError

try:
    from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False
    print("Warning: AWSIoTPythonSDK not available, falling back to boto3")


class ForwarderConfig:
    """Configuration for forwarders using SigV4"""
    
    def __init__(self, config_dict):
        # AWS Credentials
        self.aws_access_key_id = config_dict.get('aws_access_key_id')
        self.aws_secret_access_key = config_dict.get('aws_secret_access_key')
        self.aws_region = config_dict.get('aws_region', 'us-east-1')
        
        # AWS IoT Core
        self.iot_endpoint = config_dict['iot_endpoint']
        
        # Local Mosquitto
        self.mqtt_broker = config_dict.get('mqtt_broker', 'localhost')
        self.mqtt_port = config_dict.get('mqtt_port', 1883)
        self.mqtt_user = config_dict.get('mqtt_user')
        self.mqtt_pass = config_dict.get('mqtt_pass')
        
        # CloudWatch
        self.cloudwatch_namespace = config_dict.get('cloudwatch_namespace')
        self.cloudwatch_enabled = config_dict.get('cloudwatch_enabled', True)
        
        # Drone ID
        self.drone_id = config_dict.get('drone_id', 'drone_001')


class ForwarderBase:
    """Base class for forwarders using SigV4"""
    
    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.mqtt_client = None
        self.iot_client = None
        self.mqtt_connected = False
        self.iot_connected = False
        
        # Setup logging
        self.logger = logging.getLogger(name)
        
        # Setup boto3 session for credentials
        if config.aws_access_key_id and config.aws_secret_access_key:
            self.session = boto3.Session(
                aws_access_key_id=config.aws_access_key_id,
                aws_secret_access_key=config.aws_secret_access_key,
                region_name=config.aws_region
            )
        else:
            # Use default credentials (IAM role)
            self.session = boto3.Session(region_name=config.aws_region)
        
        # Setup CloudWatch
        if config.cloudwatch_enabled and config.cloudwatch_namespace:
            try:
                self.cloudwatch = self.session.client('cloudwatch')
            except Exception as e:
                self.logger.warning(f"CloudWatch client init failed: {e}")
                self.cloudwatch = None
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
    
    def connect_mosquitto(self, on_connect_callback, on_message_callback):
        """Connect to local Mosquitto broker"""
        client = mqtt.Client(client_id=f"{self.name}_forwarder_{int(time.time())}")
        
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
    
    def connect_iot_sigv4(self):
        """Connect to AWS IoT Core using SigV4 (WebSocket)"""
        if not SDK_AVAILABLE:
            raise Exception("AWSIoTPythonSDK not installed. Install with: pip install AWSIoTPythonSDK")
        
        self.logger.info(f"Connecting to AWS IoT Core at {self.config.iot_endpoint} (SigV4)")
        
        # Get credentials
        credentials = self.session.get_credentials()
        if credentials is None:
            raise Exception("No AWS credentials found")
        
        frozen_creds = credentials.get_frozen_credentials()
        
        # Create IoT client with WebSocket
        self.iot_client = AWSIoTMQTTClient(
            f"{self.name}_forwarder",
            useWebsocket=True
        )
        
        # Configure endpoint (port 443 for WebSocket)
        self.iot_client.configureEndpoint(self.config.iot_endpoint, 443)
        
        # Configure IAM credentials
        self.iot_client.configureIAMCredentials(
            frozen_creds.access_key,
            frozen_creds.secret_key,
            frozen_creds.token
        )
        
        # Configure connection parameters
        self.iot_client.configureAutoReconnectBackoffTime(1, 32, 20)
        self.iot_client.configureOfflinePublishQueueing(-1)
        self.iot_client.configureDrainingFrequency(2)
        self.iot_client.configureConnectDisconnectTimeout(10)
        self.iot_client.configureMQTTOperationTimeout(5)
        
        # Connect
        if self.iot_client.connect():
            self.iot_connected = True
            self.logger.info("Connected to AWS IoT Core")
        else:
            raise Exception("Failed to connect to IoT Core")
        
        return self.iot_client
    
    def publish_to_iot(self, topic, payload, qos=1):
        """Publish message to AWS IoT Core"""
        if not self.iot_connected or not self.iot_client:
            self.logger.error("Cannot publish to IoT: not connected")
            return False
        
        try:
            # Ensure payload is string
            if isinstance(payload, dict):
                payload = json.dumps(payload)
            elif isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            
            self.iot_client.publish(topic, payload, qos)
            self.logger.debug(f"Published to IoT: {topic}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to publish to IoT: {e}")
            return False
    
    def subscribe_to_iot(self, topic, callback, qos=1):
        """Subscribe to AWS IoT Core topic"""
        if not self.iot_connected or not self.iot_client:
            raise Exception("Cannot subscribe: not connected to IoT Core")
        
        def wrapped_callback(client, userdata, message):
            try:
                callback(message.topic, message.payload)
            except Exception as e:
                self.logger.error(f"Error in IoT callback: {e}")
        
        self.iot_client.subscribe(topic, qos, wrapped_callback)
        self.logger.info(f"Subscribed to IoT topic: {topic}")