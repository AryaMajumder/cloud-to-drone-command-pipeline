#!/usr/bin/env python3
"""
ACK Forwarder: Agent → Cloud (Enhanced with Resilience)
Uses boto3 IoT Data client (IAM role auth, no certs needed)
"""

import os
import json
import time
import logging
import threading
import sys
from collections import deque
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../lib'))

import paho.mqtt.client as mqtt
import boto3

# Setup logging
LOG_FILE = os.path.join(os.path.dirname(__file__), 'logs', 'ack_forwarder.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

class ResilientAckForwarder:
    """ACK Forwarder with broker-to-cloud resilience"""
    
    def __init__(self):
        self.logger = logging.getLogger("ack")
        
        # Load config from environment
        self.iot_endpoint = os.environ.get('iot_endpoint')
        self.aws_region = os.environ.get('aws_region', 'us-east-1')
        self.drone_id = os.environ.get('drone_id', 'drone_001')
        self.mqtt_broker = os.environ.get('mqtt_broker', 'localhost')
        self.mqtt_port = int(os.environ.get('mqtt_port', '1883'))
        
        if not self.iot_endpoint:
            raise ValueError("iot_endpoint not set in environment")
        
        # Create boto3 IoT Data client
        self.iot_data = self._create_iot_client()
        
        # MQTT client
        self.mqtt_client = None
        self.mqtt_connected = False
        
        # Resilience components
        self.buffer = deque()
        self.max_buffer_size = 1000
        self.buffer_lock = threading.Lock()
        
        # Connection state
        self.iot_connection_healthy = True  # boto3 is always "connected"
        self.reconnect_attempts = 0
        
        # Statistics
        self.stats = {
            'messages_received': 0,
            'messages_forwarded': 0,
            'messages_buffered': 0,
            'messages_dropped': 0,
            'reconnection_attempts': 0
        }
        
        self.running = False
    
    def _create_iot_client(self):
        """Create boto3 IoT Data client"""
        endpoint_url = f"https://{self.iot_endpoint}"
        self.logger.info(f"Creating IoT Data client for {endpoint_url}")
        return boto3.client('iot-data', 
                          region_name=self.aws_region,
                          endpoint_url=endpoint_url)
    
    def on_connect(self, client, userdata, flags, rc):
        """Callback when connected to local Mosquitto"""
        if rc == 0:
            self.mqtt_connected = True
            self.logger.info("✓ Connected to Mosquitto")
            topic = "drone/+/cmd_ack"
            client.subscribe(topic, qos=1)
            self.logger.info(f"✓ Subscribed to {topic}")
        else:
            self.mqtt_connected = False
            self.logger.error(f"❌ Mosquitto connection failed: rc={rc}")
    
    def on_disconnect(self, client, userdata, rc):
        """Callback when disconnected from Mosquitto"""
        self.mqtt_connected = False
        if rc != 0:
            self.logger.warning(f"⚠️ Unexpected disconnect from Mosquitto: rc={rc}")
    
    def on_message(self, client, userdata, msg):
        """
        Callback when ACK received from local Mosquitto.
        Try immediate delivery or buffer if cloud unavailable.
        """
        topic = msg.topic
        payload = msg.payload.decode('utf-8')
        
        self.stats['messages_received'] += 1
        
        try:
            data = json.loads(payload)
            cmd_id = data.get('cmd_id', 'unknown')
            status = data.get('status', 'unknown')
            
            self.logger.info(f"📨 ACK received: cmd_id={cmd_id}, status={status}")
            
            # Try immediate delivery if cloud connection healthy
            if self.iot_connection_healthy:
                success = self._publish_to_iot(topic, payload)
                
                if success:
                    self.stats['messages_forwarded'] += 1
                    self.logger.info(f"✅ ACK forwarded to cloud: {cmd_id}")
                    return
                else:
                    self.iot_connection_healthy = False
                    self.logger.warning(f"⚠️ Cloud publish failed")
            
            # Buffer the message
            self._buffer_message(topic, payload, cmd_id)
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Invalid JSON in ACK: {e}")
        except Exception as e:
            self.logger.exception(f"❌ Error handling ACK: {e}")
    
    def _publish_to_iot(self, topic, payload):
        """
        Publish to IoT Core using boto3.
        Returns True if successful, False if failed.
        """
        try:
            self.iot_data.publish(
                topic=topic,
                qos=1,
                payload=payload.encode('utf-8') if isinstance(payload, str) else payload
            )
            return True
        except Exception as e:
            self.logger.error(f"❌ IoT publish failed: {e}")
            return False
    
    def _buffer_message(self, topic, payload, cmd_id):
        """Add message to buffer with overflow handling"""
        with self.buffer_lock:
            if len(self.buffer) >= self.max_buffer_size:
                dropped = self.buffer.popleft()
                self.stats['messages_dropped'] += 1
                self.logger.error(f"❌ BUFFER OVERFLOW: Dropped {dropped['cmd_id']}")
            
            self.buffer.append({
                'topic': topic,
                'payload': payload,
                'cmd_id': cmd_id,
                'buffered_at': time.time(),
                'retry_count': 0
            })
            
            self.stats['messages_buffered'] += 1
            self.logger.warning(f"⚠️ Buffered ACK {cmd_id} (queue: {len(self.buffer)})")
    
    def _resilience_loop(self):
        """
        Background thread that handles:
        1. Connection monitoring
        2. Reconnection with exponential backoff
        3. Buffer draining when connection restored
        """
        self.logger.info("🔄 Resilience loop started")
        
        while self.running:
            try:
                if not self.iot_connection_healthy:
                    self._attempt_reconnection()
                
                if self.iot_connection_healthy and self.buffer:
                    self._drain_buffer()
                
                time.sleep(1)
                
            except Exception as e:
                self.logger.exception(f"❌ Error in resilience loop: {e}")
                time.sleep(5)
    
    def _attempt_reconnection(self):
        """Test IoT connection with small publish"""
        delay = min(2 ** self.reconnect_attempts, 60)
        
        self.logger.info(f"🔌 Testing IoT connection (attempt #{self.reconnect_attempts + 1})")
        
        try:
            test_topic = "drone/test/health"
            test_payload = json.dumps({
                'forwarder': 'ack',
                'timestamp': datetime.utcnow().isoformat(),
                'buffer_size': len(self.buffer)
            })
            
            success = self._publish_to_iot(test_topic, test_payload)
            
            if success:
                self.iot_connection_healthy = True
                self.reconnect_attempts = 0
                self.stats['reconnection_attempts'] += 1
                self.logger.info(f"✅ IoT connection restored! Buffer: {len(self.buffer)}")
            else:
                self.reconnect_attempts += 1
                time.sleep(delay)
        
        except Exception as e:
            self.reconnect_attempts += 1
            self.logger.error(f"❌ Reconnection test failed: {e}")
            time.sleep(delay)
    
    def _drain_buffer(self):
        """Send buffered messages to cloud in FIFO order"""
        initial_size = len(self.buffer)
        if initial_size == 0:
            return
            
        self.logger.info(f"🚰 Draining buffer ({initial_size} messages)...")
        
        drained = 0
        
        while self.buffer and self.iot_connection_healthy:
            with self.buffer_lock:
                if not self.buffer:
                    break
                msg = self.buffer[0]
            
            success = self._publish_to_iot(msg['topic'], msg['payload'])
            
            if success:
                with self.buffer_lock:
                    if self.buffer and self.buffer[0] == msg:
                        self.buffer.popleft()
                        drained += 1
                
                self.stats['messages_forwarded'] += 1
                self.logger.info(f"✅ Drained {msg['cmd_id']} ({len(self.buffer)} left)")
            
            else:
                self.iot_connection_healthy = False
                msg['retry_count'] += 1
                
                if msg['retry_count'] > 10:
                    with self.buffer_lock:
                        if self.buffer and self.buffer[0] == msg:
                            self.buffer.popleft()
                            self.stats['messages_dropped'] += 1
                    self.logger.error(f"❌ Giving up on {msg['cmd_id']} after 10 retries")
                
                break
            
            time.sleep(0.1)
        
        if drained > 0:
            self.logger.info(f"✅ Drained {drained}/{initial_size} messages")
    
    def _log_statistics(self):
        """Periodically log statistics"""
        while self.running:
            time.sleep(60)
            
            self.logger.info("=" * 60)
            self.logger.info("📊 STATISTICS")
            self.logger.info(f"  Received:  {self.stats['messages_received']}")
            self.logger.info(f"  Forwarded: {self.stats['messages_forwarded']}")
            self.logger.info(f"  Buffered:  {self.stats['messages_buffered']}")
            self.logger.info(f"  Dropped:   {self.stats['messages_dropped']}")
            self.logger.info(f"  Buffer:    {len(self.buffer)}")
            self.logger.info(f"  IoT:       {'✅' if self.iot_connection_healthy else '❌'}")
            self.logger.info("=" * 60)
    
    def run(self):
        """Main run loop"""
        self.logger.info("🚀 ACK Forwarder starting with resilience...")
        self.logger.info(f"🔌 IoT: {self.iot_endpoint}")
        self.logger.info(f"🔌 Mosquitto: {self.mqtt_broker}:{self.mqtt_port}")
        
        try:
            # Connect to local MQTT
            self.mqtt_client = mqtt.Client(client_id="ack_forwarder")
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.on_message = self.on_message
            self.mqtt_client.on_disconnect = self.on_disconnect
            
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, keepalive=60)
            self.mqtt_client.loop_start()
            
            time.sleep(2)
            if not self.mqtt_connected:
                self.logger.error("❌ Failed to connect to Mosquitto")
                return 1
            
            self.logger.info("✅ Mosquitto connected")
            
            # Start background threads
            self.running = True
            
            resilience_thread = threading.Thread(
                target=self._resilience_loop,
                daemon=True,
                name="resilience"
            )
            resilience_thread.start()
            
            stats_thread = threading.Thread(
                target=self._log_statistics,
                daemon=True,
                name="stats"
            )
            stats_thread.start()
            
            self.logger.info("=" * 60)
            self.logger.info("✅ ACK FORWARDER RUNNING (with resilience)")
            self.logger.info("=" * 60)
            
            while True:
                time.sleep(1)
        
        except KeyboardInterrupt:
            self.logger.info("👋 Shutting down...")
            self.running = False
            return 0
        except Exception as e:
            self.logger.exception(f"💥 Fatal: {e}")
            return 1


if __name__ == "__main__":
    forwarder = ResilientAckForwarder()
    exit(forwarder.run())
