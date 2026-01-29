#!/usr/bin/env python3
"""
ACK Forwarder: Agent → Cloud (Enhanced with Resilience)

Enhancements:
- Message buffering when cloud unavailable
- Exponential backoff retry
- Connection state tracking
- Buffer overflow handling
- Graceful reconnection
"""

import os
import json
import time
import logging
import threading
from collections import deque
from datetime import datetime
from forwarder_lib import ForwarderBase, ForwarderConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class ResilientAckForwarder(ForwarderBase):
    """ACK Forwarder with broker-to-cloud resilience"""
    
    def __init__(self, config):
        super().__init__("ack", config)
        
        # Resilience components
        self.buffer = deque()  # Message buffer (FIFO queue)
        self.max_buffer_size = 1000  # Max messages to buffer
        self.buffer_lock = threading.Lock()
        
        # Connection state
        self.iot_connection_healthy = False
        self.reconnect_attempts = 0
        self.max_reconnect_delay = 60  # Max backoff time in seconds
        
        # Statistics
        self.stats = {
            'messages_received': 0,
            'messages_forwarded': 0,
            'messages_buffered': 0,
            'messages_dropped': 0,
            'reconnection_attempts': 0,
            'buffer_overflows': 0
        }
        
        # Start resilience loop in background thread
        self.resilience_thread = None
        self.running = False
    
    def on_connect(self, client, userdata, flags, rc):
        """Callback when connected to local Mosquitto"""
        if rc == 0:
            self.logger.info("✓ Connected to Mosquitto")
            topic = f"drone/{self.config.drone_id}/cmd_ack"
            client.subscribe(topic, qos=1)
            self.logger.info(f"✓ Subscribed to {topic}")
        else:
            self.logger.error(f"❌ Mosquitto connection failed: rc={rc}")
    
    def on_message(self, client, userdata, msg):
        """
        Callback when ACK received from local Mosquitto.
        
        This is the main entry point for ACKs from the companion.
        We try immediate delivery, or buffer if cloud unavailable.
        """
        topic = msg.topic
        payload = msg.payload.decode('utf-8')
        
        self.stats['messages_received'] += 1
        
        try:
            # Parse ACK message
            data = json.loads(payload)
            cmd_id = data.get('cmd_id', 'unknown')
            status = data.get('status', 'unknown')
            
            self.logger.info(f"📨 ACK received: cmd_id={cmd_id}, status={status}")
            
            # Try immediate delivery if cloud connection healthy
            if self.iot_connection_healthy:
                success = self._try_publish_to_iot(topic, payload)
                
                if success:
                    # Successfully forwarded
                    self.stats['messages_forwarded'] += 1
                    self.logger.info(f"✅ ACK forwarded to cloud: {cmd_id}")
                    self.publish_metric('AckForwarded', 1)
                    return
                else:
                    # Publish failed, mark connection unhealthy
                    self.iot_connection_healthy = False
                    self.logger.warning(f"⚠️ Cloud publish failed, marking connection unhealthy")
            
            # Cloud unavailable or publish failed - buffer the message
            self._buffer_message(topic, payload, cmd_id)
            
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Invalid JSON in ACK: {e}")
        except Exception as e:
            self.logger.exception(f"❌ Error handling ACK: {e}")
            self.publish_metric('AckForwardFailed', 1)
    
    def _try_publish_to_iot(self, topic, payload):
        """
        Attempt to publish message to IoT Core.
        Returns True if successful, False if failed.
        """
        try:
            # Use parent class publish method
            result = self.publish_to_iot(topic, payload, qos=1)
            return result
        except Exception as e:
            self.logger.error(f"❌ IoT publish exception: {e}")
            return False
    
    def _buffer_message(self, topic, payload, cmd_id):
        """
        Add message to buffer with overflow handling.
        """
        with self.buffer_lock:
            if len(self.buffer) >= self.max_buffer_size:
                # Buffer full - apply overflow policy
                self._handle_buffer_overflow(cmd_id)
            
            # Add to buffer
            self.buffer.append({
                'topic': topic,
                'payload': payload,
                'cmd_id': cmd_id,
                'buffered_at': time.time(),
                'retry_count': 0
            })
            
            self.stats['messages_buffered'] += 1
            self.logger.warning(
                f"⚠️ Buffered ACK {cmd_id} (queue size: {len(self.buffer)})"
            )
            self.publish_metric('AckBuffered', 1)
    
    def _handle_buffer_overflow(self, new_cmd_id):
        """
        Handle buffer overflow by dropping oldest message.
        """
        if self.buffer:
            dropped = self.buffer.popleft()
            self.stats['messages_dropped'] += 1
            self.stats['buffer_overflows'] += 1
            
            self.logger.error(
                f"❌ BUFFER OVERFLOW: Dropped ACK {dropped['cmd_id']} "
                f"to make room for {new_cmd_id}"
            )
            self.publish_metric('AckDropped', 1)
            self.publish_metric('BufferOverflow', 1)
    
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
                # Check if we need to reconnect
                if not self.iot_connection_healthy:
                    self._attempt_reconnection()
                
                # Try to drain buffer if connected
                if self.iot_connection_healthy and self.buffer:
                    self._drain_buffer()
                
                # Sleep briefly
                time.sleep(1)
                
            except Exception as e:
                self.logger.exception(f"❌ Error in resilience loop: {e}")
                time.sleep(5)
        
        self.logger.info("🔄 Resilience loop stopped")
    
    def _attempt_reconnection(self):
        """
        Attempt to reconnect to IoT Core with exponential backoff.
        """
        # Calculate backoff delay
        delay = min(2 ** self.reconnect_attempts, self.max_reconnect_delay)
        
        self.logger.info(
            f"🔌 Attempting IoT Core reconnection "
            f"(attempt #{self.reconnect_attempts + 1}, delay={delay}s)"
        )
        
        try:
            # Try to reconnect IoT connection
            # Note: IoT connection should auto-reconnect, but we verify health
            
            # Test connection with a small publish (to a test topic)
            test_topic = f"drone/{self.config.drone_id}/forwarder_health"
            test_payload = json.dumps({
                'forwarder': 'ack',
                'timestamp': datetime.utcnow().isoformat(),
                'buffer_size': len(self.buffer)
            })
            
            success = self._try_publish_to_iot(test_topic, test_payload)
            
            if success:
                # Connection restored!
                self.iot_connection_healthy = True
                self.reconnect_attempts = 0
                self.stats['reconnection_attempts'] += 1
                
                self.logger.info(
                    f"✅ IoT Core connection restored! "
                    f"Buffer has {len(self.buffer)} messages to drain"
                )
                self.publish_metric('ConnectionRestored', 1)
            else:
                # Still failing
                self.reconnect_attempts += 1
                self.logger.warning(
                    f"⚠️ Reconnection failed, will retry in {delay}s"
                )
                time.sleep(delay)
        
        except Exception as e:
            self.reconnect_attempts += 1
            self.logger.error(f"❌ Reconnection error: {e}")
            time.sleep(delay)
    
    def _drain_buffer(self):
        """
        Send buffered messages to cloud in FIFO order.
        Stops if connection fails again.
        """
        initial_size = len(self.buffer)
        self.logger.info(f"🚰 Draining buffer ({initial_size} messages)...")
        
        drained = 0
        
        while self.buffer and self.iot_connection_healthy:
            with self.buffer_lock:
                if not self.buffer:
                    break
                
                # Peek at oldest message
                msg = self.buffer[0]
            
            # Try to send it
            success = self._try_publish_to_iot(msg['topic'], msg['payload'])
            
            if success:
                # Successfully sent, remove from buffer
                with self.buffer_lock:
                    if self.buffer and self.buffer[0] == msg:
                        self.buffer.popleft()
                        drained += 1
                
                self.stats['messages_forwarded'] += 1
                
                self.logger.info(
                    f"✅ Drained ACK {msg['cmd_id']} "
                    f"(remaining: {len(self.buffer)})"
                )
                self.publish_metric('AckForwarded', 1)
            
            else:
                # Failed to send, connection lost again
                self.iot_connection_healthy = False
                
                msg['retry_count'] += 1
                
                self.logger.warning(
                    f"⚠️ Buffer drain interrupted (sent {drained}/{initial_size})"
                )
                
                # Check if message has been retried too many times
                if msg['retry_count'] > 10:
                    with self.buffer_lock:
                        if self.buffer and self.buffer[0] == msg:
                            self.buffer.popleft()
                            self.stats['messages_dropped'] += 1
                    
                    self.logger.error(
                        f"❌ Giving up on ACK {msg['cmd_id']} after 10 retries"
                    )
                    self.publish_metric('AckDropped', 1)
                
                break
            
            # Small delay between sends to avoid overwhelming
            time.sleep(0.1)
        
        if drained > 0:
            self.logger.info(
                f"✅ Buffer drain complete: {drained}/{initial_size} messages sent"
            )
    
    def _log_statistics(self):
        """Periodically log statistics"""
        while self.running:
            time.sleep(60)  # Every minute
            
            self.logger.info("=" * 60)
            self.logger.info("📊 FORWARDER STATISTICS (last minute)")
            self.logger.info(f"  Messages received:    {self.stats['messages_received']}")
            self.logger.info(f"  Messages forwarded:   {self.stats['messages_forwarded']}")
            self.logger.info(f"  Messages buffered:    {self.stats['messages_buffered']}")
            self.logger.info(f"  Messages dropped:     {self.stats['messages_dropped']}")
            self.logger.info(f"  Buffer size:          {len(self.buffer)}")
            self.logger.info(f"  IoT connection:       {'✅ HEALTHY' if self.iot_connection_healthy else '❌ UNHEALTHY'}")
            self.logger.info(f"  Reconnect attempts:   {self.stats['reconnection_attempts']}")
            self.logger.info(f"  Buffer overflows:     {self.stats['buffer_overflows']}")
            self.logger.info("=" * 60)
            
            # Publish aggregate metrics
            self.publish_metric('BufferSize', len(self.buffer), unit='Count')
            self.publish_metric('ConnectionHealthy', 1 if self.iot_connection_healthy else 0, unit='Count')
    
    def run(self):
        """Main run loop"""
        self.logger.info("🚀 ACK Forwarder starting with resilience...")
        
        try:
            # Connect to IoT Core
            self.logger.info(f"🔌 Connecting to IoT Core: {self.config.iot_endpoint}")
            self.iot_connection = self.connect_iot()
            self.iot_connection_healthy = True
            self.logger.info("✅ IoT Core connected")
            
            # Connect to local Mosquitto
            self.logger.info(f"🔌 Connecting to Mosquitto: {self.config.mqtt_broker}:{self.config.mqtt_port}")
            self.mqtt_client = self.connect_mosquitto(self.on_connect, self.on_message)
            
            # Wait for Mosquitto connection
            time.sleep(2)
            if not self.mqtt_connected:
                self.logger.error("❌ Failed to connect to Mosquitto")
                return 1
            
            self.logger.info("✅ Mosquitto connected")
            
            # Start background threads
            self.running = True
            
            # Resilience loop thread
            self.resilience_thread = threading.Thread(
                target=self._resilience_loop,
                daemon=True,
                name="resilience_loop"
            )
            self.resilience_thread.start()
            
            # Statistics logging thread
            stats_thread = threading.Thread(
                target=self._log_statistics,
                daemon=True,
                name="stats_logger"
            )
            stats_thread.start()
            
            self.logger.info("=" * 60)
            self.logger.info("✅ ACK FORWARDER RUNNING (with resilience)")
            self.logger.info("=" * 60)
            
            # Main loop - just keep running
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.logger.info("👋 Shutting down (Ctrl+C)...")
                self.running = False
                
                # Wait for resilience thread to finish
                if self.resilience_thread:
                    self.resilience_thread.join(timeout=5)
                
                # Final statistics
                self.logger.info("📊 FINAL STATISTICS:")
                self.logger.info(f"  Total received:   {self.stats['messages_received']}")
                self.logger.info(f"  Total forwarded:  {self.stats['messages_forwarded']}")
                self.logger.info(f"  Total dropped:    {self.stats['messages_dropped']}")
                self.logger.info(f"  Buffer remaining: {len(self.buffer)}")
                
                return 0
        
        except Exception as e:
            self.logger.exception(f"💥 Fatal error: {e}")
            return 1


if __name__ == "__main__":
    # Load configuration from environment or config file
    config = ForwarderConfig({
        'iot_endpoint': os.environ.get('IOT_ENDPOINT', 'YOUR_ENDPOINT.iot.us-east-1.amazonaws.com'),
        'iot_cert': os.environ.get('IOT_CERT', '/path/to/cert.pem'),
        'iot_key': os.environ.get('IOT_KEY', '/path/to/private.key'),
        'iot_ca': os.environ.get('IOT_CA', '/path/to/AmazonRootCA1.pem'),
        
        'mqtt_broker': os.environ.get('MQTT_BROKER', 'localhost'),
        'mqtt_port': int(os.environ.get('MQTT_PORT', '1883')),
        'mqtt_user': os.environ.get('MQTT_USER'),
        'mqtt_pass': os.environ.get('MQTT_PASS'),
        
        'cloudwatch_namespace': 'DroneC2/ACK',
        'cloudwatch_enabled': True,
        
        'drone_id': os.environ.get('DRONE_ID', 'drone_001')
    })
    
    forwarder = ResilientAckForwarder(config)
    exit(forwarder.run())
