"""
Dispatcher - Pure boundary to transport.
Publishes payload. Nothing more.
"""

import json
import logging
import boto3
from typing import Dict, Any
from command_envelope import CommandEnvelope

logger = logging.getLogger(__name__)


class DispatchError(Exception):
    """Failed to dispatch command"""
    pass


class MQTTAdapter:
    """
    MQTT transport adapter.
    Pure boundary - no interpretation.
    """
    
    def __init__(self, iot_endpoint: str):
        self.client = boto3.client(
            'iot-data',
            endpoint_url=f'https://{iot_endpoint}'
        )
    
    def publish(self, topic: str, payload: str, qos: int = 1) -> None:
        """
        Publish to MQTT topic.
        
        Does NOT:
        - Retry beyond basic publish
        - Wait for ACK
        - Interpret payload
        - Track state
        
        After this succeeds: done.
        """
        try:
            self.client.publish(
                topic=topic,
                qos=qos,
                payload=payload.encode('utf-8')
            )
            logger.debug(f"Published to {topic}, qos={qos}")
        except Exception as e:
            raise DispatchError(f"MQTT publish failed: {e}")


def extract_target_id(payload: Dict[str, Any]) -> str:
    """
    Extract target ID from payload.
    
    Convention: payload should contain 'target_id' or 'drone_id'.
    Falls back to 'default' if missing.
    
    This is the ONLY payload inspection dispatcher does.
    """
    return payload.get('target_id') or payload.get('drone_id') or 'default'


def dispatch(command: CommandEnvelope, mqtt_adapter: MQTTAdapter) -> None:
    """
    Dispatch command to transport.
    
    Does NOT:
    - Understand MAVLink
    - Modify payload
    - Add retry logic
    - Track execution
    
    Only:
    - Extracts payload
    - Publishes to MQTT
    - Logs result
    
    This is the boundary between cloud and broker.
    """
    logger.info(f"Dispatching command: {command.command_id}")
    
    # Extract target for topic routing
    target_id = extract_target_id(command.payload)
    
    # Build topic (convention: drone/{target_id}/cmd)
    topic = f"drone/{target_id}/cmd"
    
    # Publish raw payload (broker interprets, not us)
    payload_str = json.dumps(command.payload)
    
    try:
        mqtt_adapter.publish(topic, payload_str)
        logger.info(f"Dispatched to {topic}: {command.command_id}")
    except DispatchError as e:
        logger.error(f"Dispatch failed: {e}")
        raise