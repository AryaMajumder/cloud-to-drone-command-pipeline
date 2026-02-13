"""
Single Lambda - Multiple Roles.

Roles determined by event source:
- Direct invocation → PRODUCER
- SQS trigger → PROCESSOR + DISPATCHER

Roles are explicit. Boundaries enforced. Logs identify transitions.
"""

import os
import json
import logging
from typing import Dict, Any

from command_envelope import wrap_payload, CommandEnvelope
from processor import process, StructuralError
from dispatcher import dispatch, MQTTAdapter, DispatchError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Environment
QUEUE_URL = os.environ.get('QUEUE_URL')
IOT_ENDPOINT = os.environ.get('IOT_ENDPOINT')


def produce_to_queue(payload: Dict[str, Any]) -> CommandEnvelope:
    """
    Producer role: Create command envelope and enqueue.
    
    Invariant: "I created this command."
    """
    import boto3
    
    logger.info("=== ROLE: PRODUCER ===")
    
    # Wrap input
    command = wrap_payload(payload)
    
    logger.info(f"Created command: {command.command_id}")
    
    # Enqueue
    sqs = boto3.client('sqs')
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(command.to_dict()),
        MessageDeduplicationId=command.command_id,
        MessageGroupId=payload.get('target_id', 'default')
    )
    
    logger.info(f"Enqueued command: {command.command_id}")
    
    return command


def process_and_dispatch(sqs_records: list, mqtt_adapter: MQTTAdapter) -> Dict[str, Any]:
    """
    Processor + Dispatcher roles: Validate structure and publish.
    
    Invariants:
    - Processor: "This command is structurally valid."
    - Dispatcher: "I delivered this command."
    """
    processed = []
    rejected = []
    dispatched = []
    failed = []
    
    for record in sqs_records:
        try:
            # Deserialize
            body = json.loads(record['body'])
            command = CommandEnvelope.from_dict(body)
            
            # === ROLE: PROCESSOR ===
            logger.info("=== ROLE: PROCESSOR ===")
            validated = process(command)
            processed.append(validated.command_id)
            
            # === ROLE: DISPATCHER ===
            logger.info("=== ROLE: DISPATCHER ===")
            dispatch(validated, mqtt_adapter)
            dispatched.append(validated.command_id)
            
        except StructuralError as e:
            logger.warning(f"Structural validation failed: {e}")
            rejected.append({
                'command': record['body'],
                'reason': str(e)
            })
            
        except DispatchError as e:
            logger.error(f"Dispatch failed: {e}")
            failed.append({
                'command_id': command.command_id,
                'reason': str(e)
            })
            # Re-raise to trigger SQS retry
            raise
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            rejected.append({
                'command': record['body'],
                'reason': f"Processing error: {e}"
            })
    
    return {
        'processed': len(processed),
        'rejected': len(rejected),
        'dispatched': len(dispatched),
        'failed': len(failed),
        'rejected_details': rejected
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda entry point.
    
    Determines role based on event source:
    - Direct invocation → PRODUCER
    - SQS trigger → PROCESSOR + DISPATCHER
    
    Each role:
    - Enforces its boundary
    - Logs its transition
    - Preserves its invariant
    """
    logger.info(f"Lambda invoked, event source: {event.get('Records', [{}])[0].get('eventSource', 'direct')}")
    
    try:
        # Check event source
        if 'Records' in event and event['Records'][0].get('eventSource') == 'aws:sqs':
            # PROCESSOR + DISPATCHER flow
            mqtt_adapter = MQTTAdapter(iot_endpoint=IOT_ENDPOINT)
            result = process_and_dispatch(event['Records'], mqtt_adapter)
            
            return {
                'statusCode': 200,
                **result
            }
        
        else:
            # PRODUCER flow (direct invocation)
            payload = event.get('payload', event)
            command = produce_to_queue(payload)
            
            return {
                'statusCode': 200,
                'command_id': command.command_id,
                'created_at': command.created_at
            }
    
    except Exception as e:
        logger.error(f"Lambda failed: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'error': str(e)
        }


if __name__ == '__main__':
    # Local testing
    test_payload = {
        "target_id": "DRONE01",
        "action": "rtl"
    }
    
    result = lambda_handler({'payload': test_payload}, None)
    print(json.dumps(result, indent=2))