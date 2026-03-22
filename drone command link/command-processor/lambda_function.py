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
import sys
from datetime import datetime, timezone
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError

from command_envelope import wrap_payload, CommandEnvelope
from processor import process, StructuralError
from dispatcher import dispatch, MQTTAdapter, DispatchError

# ============================================
# CRITICAL: Force logging configuration
# ============================================
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout,
    force=True
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("=" * 60)
logger.info("Lambda module loaded - logging configured")
logger.info("=" * 60)

# Environment
QUEUE_URL       = os.environ.get('QUEUE_URL')
IOT_ENDPOINT    = os.environ.get('IOT_ENDPOINT')
DYNAMODB_TABLE  = os.environ.get('COMMANDS_TABLE', 'DroneCommands')

# DynamoDB resource (module-level, reused across invocations)
_dynamodb = boto3.resource('dynamodb')
_commands_table = _dynamodb.Table(DYNAMODB_TABLE)


# ─────────────────────────────────────────────────────────────────────────────
# DynamoDB helper
# ─────────────────────────────────────────────────────────────────────────────

def mark_published_to_iot(command_id: str) -> None:
    """
    Write PUBLISHED_TO_IOT status to DynamoDB.

    Called immediately after a successful dispatch() so the lifecycle
    transition is recorded before any ACK can arrive.

    Raises on failure — the caller (process_and_dispatch) will catch it,
    log it, and re-raise so SQS retries the message.
    """
    timestamp = datetime.now(timezone.utc).isoformat()

    logger.info(f"Writing PUBLISHED_TO_IOT for command_id={command_id} to table={DYNAMODB_TABLE}")

    try:
        _commands_table.update_item(
            Key={'command_id': command_id},
            UpdateExpression='SET #s = :status, updated_at = :ts',
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={
                ':status': 'PUBLISHED_TO_IOT',
                ':ts': timestamp,
            },
        )
        logger.info(f"✓ DynamoDB updated: command_id={command_id} status=PUBLISHED_TO_IOT")

    except ClientError as e:
        logger.error(
            f"✗ DynamoDB update FAILED for command_id={command_id}: "
            f"{e.response['Error']['Code']} — {e.response['Error']['Message']}"
        )
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Producer role
# ─────────────────────────────────────────────────────────────────────────────

def produce_to_queue(payload: Dict[str, Any]) -> CommandEnvelope:
    """
    Producer role: Create command envelope and enqueue.

    Invariant: "I created this command."
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("=== ROLE: PRODUCER ===")
    logger.info("=" * 60)
    logger.info(f"Queue URL: {QUEUE_URL}")
    logger.info(f"Input payload: {json.dumps(payload)}")

    command = wrap_payload(payload)

    logger.info(f"Created command: {command.command_id}")
    logger.info(f"Timestamp: {command.created_at}")

    target_id = payload.get('target_id', 'default')

    logger.info(f"Enqueueing to SQS (MessageGroupId: {target_id})...")

    try:
        sqs = boto3.client('sqs')
        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(command.to_dict()),
            MessageDeduplicationId=command.command_id,
            MessageGroupId=target_id
        )
        logger.info(f"✓ Enqueued successfully — SQS Message ID: {response['MessageId']}")

    except Exception as e:
        logger.error(f"✗ Failed to enqueue: {e}")
        raise

    logger.info("=" * 60)
    logger.info("PRODUCER COMPLETE")
    logger.info("=" * 60)
    logger.info("")

    return command


# ─────────────────────────────────────────────────────────────────────────────
# Processor + Dispatcher role
# ─────────────────────────────────────────────────────────────────────────────

def process_and_dispatch(sqs_records: list, mqtt_adapter: MQTTAdapter) -> Dict[str, Any]:
    """
    Processor + Dispatcher roles: Validate structure and publish.

    Invariants:
    - Processor:   "This command is structurally valid."
    - Dispatcher:  "I delivered this command."
    - Post-dispatch: DynamoDB is updated to PUBLISHED_TO_IOT.
    """
    processed  = []
    rejected   = []
    dispatched = []
    failed     = []

    logger.info("")
    logger.info("=" * 60)
    logger.info("=== ROLE: PROCESSOR ===")
    logger.info("=" * 60)
    logger.info(f"IoT Endpoint: {IOT_ENDPOINT}")
    logger.info(f"Processing {len(sqs_records)} message(s)")

    for record in sqs_records:
        command = None
        try:
            # ── Deserialize ───────────────────────────────────────────
            body = json.loads(record['body'])
            logger.info(f"Message body: {json.dumps(body)}")

            command = CommandEnvelope.from_dict(body)

            # ── ROLE: PROCESSOR ───────────────────────────────────────
            logger.info(f"Validating command: {command.command_id}")
            validated = process(command)
            processed.append(validated.command_id)
            logger.info(f"✓ Command structure valid: {command.command_id}")

            # ── ROLE: DISPATCHER ──────────────────────────────────────
            logger.info("")
            logger.info("=" * 60)
            logger.info("=== ROLE: DISPATCHER ===")
            logger.info("=" * 60)

            target_id = validated.payload.get('target_id') or validated.payload.get('drone_id') or 'default'
            topic = f"drone/{target_id}/cmd"

            logger.info(f"Target: {target_id}")
            logger.info(f"Topic:  {topic}")
            logger.info(f"Payload: {json.dumps(validated.payload)}")

            dispatch(validated, mqtt_adapter)
            dispatched.append(validated.command_id)
            logger.info(f"✓ Dispatched to {topic}: {command.command_id}")

            # ── POST-DISPATCH: record status in DynamoDB ──────────────
            # This is the only place PUBLISHED_TO_IOT is written.
            # It runs after a confirmed successful publish so the state
            # transition is accurate. If this fails, the exception
            # propagates, SQS retries the message, and dispatch is
            # idempotent (same payload, same command_id, qos=1).
            mark_published_to_iot(validated.command_id)

        except StructuralError as e:
            logger.warning(f"✗ Structural validation failed: {e}")
            rejected.append({
                'command': record['body'],
                'reason': str(e)
            })

        except DispatchError as e:
            logger.error(f"✗ Dispatch failed: {e}")
            failed.append({
                'command_id': command.command_id if command else 'unknown',
                'reason': str(e)
            })
            # Re-raise so SQS retries
            raise

        except ClientError as e:
            # DynamoDB write failed after a successful IoT publish.
            # Re-raise so SQS retries. The publish is idempotent (same
            # command_id) so a duplicate publish is safe.
            logger.error(
                f"✗ DynamoDB PUBLISHED_TO_IOT write failed for "
                f"command_id={command.command_id if command else 'unknown'}: {e}"
            )
            raise

        except Exception as e:
            logger.error(f"✗ Unexpected error: {e}")
            logger.error("Traceback:", exc_info=True)
            rejected.append({
                'command': record['body'],
                'reason': f"Processing error: {e}"
            })

    logger.info("")
    logger.info("=" * 60)
    logger.info("PROCESSOR + DISPATCHER COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Summary: Processed={len(processed)}, Dispatched={len(dispatched)}, Rejected={len(rejected)}, Failed={len(failed)}")
    logger.info("=" * 60)
    logger.info("")

    return {
        'processed':        len(processed),
        'rejected':         len(rejected),
        'dispatched':       len(dispatched),
        'failed':           len(failed),
        'rejected_details': rejected
    }


# ─────────────────────────────────────────────────────────────────────────────
# Lambda entry point
# ─────────────────────────────────────────────────────────────────────────────

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda entry point.

    Determines role based on event source:
    - Direct invocation → PRODUCER
    - SQS trigger       → PROCESSOR + DISPATCHER
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("LAMBDA INVOKED")
    logger.info("=" * 60)
    logger.info(f"Request ID: {context.aws_request_id}")
    logger.info(f"Function: {context.function_name}")
    logger.info(f"Event source: {event.get('Records', [{}])[0].get('eventSource', 'direct')}")
    logger.info("=" * 60)

    try:
        if 'Records' in event and event['Records'][0].get('eventSource') == 'aws:sqs':
            # PROCESSOR + DISPATCHER flow
            mqtt_adapter = MQTTAdapter(iot_endpoint=IOT_ENDPOINT)
            result = process_and_dispatch(event['Records'], mqtt_adapter)
            return {'statusCode': 200, **result}

        else:
            # PRODUCER flow (direct invocation)
            payload = event.get('payload', event)
            command = produce_to_queue(payload)
            return {
                'statusCode':  200,
                'command_id':  command.command_id,
                'created_at':  command.created_at
            }

    except Exception as e:
        logger.error("")
        logger.error("=" * 60)
        logger.error(f"LAMBDA FAILED: {str(e)}")
        logger.error("=" * 60)
        logger.error("Full traceback:", exc_info=True)
        logger.error("=" * 60)

        return {
            'statusCode': 500,
            'error': str(e)
        }


# Module loaded
logger.info("Lambda handler ready")
logger.info("=" * 60)


if __name__ == '__main__':
    test_payload = {
        "target_id": "DRONE01",
        "action": "rtl"
    }
    result = lambda_handler({'payload': test_payload}, None)
    print(json.dumps(result, indent=2))
