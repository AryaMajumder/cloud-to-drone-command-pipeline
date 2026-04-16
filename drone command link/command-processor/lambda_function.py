"""
Single Lambda — Three Roles, Two Queues.

Role assignment by event source:
  Direct invocation     → PRODUCER
  SQS from CommandQueue → PROCESSOR   (validates + reasons, writes to DispatchQueue)
  SQS from DispatchQueue→ DISPATCHER  (speaks MQTT, no logic)

Queue topology:
  CommandQueue   — intent bus.    One message per intent (even multi-target).
  DispatchQueue  — work queue.    One message per drone per command.

Why two queues:
  The Processor needs a place to put its output. Without a DispatchQueue,
  the Processor would have to either call MQTT directly (breaks the boundary)
  or write back to CommandQueue (creates a loop). DispatchQueue is the clean
  hand-off between reasoning and delivery.

Producer invariant:  "I have intent. Here it is."
Processor invariant: "I reasoned about that intent."
Dispatcher invariant: "Given this command, I published exactly this."
"""

import os
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Union

import boto3
from botocore.exceptions import ClientError

from command_envelope import wrap_payload, CommandEnvelope
from processor import process, StructuralError
from dispatcher import dispatch, MQTTAdapter, DispatchError

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
    force=True,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("=" * 60)
logger.info("Lambda module loaded")
logger.info("=" * 60)

# ──────────────────────────────────────────────────────────────────────────────
# Environment
# ──────────────────────────────────────────────────────────────────────────────
COMMAND_QUEUE_URL  = os.environ.get("COMMAND_QUEUE_URL")   # Producer writes here
DISPATCH_QUEUE_URL = os.environ.get("DISPATCH_QUEUE_URL")  # Processor writes here
IOT_ENDPOINT       = os.environ.get("IOT_ENDPOINT")
DYNAMODB_TABLE     = os.environ.get("COMMANDS_TABLE", "DroneCommands")

_dynamodb       = boto3.resource("dynamodb")
_commands_table = _dynamodb.Table(DYNAMODB_TABLE)


# ──────────────────────────────────────────────────────────────────────────────
# SQS helper
# ──────────────────────────────────────────────────────────────────────────────

def _enqueue(sqs_client, queue_url: str, command: CommandEnvelope, target_id: str) -> str:
    """
    Send a single CommandEnvelope to any SQS FIFO queue.

    MessageGroupId  = target_id   (per-drone ordering)
    DeduplicationId = command_id  (exactly-once delivery)

    Returns SQS MessageId. Raises on failure.
    """
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(command.to_dict()),
        MessageDeduplicationId=command.command_id,
        MessageGroupId=target_id,
    )
    return response["MessageId"]


# ──────────────────────────────────────────────────────────────────────────────
# DynamoDB helper
# ──────────────────────────────────────────────────────────────────────────────

def mark_published_to_iot(command_id: str) -> None:
    """
    Write PUBLISHED_TO_IOT status to DynamoDB after a confirmed MQTT publish.

    Raises on failure so SQS retries the DispatchQueue message.
    Dispatcher publish is idempotent (same command_id, qos=1).
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    logger.info("Writing PUBLISHED_TO_IOT: command_id=%s", command_id)

    try:
        _commands_table.update_item(
            Key={"command_id": command_id},
            UpdateExpression="SET #s = :status, updated_at = :ts",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":status": "PUBLISHED_TO_IOT",
                ":ts":     timestamp,
            },
        )
        logger.info("✓ DynamoDB updated: command_id=%s → PUBLISHED_TO_IOT", command_id)

    except ClientError as e:
        logger.error(
            "✗ DynamoDB update FAILED for command_id=%s: %s — %s",
            command_id,
            e.response["Error"]["Code"],
            e.response["Error"]["Message"],
        )
        raise


# ──────────────────────────────────────────────────────────────────────────────
# ROLE: PRODUCER
# ──────────────────────────────────────────────────────────────────────────────

def produce_to_queue(payload: Dict[str, Any]) -> CommandEnvelope:
    """
    Producer role: wrap intent into an envelope and put it on the CommandQueue.

    Invariant: "I have intent. Here it is."

    Producer does NOT:
    - Know how many drones will receive this command
    - Know MQTT exists
    - Expand target_ids
    - Retry execution

    If 'target_ids' is present, the Producer puts it on the bus as-is.
    The Processor will expand it. This is deliberate: the Producer's job
    is to faithfully represent intent, not to reason about it.

    Always returns a single CommandEnvelope — one message on CommandQueue,
    regardless of how many targets the payload names.
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("=== ROLE: PRODUCER ===")
    logger.info("=" * 60)
    logger.info("Queue: %s", COMMAND_QUEUE_URL)
    logger.info("Payload: %s", json.dumps(payload))

    command = wrap_payload(payload)

    # MessageGroupId for a multi-target command uses a synthetic group key.
    # The real per-drone MessageGroupIds are set by the Processor when it
    # writes individual commands to the DispatchQueue.
    group_key = payload.get("target_id") or "multi"

    sqs = boto3.client("sqs")
    try:
        msg_id = _enqueue(sqs, COMMAND_QUEUE_URL, command, group_key)
        logger.info(
            "✓ Enqueued: command_id=%s MessageGroupId=%s SQS-MessageId=%s",
            command.command_id, group_key, msg_id,
        )
    except Exception as e:
        logger.error("✗ Failed to enqueue: %s", e)
        raise

    logger.info("=" * 60)
    logger.info("PRODUCER COMPLETE")
    logger.info("=" * 60)
    logger.info("")

    return command


# ──────────────────────────────────────────────────────────────────────────────
# ROLE: PROCESSOR
# ──────────────────────────────────────────────────────────────────────────────

def run_processor(sqs_records: list) -> Dict[str, Any]:
    """
    Processor role: validate + reason about commands, write output to DispatchQueue.

    Reads from: CommandQueue (one intent per message)
    Writes to:  DispatchQueue (one message per drone per command)

    For single-drone commands:  1 CommandQueue message → 1 DispatchQueue message
    For fan-out commands:       1 CommandQueue message → N DispatchQueue messages

    The Dispatcher never sees CommandQueue messages. It only ever reads from
    DispatchQueue, where every message is a single-drone command.

    Future reasoning steps (task decomposition, AI planning, rate limiting)
    all live here. They share the same contract with process():
        receive one CommandEnvelope → return one or many.
    """
    processed = []
    rejected  = []
    failed    = []

    logger.info("")
    logger.info("=" * 60)
    logger.info("=== ROLE: PROCESSOR ===")
    logger.info("=" * 60)
    logger.info("Processing %d CommandQueue message(s)", len(sqs_records))

    sqs = boto3.client("sqs")

    for record in sqs_records:
        try:
            body    = json.loads(record["body"])
            command = CommandEnvelope.from_dict(body)

            logger.info("Received command_id=%s", command.command_id)

            # process() returns CommandEnvelope (single) or List[CommandEnvelope] (fan-out)
            result = process(command)

            # Normalise to list so the enqueue loop is identical for both cases
            children: List[CommandEnvelope] = (
                result if isinstance(result, list) else [result]
            )

            logger.info(
                "Processor output: command_id=%s → %d dispatch envelope(s)",
                command.command_id, len(children),
            )

            # Write each child to DispatchQueue
            enqueue_failures = []
            for child in children:
                target_id = (
                    child.payload.get("target_id")
                    or child.payload.get("drone_id")
                    or "default"
                )
                try:
                    msg_id = _enqueue(sqs, DISPATCH_QUEUE_URL, child, target_id)
                    logger.info(
                        "  ✓ → DispatchQueue: command_id=%s target=%s SQS-MessageId=%s",
                        child.command_id, target_id, msg_id,
                    )
                    processed.append(child.command_id)
                except Exception as e:
                    logger.error(
                        "  ✗ Failed to enqueue to DispatchQueue: command_id=%s target=%s: %s",
                        child.command_id, target_id, e,
                    )
                    enqueue_failures.append(child.command_id)

            if enqueue_failures:
                # Partial fan-out failure — re-raise to retry the whole
                # CommandQueue message. Already-enqueued children are
                # idempotent (same command_id, SQS deduplication).
                raise RuntimeError(
                    f"Partial DispatchQueue enqueue failure: {enqueue_failures}"
                )

        except StructuralError as e:
            logger.warning("✗ Structural validation failed: %s", e)
            rejected.append({"command": record["body"], "reason": str(e)})

        except Exception as e:
            logger.error("✗ Processor error: %s", e, exc_info=True)
            failed.append({"command": record["body"], "reason": str(e)})
            raise  # SQS retries CommandQueue message

    logger.info("")
    logger.info("=" * 60)
    logger.info("PROCESSOR COMPLETE")
    logger.info("=" * 60)
    logger.info(
        "Summary: Processed=%d, Rejected=%d, Failed=%d",
        len(processed), len(rejected), len(failed),
    )
    logger.info("=" * 60)
    logger.info("")

    return {
        "processed":        len(processed),
        "rejected":         len(rejected),
        "failed":           len(failed),
        "rejected_details": rejected,
    }


# ──────────────────────────────────────────────────────────────────────────────
# ROLE: DISPATCHER
# ──────────────────────────────────────────────────────────────────────────────

def run_dispatcher(sqs_records: list, mqtt_adapter: MQTTAdapter) -> Dict[str, Any]:
    """
    Dispatcher role: translate commands to MQTT, update DynamoDB.

    Reads from: DispatchQueue (always single-drone commands)
    Writes to:  AWS IoT Core (MQTT)

    Rules:
    - No branching logic
    - No safety logic
    - No policy decisions
    - Given the same command, output is deterministic

    The Dispatcher never knows whether a command originated from a direct
    Producer invocation or from a fan-out expansion. It doesn't need to.
    """
    dispatched = []
    failed     = []

    logger.info("")
    logger.info("=" * 60)
    logger.info("=== ROLE: DISPATCHER ===")
    logger.info("=" * 60)
    logger.info("Dispatching %d DispatchQueue message(s)", len(sqs_records))

    for record in sqs_records:
        command = None
        try:
            body    = json.loads(record["body"])
            command = CommandEnvelope.from_dict(body)

            target_id = (
                command.payload.get("target_id")
                or command.payload.get("drone_id")
                or "default"
            )
            topic = f"drone/{target_id}/cmd"

            logger.info(
                "Dispatching command_id=%s target=%s topic=%s",
                command.command_id, target_id, topic,
            )

            dispatch(command, mqtt_adapter)
            dispatched.append(command.command_id)
            logger.info("✓ Published: command_id=%s → %s", command.command_id, topic)

            mark_published_to_iot(command.command_id)

        except DispatchError as e:
            logger.error("✗ Dispatch failed: %s", e)
            failed.append({
                "command_id": command.command_id if command else "unknown",
                "reason":     str(e),
            })
            raise  # SQS retries DispatchQueue message

        except ClientError as e:
            # IoT publish succeeded but DynamoDB write failed.
            # Re-raise to retry. Publish is idempotent (command_id, qos=1).
            logger.error(
                "✗ DynamoDB write failed after successful publish: command_id=%s",
                command.command_id if command else "unknown",
            )
            raise

        except Exception as e:
            logger.error("✗ Unexpected dispatcher error: %s", e, exc_info=True)
            raise

    logger.info("")
    logger.info("=" * 60)
    logger.info("DISPATCHER COMPLETE")
    logger.info("=" * 60)
    logger.info(
        "Summary: Dispatched=%d, Failed=%d",
        len(dispatched), len(failed),
    )
    logger.info("=" * 60)
    logger.info("")

    return {"dispatched": len(dispatched), "failed": len(failed)}


# ──────────────────────────────────────────────────────────────────────────────
# Lambda entry point
# ──────────────────────────────────────────────────────────────────────────────

def _source_queue(record: Dict[str, Any]) -> str:
    """
    Extract the source queue name from an SQS record's eventSourceARN.
    Used to distinguish CommandQueue from DispatchQueue triggers.
    """
    arn = record.get("eventSourceARN", "")
    # ARN format: arn:aws:sqs:<region>:<account>:<queue-name>
    return arn.split(":")[-1]


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda entry point — routes to correct role based on event source.

    Direct invocation  → PRODUCER
    SQS (CommandQueue) → PROCESSOR
    SQS (DispatchQueue)→ DISPATCHER
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("LAMBDA INVOKED")
    logger.info("=" * 60)
    logger.info("Request ID: %s", context.aws_request_id if context else "local")

    try:
        records = event.get("Records", [])

        if not records:
            # ── PRODUCER ──────────────────────────────────────────────
            payload = event.get("payload", event)
            command = produce_to_queue(payload)
            return {
                "statusCode": 200,
                "command_id": command.command_id,
                "created_at": command.created_at,
            }

        event_source = records[0].get("eventSource", "")

        if event_source == "aws:sqs":
            source_queue = _source_queue(records[0])
            logger.info("SQS trigger from queue: %s", source_queue)

            # Distinguish by queue name suffix
            if "DispatchQueue" in source_queue:
                # ── DISPATCHER ────────────────────────────────────────
                mqtt_adapter = MQTTAdapter(iot_endpoint=IOT_ENDPOINT)
                result = run_dispatcher(records, mqtt_adapter)
                return {"statusCode": 200, **result}
            else:
                # ── PROCESSOR (CommandQueue) ──────────────────────────
                result = run_processor(records)
                return {"statusCode": 200, **result}

        logger.warning("Unknown event source: %s", event_source)
        return {"statusCode": 400, "error": f"Unknown event source: {event_source}"}

    except Exception as e:
        logger.error("LAMBDA FAILED: %s", e, exc_info=True)
        return {"statusCode": 500, "error": str(e)}


logger.info("Lambda handler ready")
logger.info("=" * 60)