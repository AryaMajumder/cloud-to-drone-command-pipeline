"""
ack_processor Lambda — v3

Triggered by IoT Core rule on topic: drone/+/cmd_ack

Responsibilities:
  1. Validate payload — drop RECEIVED (edge-only state) and malformed messages
  2. Upsert into DynamoDB Commands table by cmd_id (idempotent)

Payload from agent / state_publisher:
  {
      "cmd_id":      "cmd-1742000000-abc12345",
      "drone_id":    "drone-01",
      "status":      "EXECUTED" | "FAILED" | "REJECTED",
      "ts_utc":      "2026-03-21T10:00:00Z",
      "exec_result": null | {"success": true, "detail": "RTL ACCEPTED"},
      "reason":      null | "send_failed" | "unsupported cmd: XYZ",
      "replayed":    false | true
  }

DynamoDB write notes:
  - exec_result stored as a native Map (nested object), not a JSON string.
    Querying/filtering on exec_result fields works correctly this way.
  - reason and exec_result are only written when present — avoids
    overwriting a previous good value with empty on a replayed message
    that lacks those fields.

Environment variables required:
  COMMANDS_TABLE  — DynamoDB table name  (e.g. "DroneCommands")
  AWS_REGION      — AWS region           (e.g. "us-east-1")
"""

import os
import json
import logging
import boto3
from datetime import datetime, timezone

log = logging.getLogger()
log.setLevel(logging.INFO)

COMMANDS_TABLE = os.environ["COMMANDS_TABLE"]

dynamodb = boto3.resource("dynamodb")
table    = dynamodb.Table(COMMANDS_TABLE)

# Only terminal states from the agent should reach the cloud.
# RECEIVED is edge-only — drop it if it somehow arrives.
VALID_STATUSES = {"EXECUTED", "FAILED", "REJECTED"}


def handler(event, context):
    log.info("ack_processor received: %s", json.dumps(event))

    cmd_id      = event.get("cmd_id")
    drone_id    = event.get("drone_id")
    status      = event.get("status")
    reason      = event.get("reason")
    exec_result = event.get("exec_result")   # already a dict from IoT Core
    ts_utc      = event.get("ts_utc")
    replayed    = event.get("replayed", False)

    # ── validation ────────────────────────────────────────────────────────────
    if not cmd_id or not drone_id or not status:
        log.error("Malformed ack payload — missing required fields: %s", event)
        return {"statusCode": 400, "body": "malformed payload"}

    if status not in VALID_STATUSES:
        log.warning(
            "Dropping status='%s' for cmd_id=%s — not a terminal state",
            status, cmd_id
        )
        return {"statusCode": 200, "body": f"dropped non-terminal status: {status}"}

    now = datetime.now(timezone.utc).isoformat()

    # ── build update expression ───────────────────────────────────────────────
    # Always write: status, drone_id, updated_at, cloud_received_at, replayed
    # Conditionally write: reason, exec_result (only when present in payload)
    # This avoids overwriting a previous exec_result with null on a bare replay.
    update_parts = [
        "#st                = :status",
        "drone_id           = :drone_id",
        "updated_at         = :updated_at",
        "cloud_received_at  = :cloud_received_at",
        "replayed           = :replayed",
    ]
    expr_values = {
        ":status":            status,
        ":drone_id":          drone_id,
        ":updated_at":        ts_utc or now,
        ":cloud_received_at": now,
        ":replayed":          replayed,
    }

    if reason:
        update_parts.append("reason = :reason")
        expr_values[":reason"] = reason

    if exec_result is not None:
        update_parts.append("exec_result = :exec_result")
        # exec_result arrives as a plain dict from IoT Core — store as-is.
        # DynamoDB boto3 resource layer serialises Python dicts to DynamoDB
        # Maps automatically. Do NOT json.dumps() it.
        expr_values[":exec_result"] = exec_result

    update_expression = "SET " + ",\n    ".join(update_parts)

    # ── DynamoDB upsert ───────────────────────────────────────────────────────
    try:
        table.update_item(
            Key={"cmd_id": cmd_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=expr_values,
        )
        log.info(
            "DynamoDB updated — cmd_id=%s  status=%s  drone_id=%s  replayed=%s",
            cmd_id, status, drone_id, replayed
        )
    except Exception as e:
        log.error("DynamoDB update failed for cmd_id=%s: %s", cmd_id, e)
        raise   # let Lambda retry

    return {
        "statusCode": 200,
        "body": f"processed cmd_id={cmd_id} status={status}"
    }
