"""
ack_processor Lambda — v2 (fixed)

Triggered by IoT Core rule on topic: drone/+/cmd_ack

What changed from v1 (the broken deploy-ack-storage version):
  - DynamoDB key is cmd_id  (NOT command_id — that was the original bug)
  - Status values: EXECUTED | FAILED | REJECTED  (agent uses these; old code used ACK|NACK)
  - COMMANDS_TABLE from env var (not hardcoded)
  - IAM role must include iot:Publish for cmd_confirmed topic (see deploy script)

Responsibilities:
  1. Parse ack payload from agent (via IoT Core)
  2. Upsert into DynamoDB Commands table by cmd_id (idempotent)
  3. Publish {confirmed_ids: [cmd_id]} to drone/<id>/cmd_confirmed
     so state_publisher stops replaying that command

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

COMMANDS_TABLE = os.environ["COMMANDS_TABLE"]   # must be set on Lambda config
AWS_REGION     = os.environ.get("AWS_REGION", "us-east-1")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table    = dynamodb.Table(COMMANDS_TABLE)

iot_data = boto3.client("iot-data", region_name=AWS_REGION)

# States that the agent actually emits (agent.py lines: publish_ack calls)
VALID_STATUSES = {"EXECUTED", "FAILED", "REJECTED"}


def handler(event, context):
    """
    IoT Core passes the MQTT payload directly as the event dict.

    Agent payload shape (from publish_ack in agent.py):
    {
        "cmd_id":      "cmd-1742000000-abc12345",
        "drone_id":    "drone-01",
        "status":      "EXECUTED" | "FAILED" | "REJECTED",
        "ts_utc":      "2026-03-21T10:00:00Z",
        "exec_result": null | {...},
        "reason":      null | "send_failed" | "unsupported cmd: XYZ"
    }

    The forwarder passes this through untouched, so the shape is identical
    here at the Lambda.
    """
    log.info("ack_processor received: %s", json.dumps(event))

    cmd_id      = event.get("cmd_id")
    drone_id    = event.get("drone_id")
    status      = event.get("status")
    reason      = event.get("reason")
    exec_result = event.get("exec_result")
    ts_utc      = event.get("ts_utc")          # agent's timestamp
    replayed    = event.get("replayed", False)

    # ── validation ────────────────────────────────────────────────────────────
    if not cmd_id or not drone_id or not status:
        log.error("Malformed ack payload — missing required fields: %s", event)
        return {"statusCode": 400, "body": "malformed payload"}

    if status not in VALID_STATUSES:
        # RECEIVED is normal on the agent side but should never reach the cloud
        # (agent only forwards EXECUTED/FAILED/REJECTED upstream).
        # Drop rather than corrupt DynamoDB with an unexpected state.
        log.error(
            "Unexpected status '%s' for cmd_id=%s — expected one of %s",
            status, cmd_id, VALID_STATUSES
        )
        return {"statusCode": 400, "body": f"unexpected status: {status}"}

    cloud_confirmed_at = datetime.now(timezone.utc).isoformat()

    # ── DynamoDB upsert ───────────────────────────────────────────────────────
    # update_item is idempotent: same payload arriving twice (e.g. from
    # state_publisher replay) just overwrites with identical values — no error.
    #
    # KEY: cmd_id  (string, partition key in DroneCommands table)
    # The old deploy script created the table with command_id as PK — that
    # table must be deleted and recreated. See redeploy-ack-processor.sh.
    try:
        table.update_item(
            Key={"cmd_id": cmd_id},
            UpdateExpression="""
                SET #st                = :status,
                    drone_id           = :drone_id,
                    reason             = :reason,
                    exec_result        = :exec_result,
                    updated_at         = :updated_at,
                    cloud_confirmed_at = :cloud_confirmed_at,
                    replayed           = :replayed
            """,
            ExpressionAttributeNames={
                "#st": "status"   # 'status' is a reserved word in DynamoDB
            },
            ExpressionAttributeValues={
                ":status":             status,
                ":drone_id":           drone_id,
                ":reason":             reason or "",
                ":exec_result":        json.dumps(exec_result) if exec_result else "",
                ":updated_at":         ts_utc or cloud_confirmed_at,
                ":cloud_confirmed_at": cloud_confirmed_at,
                ":replayed":           replayed,
            },
        )
        log.info(
            "DynamoDB updated — cmd_id=%s  status=%s  drone_id=%s  replayed=%s",
            cmd_id, status, drone_id, replayed
        )
    except Exception as e:
        log.error("DynamoDB update failed for cmd_id=%s: %s", cmd_id, e)
        raise   # let Lambda retry via IoT Rule retry policy

    # ── publish cmd_confirmed back to drone ───────────────────────────────────
    # state_publisher on the edge subscribes to this and marks
    # cloud_confirmed=1 so the command stops being replayed upstream.
    # IAM role must allow iot:Publish on drone/*/cmd_confirmed.
    confirm_topic   = f"drone/{drone_id}/cmd_confirmed"
    confirm_payload = json.dumps({"confirmed_ids": [cmd_id]})

    try:
        iot_data.publish(
            topic=confirm_topic,
            qos=1,
            payload=confirm_payload,
        )
        log.info(
            "Published cmd_confirmed — cmd_id=%s  topic=%s",
            cmd_id, confirm_topic
        )
    except Exception as e:
        # Non-fatal: DynamoDB write already succeeded.
        # state_publisher replays next cycle; Lambda will try again.
        log.warning(
            "Could not publish cmd_confirmed for cmd_id=%s: %s (non-fatal)",
            cmd_id, e
        )

    return {
        "statusCode": 200,
        "body": f"processed cmd_id={cmd_id} status={status}"
    }
