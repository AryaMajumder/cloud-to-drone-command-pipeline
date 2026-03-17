"""
ACK Processor Lambda Function
Processes drone command acknowledgements and updates DynamoDB
"""

import json
import boto3
import logging
from datetime import datetime
from decimal import Decimal

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('DroneCommands')


def decimal_default(obj):
    """JSON serializer for Decimal objects"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def lambda_handler(event, context):
    """
    Process ACK message from IoT Core.
    
    Expected event format (from IoT Rule):
    {
      "cmd_id": "abc-123",
      "drone_id": "drone-01",
      "status": "ACK" | "EXECUTED" | "NACK",
      "ts_utc": "2026-02-23T20:32:05Z",
      "exec_result": {...},  # Optional
      "reason": "..."        # Optional (for NACK)
    }
    """
    logger.info(f"Received ACK event: {json.dumps(event, default=str)}")
    
    # Extract fields
    cmd_id = event.get('cmd_id')
    drone_id = event.get('drone_id')
    status = event.get('status')
    timestamp = event.get('ts_utc', datetime.utcnow().isoformat() + 'Z')
    exec_result = event.get('exec_result')
    reason = event.get('reason')
    
    # Validate required fields
    if not cmd_id:
        logger.error("Missing cmd_id in ACK event")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Missing cmd_id'})
        }
    
    if not status:
        logger.error(f"Missing status for cmd_id: {cmd_id}")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Missing status'})
        }
    
    logger.info(f"Processing ACK: cmd_id={cmd_id}, status={status}")
    
    try:
        # Build update expression
        update_expr_parts = ['#status = :status', 'updated_at = :updated_at']
        expr_attr_names = {'#status': 'status'}
        expr_attr_values = {
            ':status': status,
            ':updated_at': timestamp
        }
        
        # Add optional fields
        if exec_result:
            update_expr_parts.append('exec_result = :result')
            expr_attr_values[':result'] = exec_result
        
        if reason:
            update_expr_parts.append('failure_reason = :reason')
            expr_attr_values[':reason'] = reason
        
        # Update or insert item
        response = table.update_item(
            Key={'command_id': cmd_id},
            UpdateExpression='SET ' + ', '.join(update_expr_parts),
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values,
            ReturnValues='ALL_NEW'
        )
        
        updated_item = response.get('Attributes', {})
        logger.info(f"✓ Updated DynamoDB for cmd_id={cmd_id}")
        logger.info(f"  Status: {status}")
        logger.info(f"  Item: {json.dumps(updated_item, default=decimal_default)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'command_id': cmd_id,
                'status': status,
                'updated': True,
                'item': updated_item
            }, default=decimal_default)
        }
        
    except Exception as e:
        logger.error(f"Error updating DynamoDB for cmd_id={cmd_id}: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'DynamoDB update failed',
                'command_id': cmd_id,
                'details': str(e)
            })
        }