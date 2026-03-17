#!/bin/bash
################################################################################
# Deploy ACK Storage Infrastructure
# Creates: DynamoDB table, IAM role, Lambda function, IoT Rule
################################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""
}

print_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Configuration
AWS_REGION="us-east-1"
DYNAMODB_TABLE="DroneCommands"
IAM_ROLE_NAME="lambda-drone-ack-processor-role"
LAMBDA_FUNCTION_NAME="ack_processor"
IOT_RULE_NAME="drone_ack_processor"

# Get AWS Account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
print_info "AWS Account ID: $AWS_ACCOUNT_ID"
print_info "Region: $AWS_REGION"

################################################################################
# Step 1: Create DynamoDB Table
################################################################################
print_header "Step 1: Creating DynamoDB Table"

if aws dynamodb describe-table --table-name "$DYNAMODB_TABLE" --region "$AWS_REGION" &>/dev/null; then
    print_warning "DynamoDB table '$DYNAMODB_TABLE' already exists"
else
    print_info "Creating DynamoDB table: $DYNAMODB_TABLE"
    
    aws dynamodb create-table \
        --table-name "$DYNAMODB_TABLE" \
        --attribute-definitions \
            AttributeName=command_id,AttributeType=S \
        --key-schema \
            AttributeName=command_id,KeyType=HASH \
        --billing-mode PAY_PER_REQUEST \
        --region "$AWS_REGION" \
        --tags Key=Project,Value=DroneC2 Key=Component,Value=ACKStorage
    
    print_info "Waiting for table to become active..."
    aws dynamodb wait table-exists --table-name "$DYNAMODB_TABLE" --region "$AWS_REGION"
    
    print_success "DynamoDB table created: $DYNAMODB_TABLE"
fi

# Describe table
TABLE_ARN=$(aws dynamodb describe-table \
    --table-name "$DYNAMODB_TABLE" \
    --region "$AWS_REGION" \
    --query 'Table.TableArn' \
    --output text)

print_info "Table ARN: $TABLE_ARN"

################################################################################
# Step 2: Create IAM Role for Lambda
################################################################################
print_header "Step 2: Creating IAM Role for Lambda"

# Create trust policy
cat > /tmp/lambda-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Check if role exists
if aws iam get-role --role-name "$IAM_ROLE_NAME" &>/dev/null; then
    print_warning "IAM role '$IAM_ROLE_NAME' already exists"
else
    print_info "Creating IAM role: $IAM_ROLE_NAME"
    
    aws iam create-role \
        --role-name "$IAM_ROLE_NAME" \
        --assume-role-policy-document file:///tmp/lambda-trust-policy.json \
        --description "Role for drone ACK processor Lambda to access DynamoDB" \
        --tags Key=Project,Value=DroneC2
    
    print_success "IAM role created: $IAM_ROLE_NAME"
fi

ROLE_ARN=$(aws iam get-role --role-name "$IAM_ROLE_NAME" --query 'Role.Arn' --output text)
print_info "Role ARN: $ROLE_ARN"

# Attach policies
print_info "Attaching policies to role..."

# CloudWatch Logs policy
aws iam attach-role-policy \
    --role-name "$IAM_ROLE_NAME" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" \
    2>/dev/null || print_warning "CloudWatch policy already attached"

# DynamoDB policy
cat > /tmp/dynamodb-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:GetItem",
        "dynamodb:Query"
      ],
      "Resource": "$TABLE_ARN"
    }
  ]
}
EOF

POLICY_NAME="drone-ack-dynamodb-access"

# Check if policy exists
if aws iam get-policy --policy-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME}" &>/dev/null; then
    print_warning "Policy '$POLICY_NAME' already exists"
else
    print_info "Creating DynamoDB access policy..."
    
    aws iam create-policy \
        --policy-name "$POLICY_NAME" \
        --policy-document file:///tmp/dynamodb-policy.json \
        --description "Allows ACK processor to write to DroneCommands table"
    
    print_success "Policy created: $POLICY_NAME"
fi

# Attach custom policy
aws iam attach-role-policy \
    --role-name "$IAM_ROLE_NAME" \
    --policy-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME}" \
    2>/dev/null || print_warning "DynamoDB policy already attached"

print_success "Policies attached to role"

# Wait for role propagation
print_info "Waiting 10 seconds for IAM role to propagate..."
sleep 10

################################################################################
# Step 3: Create Lambda Function
################################################################################
print_header "Step 3: Creating Lambda Function"

# Create deployment package
print_info "Creating Lambda deployment package..."

LAMBDA_DIR="/tmp/lambda-ack-processor"
mkdir -p "$LAMBDA_DIR"

# Copy Lambda code
cat > "$LAMBDA_DIR/ack_processor.py" << 'LAMBDA_CODE'
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
LAMBDA_CODE

# Create ZIP package
cd "$LAMBDA_DIR"
zip -q ack_processor.zip ack_processor.py
print_success "Lambda package created: $LAMBDA_DIR/ack_processor.zip"

# Check if Lambda function exists
if aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" &>/dev/null; then
    print_warning "Lambda function '$LAMBDA_FUNCTION_NAME' already exists, updating code..."
    
    aws lambda update-function-code \
        --function-name "$LAMBDA_FUNCTION_NAME" \
        --zip-file fileb://ack_processor.zip \
        --region "$AWS_REGION" \
        --output text
    
    print_success "Lambda function code updated"
else
    print_info "Creating Lambda function: $LAMBDA_FUNCTION_NAME"
    
    aws lambda create-function \
        --function-name "$LAMBDA_FUNCTION_NAME" \
        --runtime python3.11 \
        --role "$ROLE_ARN" \
        --handler ack_processor.lambda_handler \
        --zip-file fileb://ack_processor.zip \
        --timeout 10 \
        --memory-size 128 \
        --region "$AWS_REGION" \
        --description "Processes drone command ACKs and updates DynamoDB" \
        --tags Project=DroneC2,Component=ACKProcessor
    
    print_success "Lambda function created: $LAMBDA_FUNCTION_NAME"
fi

LAMBDA_ARN=$(aws lambda get-function \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --region "$AWS_REGION" \
    --query 'Configuration.FunctionArn' \
    --output text)

print_info "Lambda ARN: $LAMBDA_ARN"

################################################################################
# Step 4: Create IoT Rule
################################################################################
print_header "Step 4: Creating IoT Rule"

# Check if rule exists
if aws iot get-topic-rule --rule-name "$IOT_RULE_NAME" --region "$AWS_REGION" &>/dev/null; then
    print_warning "IoT Rule '$IOT_RULE_NAME' already exists, deleting and recreating..."
    aws iot delete-topic-rule --rule-name "$IOT_RULE_NAME" --region "$AWS_REGION"
fi

print_info "Creating IoT Rule: $IOT_RULE_NAME"

# Create rule payload
cat > /tmp/iot-rule.json << EOF
{
  "sql": "SELECT * FROM 'drone/+/cmd_ack'",
  "description": "Route drone command ACKs to processor Lambda",
  "actions": [
    {
      "lambda": {
        "functionArn": "$LAMBDA_ARN"
      }
    }
  ],
  "ruleDisabled": false,
  "awsIotSqlVersion": "2016-03-23"
}
EOF

aws iot create-topic-rule \
    --rule-name "$IOT_RULE_NAME" \
    --topic-rule-payload file:///tmp/iot-rule.json \
    --region "$AWS_REGION"

print_success "IoT Rule created: $IOT_RULE_NAME"

################################################################################
# Step 5: Grant IoT Permission to Invoke Lambda
################################################################################
print_header "Step 5: Granting IoT Permission to Invoke Lambda"

# Remove existing permission if it exists
aws lambda remove-permission \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --statement-id iot-invoke \
    --region "$AWS_REGION" \
    2>/dev/null || true

print_info "Adding Lambda permission for IoT..."

aws lambda add-permission \
    --function-name "$LAMBDA_FUNCTION_NAME" \
    --statement-id iot-invoke \
    --action lambda:InvokeFunction \
    --principal iot.amazonaws.com \
    --source-arn "arn:aws:iot:${AWS_REGION}:${AWS_ACCOUNT_ID}:rule/${IOT_RULE_NAME}" \
    --region "$AWS_REGION"

print_success "Permission granted to IoT Rule to invoke Lambda"

################################################################################
# Step 6: Verification
################################################################################
print_header "Step 6: Verification"

print_info "Verifying deployment..."

# Check DynamoDB
if aws dynamodb describe-table --table-name "$DYNAMODB_TABLE" --region "$AWS_REGION" &>/dev/null; then
    print_success "✓ DynamoDB table exists: $DYNAMODB_TABLE"
else
    print_error "✗ DynamoDB table not found"
fi

# Check IAM Role
if aws iam get-role --role-name "$IAM_ROLE_NAME" &>/dev/null; then
    print_success "✓ IAM role exists: $IAM_ROLE_NAME"
else
    print_error "✗ IAM role not found"
fi

# Check Lambda
if aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" --region "$AWS_REGION" &>/dev/null; then
    print_success "✓ Lambda function exists: $LAMBDA_FUNCTION_NAME"
else
    print_error "✗ Lambda function not found"
fi

# Check IoT Rule
if aws iot get-topic-rule --rule-name "$IOT_RULE_NAME" --region "$AWS_REGION" &>/dev/null; then
    print_success "✓ IoT Rule exists: $IOT_RULE_NAME"
else
    print_error "✗ IoT Rule not found"
fi

################################################################################
# Summary
################################################################################
print_header "Deployment Complete!"

cat << EOF

${GREEN}✓ All components deployed successfully!${NC}

${BLUE}Resources Created:${NC}
  DynamoDB Table:    $DYNAMODB_TABLE
  IAM Role:          $IAM_ROLE_NAME
  Lambda Function:   $LAMBDA_FUNCTION_NAME
  IoT Rule:          $IOT_RULE_NAME

${BLUE}ARNs:${NC}
  Table:    $TABLE_ARN
  Role:     $ROLE_ARN
  Lambda:   $LAMBDA_ARN

${BLUE}Flow:${NC}
  Agent → Mosquitto → ACK Forwarder → IoT Core
    ↓
  IoT Rule: drone/+/cmd_ack
    ↓
  Lambda: $LAMBDA_FUNCTION_NAME
    ↓
  DynamoDB: $DYNAMODB_TABLE

${BLUE}Next Steps:${NC}

1. Test the flow:
   ${YELLOW}aws lambda invoke \\
     --function-name minimal-command-system \\
     --cli-binary-format raw-in-base64-out \\
     --payload '{"payload":{"target_id":"drone-01","action":"rtl"}}' \\
     response.json${NC}

2. Check Lambda logs:
   ${YELLOW}aws logs tail /aws/lambda/$LAMBDA_FUNCTION_NAME --follow${NC}

3. Query DynamoDB:
   ${YELLOW}aws dynamodb scan --table-name $DYNAMODB_TABLE --max-items 5${NC}

4. Monitor IoT Rule:
   ${YELLOW}aws iot get-topic-rule --rule-name $IOT_RULE_NAME${NC}

${GREEN}ACK storage infrastructure is ready!${NC}

EOF

# Cleanup temp files
rm -rf /tmp/lambda-ack-processor
rm -f /tmp/lambda-trust-policy.json /tmp/dynamodb-policy.json /tmp/iot-rule.json

exit 0