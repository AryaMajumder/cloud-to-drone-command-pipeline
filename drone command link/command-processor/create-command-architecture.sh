#!/bin/bash
# Create Lambda from scratch - assumes code already in S3
# Skips packaging and uploading. Just creates infra + function.

set -e

# ==========================================
# Configuration
# ==========================================
FUNCTION_NAME="minimal-command-system"
REGION="us-east-1"
RUNTIME="python3.11"
HANDLER="lambda_function.lambda_handler"
TIMEOUT=60
MEMORY=256
S3_KEY="lambda.zip"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET_NAME="vghvdyrdyryrd"

echo "=== Create Lambda from S3 ==="
echo ""
echo "  Account:  $ACCOUNT_ID"
echo "  Bucket:   $BUCKET_NAME"
echo "  S3 Key:   $S3_KEY"
echo "  Function: $FUNCTION_NAME"
echo ""

# ==========================================
# Step 1: Verify code exists in S3
# ==========================================
echo "Step 1: Verifying code in S3..."

if aws s3 ls "s3://${BUCKET_NAME}/${S3_KEY}" > /dev/null 2>&1; then
    echo "  ✓ s3://${BUCKET_NAME}/${S3_KEY} exists"
else
    echo "  ✗ s3://${BUCKET_NAME}/${S3_KEY} NOT FOUND"
    echo ""
    echo "  Upload your code first:"
    echo "    zip lambda.zip lambda_function.py command_envelope.py processor.py fanout.py dispatcher.py"
    echo "    aws s3 cp lambda.zip s3://${BUCKET_NAME}/lambda.zip"
    exit 1
fi
echo ""

# ==========================================
# Step 2: Get IoT Endpoint
# ==========================================
echo "Step 2: Getting IoT endpoint..."
IOT_ENDPOINT=$(aws iot describe-endpoint \
    --endpoint-type iot:Data-ATS \
    --region $REGION \
    --query 'endpointAddress' \
    --output text)
echo "  ✓ IoT Endpoint: $IOT_ENDPOINT"
echo ""

# ==========================================
# Step 3: Create CommandQueue (intent bus)
# ==========================================
echo "Step 3: Creating CommandQueue..."

COMMAND_QUEUE_URL=$(aws sqs create-queue \
    --queue-name CommandQueue.fifo \
    --attributes FifoQueue=true,ContentBasedDeduplication=true,MessageRetentionPeriod=345600,VisibilityTimeout=60 \
    --region $REGION \
    --query 'QueueUrl' \
    --output text 2>/dev/null \
    || aws sqs get-queue-url \
        --queue-name CommandQueue.fifo \
        --region $REGION \
        --query 'QueueUrl' \
        --output text)

COMMAND_QUEUE_ARN=$(aws sqs get-queue-attributes \
    --queue-url $COMMAND_QUEUE_URL \
    --attribute-names QueueArn \
    --query 'Attributes.QueueArn' \
    --output text)

echo "  ✓ CommandQueue URL: $COMMAND_QUEUE_URL"
echo "  ✓ CommandQueue ARN: $COMMAND_QUEUE_ARN"
echo ""

# ==========================================
# Step 4: Create DispatchQueue (work queue)
# ==========================================
echo "Step 4: Creating DispatchQueue..."

DISPATCH_QUEUE_URL=$(aws sqs create-queue \
    --queue-name DispatchQueue.fifo \
    --attributes FifoQueue=true,ContentBasedDeduplication=true,MessageRetentionPeriod=345600,VisibilityTimeout=60 \
    --region $REGION \
    --query 'QueueUrl' \
    --output text 2>/dev/null \
    || aws sqs get-queue-url \
        --queue-name DispatchQueue.fifo \
        --region $REGION \
        --query 'QueueUrl' \
        --output text)

DISPATCH_QUEUE_ARN=$(aws sqs get-queue-attributes \
    --queue-url $DISPATCH_QUEUE_URL \
    --attribute-names QueueArn \
    --query 'Attributes.QueueArn' \
    --output text)

echo "  ✓ DispatchQueue URL: $DISPATCH_QUEUE_URL"
echo "  ✓ DispatchQueue ARN: $DISPATCH_QUEUE_ARN"
echo ""

# ==========================================
# Step 5: Create IAM Role
# ==========================================
echo "Step 5: Creating IAM role..."
ROLE_NAME="minimal-command-lambda-role"

cat > /tmp/trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

if aws iam create-role \
    --role-name $ROLE_NAME \
    --assume-role-policy-document file:///tmp/trust-policy.json \
    --region $REGION > /dev/null 2>&1; then
    echo "  ✓ Role created: $ROLE_NAME"
else
    echo "  ✓ Role already exists: $ROLE_NAME"
fi

aws iam attach-role-policy \
    --role-name $ROLE_NAME \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole \
    2>/dev/null || true

# Policy covers both queues
cat > /tmp/lambda-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CommandQueueAccess",
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "$COMMAND_QUEUE_ARN"
    },
    {
      "Sid": "DispatchQueueAccess",
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "$DISPATCH_QUEUE_ARN"
    },
    {
      "Sid": "IoTPublish",
      "Effect": "Allow",
      "Action": ["iot:Publish"],
      "Resource": "arn:aws:iot:$REGION:$ACCOUNT_ID:topic/drone/*"
    },
    {
      "Sid": "DynamoDBCommands",
      "Effect": "Allow",
      "Action": ["dynamodb:UpdateItem"],
      "Resource": "arn:aws:dynamodb:$REGION:$ACCOUNT_ID:table/DroneCommands"
    }
  ]
}
EOF

aws iam put-role-policy \
    --role-name $ROLE_NAME \
    --policy-name LambdaExecutionPolicy \
    --policy-document file:///tmp/lambda-policy.json

ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$ROLE_NAME"
echo "  ✓ Permissions set (CommandQueue + DispatchQueue + IoT + DynamoDB)"
echo ""

# ==========================================
# Step 6: Wait for IAM propagation
# ==========================================
echo "Step 6: Waiting for IAM role to propagate (10s)..."
sleep 10
echo "  ✓ Role ready"
echo ""

# ==========================================
# Step 7: Create or update Lambda function
# ==========================================
echo "Step 7: Creating Lambda function..."

if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION > /dev/null 2>&1; then
    echo "  Function already exists - updating code and config..."

    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --s3-bucket $BUCKET_NAME \
        --s3-key $S3_KEY \
        --region $REGION > /dev/null

    aws lambda wait function-updated \
        --function-name $FUNCTION_NAME \
        --region $REGION

    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --environment "Variables={COMMAND_QUEUE_URL=$COMMAND_QUEUE_URL,DISPATCH_QUEUE_URL=$DISPATCH_QUEUE_URL,IOT_ENDPOINT=$IOT_ENDPOINT,COMMANDS_TABLE=DroneCommands}" \
        --region $REGION > /dev/null

    aws lambda wait function-updated \
        --function-name $FUNCTION_NAME \
        --region $REGION

    echo "  ✓ Function updated: $FUNCTION_NAME"
else
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime $RUNTIME \
        --handler $HANDLER \
        --role $ROLE_ARN \
        --timeout $TIMEOUT \
        --memory-size $MEMORY \
        --environment "Variables={COMMAND_QUEUE_URL=$COMMAND_QUEUE_URL,DISPATCH_QUEUE_URL=$DISPATCH_QUEUE_URL,IOT_ENDPOINT=$IOT_ENDPOINT,COMMANDS_TABLE=DroneCommands}" \
        --code S3Bucket=$BUCKET_NAME,S3Key=$S3_KEY \
        --region $REGION > /dev/null

    echo "  ✓ Function created: $FUNCTION_NAME"
fi
echo ""

# ==========================================
# Step 8: Wait for Lambda to be active
# ==========================================
echo "Step 8: Waiting for Lambda to be active..."
aws lambda wait function-active \
    --function-name $FUNCTION_NAME \
    --region $REGION
echo "  ✓ Lambda active"
echo ""

# ==========================================
# Step 9: Create SQS trigger for CommandQueue
# ==========================================
echo "Step 9: Creating CommandQueue trigger..."

EXISTING_CMD_UUID=$(aws lambda list-event-source-mappings \
    --function-name $FUNCTION_NAME \
    --event-source-arn $COMMAND_QUEUE_ARN \
    --region $REGION \
    --query 'EventSourceMappings[0].UUID' \
    --output text 2>/dev/null)

if [ "$EXISTING_CMD_UUID" != "None" ] && [ -n "$EXISTING_CMD_UUID" ]; then
    echo "  ✓ CommandQueue trigger already exists: $EXISTING_CMD_UUID"
else
    CMD_UUID=$(aws lambda create-event-source-mapping \
        --function-name $FUNCTION_NAME \
        --event-source-arn $COMMAND_QUEUE_ARN \
        --batch-size 1 \
        --enabled \
        --region $REGION \
        --query 'UUID' \
        --output text)
    echo "  ✓ CommandQueue trigger created: $CMD_UUID"
fi
echo ""

# ==========================================
# Step 10: Create SQS trigger for DispatchQueue
# ==========================================
echo "Step 10: Creating DispatchQueue trigger..."

EXISTING_DISP_UUID=$(aws lambda list-event-source-mappings \
    --function-name $FUNCTION_NAME \
    --event-source-arn $DISPATCH_QUEUE_ARN \
    --region $REGION \
    --query 'EventSourceMappings[0].UUID' \
    --output text 2>/dev/null)

if [ "$EXISTING_DISP_UUID" != "None" ] && [ -n "$EXISTING_DISP_UUID" ]; then
    echo "  ✓ DispatchQueue trigger already exists: $EXISTING_DISP_UUID"
else
    DISP_UUID=$(aws lambda create-event-source-mapping \
        --function-name $FUNCTION_NAME \
        --event-source-arn $DISPATCH_QUEUE_ARN \
        --batch-size 1 \
        --enabled \
        --region $REGION \
        --query 'UUID' \
        --output text)
    echo "  ✓ DispatchQueue trigger created: $DISP_UUID"
fi
echo ""

# ==========================================
# Done
# ==========================================
echo "=== Setup Complete ==="
echo ""
echo "  Function:       $FUNCTION_NAME"
echo "  CommandQueue:   $COMMAND_QUEUE_URL"
echo "  DispatchQueue:  $DISPATCH_QUEUE_URL"
echo "  Role:           $ROLE_ARN"
echo "  Code:           s3://${BUCKET_NAME}/${S3_KEY}"
echo "  IoT:            $IOT_ENDPOINT"
echo ""
echo "Test (single drone):"
echo "  aws lambda invoke \\"
echo "    --function-name $FUNCTION_NAME \\"
echo "    --cli-binary-format raw-in-base64-out \\"
echo "    --payload '{\"payload\":{\"target_id\":\"drone-01\",\"action\":\"rtl\"}}' \\"
echo "    response.json && cat response.json"
echo ""
echo "Test (fan-out):"
echo "  aws lambda invoke \\"
echo "    --function-name $FUNCTION_NAME \\"
echo "    --cli-binary-format raw-in-base64-out \\"
echo "    --payload '{\"payload\":{\"action\":\"rtl\",\"target_ids\":[\"drone-01\",\"drone-02\"]}}' \\"
echo "    response.json && cat response.json"
echo ""
echo "Logs:"
echo "  aws logs tail /aws/lambda/$FUNCTION_NAME --follow"
echo ""

rm -f /tmp/trust-policy.json /tmp/lambda-policy.json