#!/bin/bash
#
# Send Drone Commands - Simple CLI tool
# Usage: ./send-command.sh RTL drone-01
#

set -e

# Configuration
STACK_NAME="${STACK_NAME:-drone-c2-command-pipeline}"
AWS_REGION="${AWS_REGION:-us-east-1}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Usage
if [ $# -lt 2 ]; then
    echo "Usage: $0 <COMMAND> <DRONE_ID> [PARAMS]"
    echo ""
    echo "Commands:"
    echo "  RTL                 - Return to Launch"
    echo "  LAND                - Land immediately"
    echo "  LOITER              - Hold position"
    echo "  ARM                 - Arm motors"
    echo "  DISARM              - Disarm motors"
    echo "  SET_MODE <mode>     - Set flight mode"
    echo ""
    echo "Examples:"
    echo "  $0 RTL drone-01"
    echo "  $0 LAND drone-01"
    echo "  $0 SET_MODE drone-01 '{\"mode\":\"GUIDED\"}'"
    exit 1
fi

COMMAND=$1
DRONE_ID=$2
PARAMS=${3:-"{}"}

echo -e "${YELLOW}Sending command to drone...${NC}"
echo "  Command: $COMMAND"
echo "  Drone ID: $DRONE_ID"
echo "  Params: $PARAMS"
echo ""

# Get SQS queue URL
echo -e "${YELLOW}Getting queue URL...${NC}"
QUEUE_URL=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query 'Stacks[0].Outputs[?OutputKey==`CommandQueueUrl`].OutputValue' \
    --output text)

if [ -z "$QUEUE_URL" ]; then
    echo -e "${RED}Error: Could not get queue URL${NC}"
    echo "Check that stack $STACK_NAME exists"
    exit 1
fi

echo -e "${GREEN}✓ Queue URL: $QUEUE_URL${NC}"
echo ""

# Generate unique IDs
CMD_ID="cmd-$(date +%s)-$$"
DEDUP_ID="dedup-$(date +%s)-$$"
TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# Build message
MESSAGE=$(cat <<EOF
{
    "cmd_id": "$CMD_ID",
    "cmd": "$COMMAND",
    "drone_id": "$DRONE_ID",
    "params": $PARAMS,
    "timestamp": "$TIMESTAMP"
}
EOF
)

echo -e "${YELLOW}Sending message to SQS...${NC}"

# Send to SQS
RESULT=$(aws sqs send-message \
    --queue-url "$QUEUE_URL" \
    --message-body "$MESSAGE" \
    --message-group-id "$DRONE_ID" \
    --message-deduplication-id "$DEDUP_ID" \
    --region "$AWS_REGION" \
    --output json)

MESSAGE_ID=$(echo "$RESULT" | jq -r '.MessageId')

if [ -n "$MESSAGE_ID" ]; then
    echo -e "${GREEN}✓ Command sent successfully!${NC}"
    echo ""
    echo "Details:"
    echo "  Command ID: $CMD_ID"
    echo "  Message ID: $MESSAGE_ID"
    echo ""
    echo "Track status:"
    echo "  ./check-command-status.sh $CMD_ID"
else
    echo -e "${RED}✗ Failed to send command${NC}"
    exit 1
fi