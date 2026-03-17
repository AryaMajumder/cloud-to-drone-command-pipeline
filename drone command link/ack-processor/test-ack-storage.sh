#!/bin/bash
################################################################################
# Test ACK Storage Infrastructure
# Sends test command and verifies ACK is stored in DynamoDB
################################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Configuration
AWS_REGION="us-east-1"
DYNAMODB_TABLE="DroneCommands"
COMMAND_LAMBDA="minimal-command-system"
ACK_LAMBDA="ack_processor"

echo ""
echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}Testing ACK Storage Infrastructure${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

################################################################################
# Step 1: Send Test Command
################################################################################
print_info "Sending test RTL command to drone-01..."

aws lambda invoke \
  --function-name "$COMMAND_LAMBDA" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"payload":{"target_id":"drone-01","action":"rtl"}}' \
  --region "$AWS_REGION" \
  /tmp/response.json \
  >/dev/null 2>&1

if [ $? -eq 0 ]; then
    print_success "Command sent successfully"
else
    print_error "Failed to send command"
    exit 1
fi

# Extract command ID
COMMAND_ID=$(cat /tmp/response.json | grep -o '"command_id":"[^"]*' | cut -d'"' -f4 || echo "")

if [ -z "$COMMAND_ID" ]; then
    print_error "Could not extract command_id from response"
    cat /tmp/response.json
    exit 1
fi

print_success "Command ID: $COMMAND_ID"

################################################################################
# Step 2: Wait for Command Execution
################################################################################
print_info "Waiting 10 seconds for command to be executed..."
for i in {10..1}; do
    echo -ne "  ${i}s remaining...\r"
    sleep 1
done
echo ""

################################################################################
# Step 3: Check DynamoDB for ACK
################################################################################
print_info "Querying DynamoDB for command status..."

RESULT=$(aws dynamodb get-item \
    --table-name "$DYNAMODB_TABLE" \
    --key "{\"command_id\": {\"S\": \"$COMMAND_ID\"}}" \
    --region "$AWS_REGION" \
    2>&1)

if [ $? -ne 0 ]; then
    print_error "Failed to query DynamoDB"
    echo "$RESULT"
    exit 1
fi

# Check if item exists
if echo "$RESULT" | grep -q '"command_id"'; then
    print_success "✓ Command found in DynamoDB!"
    
    # Extract status
    STATUS=$(echo "$RESULT" | grep -o '"status"[^}]*"S"[^}]*"[^"]*"' | grep -o '"[^"]*"$' | tr -d '"' || echo "unknown")
    
    print_info "Command Status: $STATUS"
    
    # Pretty print the item
    echo ""
    echo -e "${BLUE}Full DynamoDB Item:${NC}"
    echo "$RESULT" | jq -r '.Item' 2>/dev/null || echo "$RESULT"
    
    if [ "$STATUS" = "EXECUTED" ]; then
        print_success "✓✓✓ TEST PASSED! ACK was stored with status=EXECUTED"
    elif [ "$STATUS" = "ACK" ]; then
        print_info "Command acknowledged but may still be executing"
    else
        print_error "Unexpected status: $STATUS"
    fi
else
    print_error "✗ Command NOT found in DynamoDB"
    print_info "This could mean:"
    print_info "  1. ACK hasn't been sent yet (wait longer)"
    print_info "  2. ACK forwarder isn't running"
    print_info "  3. IoT Rule isn't triggering Lambda"
    print_info "  4. Lambda is failing to write to DynamoDB"
fi

################################################################################
# Step 4: Check Lambda Logs
################################################################################
echo ""
print_info "Recent ACK processor Lambda logs:"
echo ""

aws logs tail /aws/lambda/$ACK_LAMBDA \
    --since 2m \
    --format short \
    --region "$AWS_REGION" \
    2>/dev/null | tail -15 || print_info "No recent logs found"

################################################################################
# Step 5: Show All Recent Commands
################################################################################
echo ""
print_info "Recent commands in DynamoDB (last 5):"
echo ""

aws dynamodb scan \
    --table-name "$DYNAMODB_TABLE" \
    --max-items 5 \
    --region "$AWS_REGION" \
    --query 'Items[*].[command_id.S, status.S, updated_at.S]' \
    --output table 2>/dev/null || print_info "Could not scan table"

################################################################################
# Summary
################################################################################
echo ""
echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}Test Complete${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

cat << EOF
${BLUE}Verification Commands:${NC}

1. Check your specific command:
   ${YELLOW}aws dynamodb get-item \\
     --table-name $DYNAMODB_TABLE \\
     --key '{"command_id": {"S": "$COMMAND_ID"}}'${NC}

2. Watch Lambda logs in real-time:
   ${YELLOW}aws logs tail /aws/lambda/$ACK_LAMBDA --follow${NC}

3. See all commands:
   ${YELLOW}aws dynamodb scan --table-name $DYNAMODB_TABLE${NC}

4. Monitor IoT traffic (in AWS Console):
   ${YELLOW}IoT Core → Test → MQTT test client → Subscribe to: drone/+/cmd_ack${NC}

EOF

# Cleanup
rm -f /tmp/response.json

exit 0