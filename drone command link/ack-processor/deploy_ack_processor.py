#!/bin/bash
################################################################################
# deploy.sh — Deploy ack_processor Lambda + supporting infrastructure
#
# Packages ack_processor.py from THIS directory and deploys it.
# No inline code. No surprises.
#
# Run from the directory containing ack_processor.py:
#   cd ~/ack-storage
#   bash deploy.sh
#
# What this script does:
#   1. Recreates DynamoDB table with correct PK (cmd_id)
#      — prompts before deleting if old table exists with wrong PK
#   2. Creates/updates IAM role with correct permissions
#   3. Packages ack_processor.py from disk and deploys to Lambda
#   4. Creates IoT Rule  drone/+/cmd_ack → Lambda
#   5. Verifies everything is wired correctly
################################################################################

set -e

# ── colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC}    $1"; }
ok()      { echo -e "${GREEN}[OK]${NC}      $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}    $1"; }
die()     { echo -e "${RED}[ERROR]${NC}   $1"; exit 1; }
header()  { echo -e "\n${BLUE}══════════════════════════════════════════════${NC}\n  $1\n${BLUE}══════════════════════════════════════════════${NC}\n"; }

# ── config ────────────────────────────────────────────────────────────────────
AWS_REGION="us-east-1"
TABLE_NAME="DroneCommands"
ROLE_NAME="lambda-ack-processor-role"
LAMBDA_NAME="ack_processor"
IOT_RULE_NAME="drone_ack_processor"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LAMBDA_SRC="$SCRIPT_DIR/ack_processor.py"

# ── preflight ─────────────────────────────────────────────────────────────────
header "Pre-flight"

[ -f "$LAMBDA_SRC" ] || die "ack_processor.py not found in $SCRIPT_DIR"
ok "ack_processor.py found"

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
info "Account : $AWS_ACCOUNT_ID"
info "Region  : $AWS_REGION"
info "Table   : $TABLE_NAME  (PK = cmd_id)"
info "Lambda  : $LAMBDA_NAME  (handler = ack_processor.handler)"

################################################################################
# 1. DynamoDB
################################################################################
header "1 — DynamoDB table"

NEEDS_CREATE=true

if aws dynamodb describe-table --table-name "$TABLE_NAME" --region "$AWS_REGION" &>/dev/null; then
    CURRENT_PK=$(aws dynamodb describe-table \
        --table-name "$TABLE_NAME" --region "$AWS_REGION" \
        --query 'Table.KeySchema[?KeyType==`HASH`].AttributeName' \
        --output text)

    if [ "$CURRENT_PK" = "cmd_id" ]; then
        ok "Table exists with correct PK=cmd_id — skipping recreation"
        NEEDS_CREATE=false
    else
        warn "Table exists with PK='$CURRENT_PK' — must recreate with PK=cmd_id"
        warn "This will DELETE the table and all existing rows."
        echo ""
        read -p "  Type 'yes' to delete and recreate: " CONFIRM
        [ "$CONFIRM" = "yes" ] || die "Aborted by user"

        info "Deleting table ..."
        aws dynamodb delete-table --table-name "$TABLE_NAME" --region "$AWS_REGION" --output text >/dev/null
        aws dynamodb wait table-not-exists --table-name "$TABLE_NAME" --region "$AWS_REGION"
        ok "Old table deleted"
    fi
fi

if $NEEDS_CREATE; then
    info "Creating table $TABLE_NAME with PK=cmd_id ..."
    aws dynamodb create-table \
        --table-name "$TABLE_NAME" \
        --attribute-definitions \
            AttributeName=cmd_id,AttributeType=S \
            AttributeName=drone_id,AttributeType=S \
            AttributeName=created_at,AttributeType=S \
        --key-schema AttributeName=cmd_id,KeyType=HASH \
        --billing-mode PAY_PER_REQUEST \
        --global-secondary-indexes '[
            {
                "IndexName":"DroneIdIndex",
                "KeySchema":[
                    {"AttributeName":"drone_id","KeyType":"HASH"},
                    {"AttributeName":"created_at","KeyType":"RANGE"}
                ],
                "Projection":{"ProjectionType":"ALL"}
            }
        ]' \
        --region "$AWS_REGION" \
        --tags Key=Project,Value=DroneC2 \
        --output text >/dev/null

    info "Waiting for table to become ACTIVE ..."
    aws dynamodb wait table-exists --table-name "$TABLE_NAME" --region "$AWS_REGION"
    ok "Table created"
fi

TABLE_ARN=$(aws dynamodb describe-table \
    --table-name "$TABLE_NAME" --region "$AWS_REGION" \
    --query 'Table.TableArn' --output text)
info "Table ARN: $TABLE_ARN"

################################################################################
# 2. IAM role
################################################################################
header "2 — IAM role"

TRUST_POLICY='{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Principal":{"Service":"lambda.amazonaws.com"},
    "Action":"sts:AssumeRole"
  }]
}'

if ! aws iam get-role --role-name "$ROLE_NAME" &>/dev/null; then
    info "Creating IAM role $ROLE_NAME ..."
    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document "$TRUST_POLICY" \
        --output text >/dev/null
    ok "Role created"
else
    ok "Role $ROLE_NAME already exists"
fi

ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text)
info "Role ARN: $ROLE_ARN"

# CloudWatch Logs (basic execution)
aws iam attach-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" \
    2>/dev/null || true

# DynamoDB access — inline policy (replaces any previous version)
aws iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name "ack-processor-dynamodb" \
    --policy-document "{
      \"Version\":\"2012-10-17\",
      \"Statement\":[{
        \"Effect\":\"Allow\",
        \"Action\":[
          \"dynamodb:UpdateItem\",
          \"dynamodb:PutItem\",
          \"dynamodb:GetItem\",
          \"dynamodb:Query\"
        ],
        \"Resource\":[
          \"$TABLE_ARN\",
          \"${TABLE_ARN}/index/*\"
        ]
      }]
    }"

ok "IAM policies applied"

# Wait for role to propagate before Lambda create
info "Waiting 10s for IAM propagation ..."
sleep 10

################################################################################
# 3. Lambda — package from disk and deploy
################################################################################
header "3 — Lambda function"

# Build zip from the actual file on disk
BUILD_DIR=$(mktemp -d)
cp "$LAMBDA_SRC" "$BUILD_DIR/ack_processor.py"
cd "$BUILD_DIR"
zip -q ack_processor.zip ack_processor.py
info "Packaged $LAMBDA_SRC → ack_processor.zip"

if aws lambda get-function --function-name "$LAMBDA_NAME" --region "$AWS_REGION" &>/dev/null; then
    info "Updating Lambda code ..."
    aws lambda update-function-code \
        --function-name "$LAMBDA_NAME" \
        --zip-file fileb://ack_processor.zip \
        --region "$AWS_REGION" \
        --output text >/dev/null

    aws lambda wait function-updated \
        --function-name "$LAMBDA_NAME" \
        --region "$AWS_REGION"

    info "Updating Lambda configuration ..."
    aws lambda update-function-configuration \
        --function-name "$LAMBDA_NAME" \
        --handler "ack_processor.handler" \
        --runtime python3.11 \
        --timeout 15 \
        --environment "Variables={COMMANDS_TABLE=$TABLE_NAME}" \
        --region "$AWS_REGION" \
        --output text >/dev/null

    aws lambda wait function-updated \
        --function-name "$LAMBDA_NAME" \
        --region "$AWS_REGION"

    ok "Lambda code + config updated"
else
    info "Creating Lambda function ..."
    aws lambda create-function \
        --function-name "$LAMBDA_NAME" \
        --runtime python3.11 \
        --role "$ROLE_ARN" \
        --handler "ack_processor.handler" \
        --zip-file fileb://ack_processor.zip \
        --timeout 15 \
        --memory-size 128 \
        --environment "Variables={COMMANDS_TABLE=$TABLE_NAME}" \
        --region "$AWS_REGION" \
        --output text >/dev/null

    ok "Lambda function created"
fi

LAMBDA_ARN=$(aws lambda get-function \
    --function-name "$LAMBDA_NAME" --region "$AWS_REGION" \
    --query 'Configuration.FunctionArn' --output text)
info "Lambda ARN: $LAMBDA_ARN"

cd /; rm -rf "$BUILD_DIR"

################################################################################
# 4. IoT Rule
################################################################################
header "4 — IoT Rule"

# Always recreate to ensure it points at the current Lambda ARN
if aws iot get-topic-rule --rule-name "$IOT_RULE_NAME" --region "$AWS_REGION" &>/dev/null; then
    info "Removing existing IoT Rule ..."
    aws iot delete-topic-rule --rule-name "$IOT_RULE_NAME" --region "$AWS_REGION"
fi

aws iot create-topic-rule \
    --rule-name "$IOT_RULE_NAME" \
    --topic-rule-payload "{
      \"sql\":\"SELECT * FROM 'drone/+/cmd_ack'\",
      \"description\":\"Forward drone ACKs to ack_processor Lambda\",
      \"actions\":[{\"lambda\":{\"functionArn\":\"$LAMBDA_ARN\"}}],
      \"ruleDisabled\":false,
      \"awsIotSqlVersion\":\"2016-03-23\"
    }" \
    --region "$AWS_REGION"

ok "IoT Rule created: drone/+/cmd_ack → $LAMBDA_NAME"

# Grant IoT permission to invoke Lambda
aws lambda remove-permission \
    --function-name "$LAMBDA_NAME" \
    --statement-id iot-invoke \
    --region "$AWS_REGION" 2>/dev/null || true

aws lambda add-permission \
    --function-name "$LAMBDA_NAME" \
    --statement-id iot-invoke \
    --action lambda:InvokeFunction \
    --principal iot.amazonaws.com \
    --source-arn "arn:aws:iot:${AWS_REGION}:${AWS_ACCOUNT_ID}:rule/${IOT_RULE_NAME}" \
    --region "$AWS_REGION" \
    --output text >/dev/null

ok "IoT invoke permission granted"

################################################################################
# 5. Verify
################################################################################
header "5 — Verification"

PASS=0; FAIL=0
check() {
    local label="$1"; local cmd="$2"
    if eval "$cmd" &>/dev/null; then ok "$label"; PASS=$((PASS+1))
    else echo -e "${RED}[FAIL]${NC}    $label"; FAIL=$((FAIL+1)); fi
}

check "DynamoDB table exists" \
    "aws dynamodb describe-table --table-name $TABLE_NAME --region $AWS_REGION"

check "DynamoDB PK = cmd_id" \
    "aws dynamodb describe-table --table-name $TABLE_NAME --region $AWS_REGION \
     --query 'Table.KeySchema[?KeyType==\`HASH\`].AttributeName' \
     --output text | grep -q '^cmd_id$'"

check "Lambda handler = ack_processor.handler" \
    "aws lambda get-function-configuration \
     --function-name $LAMBDA_NAME --region $AWS_REGION \
     --query 'Handler' --output text | grep -q 'ack_processor.handler'"

check "Lambda COMMANDS_TABLE env var = $TABLE_NAME" \
    "aws lambda get-function-configuration \
     --function-name $LAMBDA_NAME --region $AWS_REGION \
     --query 'Environment.Variables.COMMANDS_TABLE' \
     --output text | grep -q '$TABLE_NAME'"

check "IoT Rule exists" \
    "aws iot get-topic-rule --rule-name $IOT_RULE_NAME --region $AWS_REGION"

echo ""
echo -e "Results: ${GREEN}${PASS} passed${NC}  ${RED}${FAIL} failed${NC}"
[ "$FAIL" -gt 0 ] && warn "Fix failures above before testing"

################################################################################
# Done
################################################################################
header "Done"

cat <<EOF
${GREEN}Deployment complete.${NC}

Smoke test (inject a fake ACK directly into Lambda):

  aws lambda invoke \\
    --function-name $LAMBDA_NAME \\
    --cli-binary-format raw-in-base64-out \\
    --payload '{"cmd_id":"smoke-001","drone_id":"drone-01","status":"EXECUTED","ts_utc":"2026-03-21T10:00:00Z","exec_result":{"success":true,"detail":"RTL ACCEPTED"}}' \\
    --region $AWS_REGION \\
    /tmp/smoke.json && cat /tmp/smoke.json

Check DynamoDB for the test row:

  aws dynamodb get-item \\
    --table-name $TABLE_NAME \\
    --key '{"cmd_id":{"S":"smoke-001"}}' \\
    --region $AWS_REGION

Watch logs live:

  aws logs tail /aws/lambda/$LAMBDA_NAME --follow --region $AWS_REGION

EOF
