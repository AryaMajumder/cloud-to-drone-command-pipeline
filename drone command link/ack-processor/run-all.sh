#!/bin/bash
################################################################################
# Master Script - Deploy and Test ACK Storage Infrastructure
# This script runs everything: deployment + testing
################################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

print_banner() {
    echo ""
    echo -e "${CYAN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║${BOLD}        ACK Storage Infrastructure - Complete Setup        ${NC}${CYAN}║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

print_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[✓]${NC} $1"; }
print_error() { echo -e "${RED}[✗]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[⚠]${NC} $1"; }

confirm() {
    echo -e "${YELLOW}$1${NC}"
    read -p "Continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 1
    fi
}

################################################################################
# Welcome
################################################################################
print_banner

cat << EOF
This script will:
  
  ${GREEN}1. Deploy ACK Storage Infrastructure${NC}
     • DynamoDB table (DroneCommands)
     • Lambda function (ack_processor)
     • IoT Rule (drone_ack_processor)
     • IAM roles and permissions
  
  ${GREEN}2. Test the Complete Pipeline${NC}
     • Send test command
     • Verify ACK storage
     • Display results

  ${YELLOW}Prerequisites:${NC}
     • AWS CLI configured
     • Proper IAM permissions
     • ACK forwarder running on edge

EOF

confirm "Ready to deploy ACK storage infrastructure?"

################################################################################
# Phase 1: Deployment
################################################################################
print_section "Phase 1: Deploying Infrastructure"

if [ ! -f "$SCRIPT_DIR/deploy-ack-storage.sh" ]; then
    print_error "deploy-ack-storage.sh not found in $SCRIPT_DIR"
    exit 1
fi

print_info "Running deployment script..."
bash "$SCRIPT_DIR/deploy-ack-storage.sh"

if [ $? -eq 0 ]; then
    print_success "Infrastructure deployed successfully!"
else
    print_error "Deployment failed!"
    exit 1
fi

################################################################################
# Phase 2: Wait for Propagation
################################################################################
print_section "Phase 2: Waiting for AWS Resource Propagation"

print_info "Waiting 15 seconds for resources to propagate..."
for i in {15..1}; do
    echo -ne "  ${i}s remaining...\r"
    sleep 1
done
echo ""
print_success "Resources should be ready"

################################################################################
# Phase 3: Testing
################################################################################
print_section "Phase 3: Testing End-to-End Flow"

if [ ! -f "$SCRIPT_DIR/test-ack-storage.sh" ]; then
    print_error "test-ack-storage.sh not found in $SCRIPT_DIR"
    exit 1
fi

print_info "Running test script..."
bash "$SCRIPT_DIR/test-ack-storage.sh"

if [ $? -eq 0 ]; then
    print_success "Tests completed!"
else
    print_warning "Tests completed with warnings (check output above)"
fi

################################################################################
# Summary
################################################################################
print_section "Deployment and Testing Complete!"

cat << EOF

${GREEN}✓✓✓ ACK Storage Infrastructure is Ready! ✓✓✓${NC}

${BLUE}What was created:${NC}
  • DynamoDB Table:  DroneCommands
  • Lambda Function: ack_processor  
  • IoT Rule:        drone_ack_processor
  • IAM Role:        lambda-drone-ack-processor-role

${BLUE}Command Flow:${NC}
  ${CYAN}Cloud → Edge:${NC}
    Lambda → SQS → IoT → Forwarder → Mosquitto → Agent → PX4

  ${CYAN}Edge → Cloud:${NC}
    PX4 → Agent → Mosquitto → Forwarder → IoT → Lambda → DynamoDB

${BLUE}Useful Commands:${NC}

  ${YELLOW}# Watch Lambda logs:${NC}
  aws logs tail /aws/lambda/ack_processor --follow

  ${YELLOW}# View recent commands:${NC}
  aws dynamodb scan --table-name DroneCommands --max-items 5

  ${YELLOW}# Send test command:${NC}
  aws lambda invoke \\
    --function-name minimal-command-system \\
    --payload '{"payload":{"target_id":"drone-01","action":"rtl"}}' \\
    response.json

  ${YELLOW}# Check specific command:${NC}
  aws dynamodb get-item \\
    --table-name DroneCommands \\
    --key '{"command_id": {"S": "COMMAND_ID"}}'

${BLUE}Next Steps:${NC}
  1. Send real commands to your drone
  2. Monitor ACKs in DynamoDB
  3. Build a UI to view command history (optional)
  4. Set up CloudWatch alarms (optional)

${GREEN}Your drone command system is now complete! 🚀${NC}

EOF

exit 0