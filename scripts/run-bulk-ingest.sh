#!/usr/bin/env bash
# ===================================================================
# run-bulk-ingest.sh — Launch or teardown the temporary EC2 bulk loader
#
# Usage:
#   ./scripts/run-bulk-ingest.sh launch [--key-pair NAME] [--admin-cidr CIDR]
#   ./scripts/run-bulk-ingest.sh status
#   ./scripts/run-bulk-ingest.sh logs
#   ./scripts/run-bulk-ingest.sh teardown
# ===================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TF_DIR="$PROJECT_ROOT/terraform"
S3_BUCKET="dynamo-ai-documents"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# -------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------

usage() {
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  launch    Create the EC2 bulk loader instance"
    echo "  status    Check instance status and ingestion progress"
    echo "  logs      Fetch the ingestion log from S3"
    echo "  teardown  Destroy the EC2 instance and its VPC"
    echo ""
    echo "Launch options:"
    echo "  --key-pair NAME     EC2 key pair for SSH access"
    echo "  --admin-cidr CIDR   Your IP in CIDR notation (e.g. 203.0.113.10/32)"
    echo ""
    exit 1
}

check_prereqs() {
    for cmd in terraform aws; do
        if ! command -v "$cmd" &>/dev/null; then
            echo -e "${RED}ERROR: $cmd not found in PATH${NC}"
            exit 1
        fi
    done
}

get_instance_ip() {
    cd "$TF_DIR"
    terraform output -raw bulk_instance_public_ip 2>/dev/null || echo ""
}

get_instance_id() {
    cd "$TF_DIR"
    terraform output -raw bulk_instance_id 2>/dev/null || echo ""
}

# -------------------------------------------------------------------
# Commands
# -------------------------------------------------------------------

cmd_launch() {
    local key_pair=""
    local admin_cidr=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --key-pair) key_pair="$2"; shift 2 ;;
            --admin-cidr) admin_cidr="$2"; shift 2 ;;
            *) echo "Unknown option: $1"; usage ;;
        esac
    done

    echo -e "${CYAN}============================================${NC}"
    echo -e "${CYAN}  Launching Bulk Ingestion EC2 Instance${NC}"
    echo -e "${CYAN}============================================${NC}"
    echo ""

    # Upload code package to S3 for the instance to download
    if [ -f "$PROJECT_ROOT/dist/lambda-code.zip" ]; then
        echo -e "${YELLOW}[1/3] Uploading code package to S3...${NC}"
        aws s3 cp "$PROJECT_ROOT/dist/lambda-code.zip" \
            "s3://$S3_BUCKET/_deploy/lambda-code.zip"
        echo "  Uploaded to s3://$S3_BUCKET/_deploy/lambda-code.zip"
    else
        echo -e "${RED}WARNING: dist/lambda-code.zip not found.${NC}"
        echo "  Run scripts/build-lambda.sh first, or the instance"
        echo "  will fail to download the application code."
    fi

    # Build terraform var args
    local tf_vars="-var=enable_bulk_instance=true"
    if [ -n "$key_pair" ]; then
        tf_vars="$tf_vars -var=bulk_key_pair_name=$key_pair"
    fi
    if [ -n "$admin_cidr" ]; then
        tf_vars="$tf_vars -var=bulk_admin_cidr=$admin_cidr"
    fi

    echo ""
    echo -e "${YELLOW}[2/3] Running terraform apply...${NC}"
    cd "$TF_DIR"

    # Target only bulk-related resources
    terraform apply \
        $tf_vars \
        -target=data.aws_ami.al2023 \
        -target=aws_vpc.bulk \
        -target=aws_internet_gateway.bulk \
        -target=aws_subnet.bulk_public \
        -target=aws_route_table.bulk_public \
        -target=aws_route_table_association.bulk_public \
        -target=aws_security_group.bulk_ec2 \
        -target=aws_instance.bulk_loader

    echo ""
    echo -e "${YELLOW}[3/3] Instance details${NC}"

    INSTANCE_IP=$(get_instance_ip)
    INSTANCE_ID=$(get_instance_id)

    echo ""
    echo -e "${GREEN}============================================${NC}"
    echo -e "${GREEN}  Bulk loader launched successfully!${NC}"
    echo -e "${GREEN}============================================${NC}"
    echo ""
    echo -e "  Instance ID: ${CYAN}$INSTANCE_ID${NC}"
    echo -e "  Public IP:   ${CYAN}$INSTANCE_IP${NC}"
    echo ""

    if [ -n "$key_pair" ] && [ -n "$INSTANCE_IP" ]; then
        echo -e "${YELLOW}SSH to monitor:${NC}"
        echo "  ssh -i ~/.ssh/$key_pair.pem ec2-user@$INSTANCE_IP"
        echo ""
        echo -e "${YELLOW}Tail the log on the instance:${NC}"
        echo "  sudo tail -f /var/log/bulk-ingest.log"
        echo ""
    fi

    echo -e "${YELLOW}Check status:${NC}"
    echo "  $0 status"
    echo ""
    echo -e "${YELLOW}Check logs in S3:${NC}"
    echo "  $0 logs"
    echo ""
    echo -e "${RED}Teardown after completion:${NC}"
    echo "  $0 teardown"
    echo ""
}

cmd_status() {
    INSTANCE_ID=$(get_instance_id)

    if [ -z "$INSTANCE_ID" ]; then
        echo -e "${YELLOW}No bulk loader instance found.${NC}"
        exit 0
    fi

    echo -e "${CYAN}Instance: $INSTANCE_ID${NC}"
    echo ""

    # Instance state
    aws ec2 describe-instances \
        --instance-ids "$INSTANCE_ID" \
        --query "Reservations[0].Instances[0].{State:State.Name,LaunchTime:LaunchTime,PublicIp:PublicIpAddress,Type:InstanceType}" \
        --output table 2>/dev/null || echo "  (instance may have been terminated)"

    echo ""

    # Check for completion marker in S3
    echo -e "${YELLOW}Checking S3 for completion marker...${NC}"
    LATEST_MARKER=$(aws s3 ls "s3://$S3_BUCKET/_logs/" \
        --recursive 2>/dev/null | grep "complete.json" | sort | tail -1 | awk '{print $4}') || true

    if [ -n "$LATEST_MARKER" ]; then
        echo -e "${GREEN}  Completion marker found: $LATEST_MARKER${NC}"
        aws s3 cp "s3://$S3_BUCKET/$LATEST_MARKER" - 2>/dev/null | python3 -m json.tool || true
    else
        echo "  No completion marker found — ingestion may still be running"
    fi
}

cmd_logs() {
    echo -e "${CYAN}Checking S3 for ingestion logs...${NC}"
    echo ""

    LATEST_LOG=$(aws s3 ls "s3://$S3_BUCKET/_logs/" \
        --recursive 2>/dev/null | grep "bulk-ingest-" | grep -v "complete" | sort | tail -1 | awk '{print $4}') || true

    if [ -n "$LATEST_LOG" ]; then
        echo -e "${GREEN}Latest log: s3://$S3_BUCKET/$LATEST_LOG${NC}"
        echo "============================================"
        aws s3 cp "s3://$S3_BUCKET/$LATEST_LOG" - 2>/dev/null || echo "  Failed to download log"
    else
        echo "  No logs found in S3 yet."
        echo "  The log is uploaded to S3 after ingestion completes."
        echo ""

        INSTANCE_IP=$(get_instance_ip)
        if [ -n "$INSTANCE_IP" ]; then
            echo "  To view live logs, SSH into the instance:"
            echo "    ssh ec2-user@$INSTANCE_IP 'sudo tail -f /var/log/bulk-ingest.log'"
        fi
    fi
}

cmd_teardown() {
    echo -e "${RED}============================================${NC}"
    echo -e "${RED}  Tearing down Bulk Ingestion EC2${NC}"
    echo -e "${RED}============================================${NC}"
    echo ""

    cd "$TF_DIR"

    terraform apply \
        -var=enable_bulk_instance=false \
        -target=aws_instance.bulk_loader \
        -target=aws_security_group.bulk_ec2 \
        -target=aws_route_table_association.bulk_public \
        -target=aws_route_table.bulk_public \
        -target=aws_subnet.bulk_public \
        -target=aws_internet_gateway.bulk \
        -target=aws_vpc.bulk \
        -target=data.aws_ami.al2023

    echo ""
    echo -e "${GREEN}Bulk loader infrastructure destroyed.${NC}"
    echo ""

    # Clean up deploy artifact
    echo "Cleaning up S3 deploy artifact..."
    aws s3 rm "s3://$S3_BUCKET/_deploy/lambda-code.zip" 2>/dev/null || true

    echo "Done."
}

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

check_prereqs

COMMAND="${1:-}"
shift || true

case "$COMMAND" in
    launch)   cmd_launch "$@" ;;
    status)   cmd_status ;;
    logs)     cmd_logs ;;
    teardown) cmd_teardown ;;
    *)        usage ;;
esac
