#!/usr/bin/env bash
# ===================================================================
# monitor-bulk-ingest.sh — Real-time monitoring of bulk ingestion
#
# Usage:
#   ./scripts/monitor-bulk-ingest.sh           Follow logs + stats
#   ./scripts/monitor-bulk-ingest.sh --stats   Stats only (no log tail)
#   ./scripts/monitor-bulk-ingest.sh --logs    Logs only (no stats)
# ===================================================================
set -euo pipefail

# Constants
REGISTRY_TABLE="sp-ingest-document-registry"
S3_BUCKET="dynamo-ai-documents"
REGION="us-east-1"
STATS_INTERVAL=60

# Log group: bulk ingest logs on EC2 go to /sp-ingest/bulk-ingest
# Lambda logs go to /aws/lambda/sp-ingest-*
BULK_LOG_GROUP="/sp-ingest/bulk-ingest"
LAMBDA_LOG_GROUPS=(
    "/aws/lambda/sp-ingest-daily-sync"
    "/aws/lambda/sp-ingest-textract-trigger"
    "/aws/lambda/sp-ingest-textract-complete"
)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# Options
MODE="both"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --stats)    MODE="stats"; shift ;;
        --logs)     MODE="logs"; shift ;;
        --interval) STATS_INTERVAL="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--stats | --logs] [--interval SECONDS]"
            echo ""
            echo "  --stats     Show stats only (no log tailing)"
            echo "  --logs      Show logs only (no stats polling)"
            echo "  --interval  Seconds between stats refresh (default: 60)"
            exit 0
            ;;
        *) echo -e "${RED}Unknown option: $1${NC}"; exit 1 ;;
    esac
done

# ===================================================================
# Functions
# ===================================================================

format_number() {
    printf "%'d" "$1" 2>/dev/null || echo "$1"
}

query_registry_stats() {
    # Query DynamoDB for textract_status counts using the GSI
    local result
    result=$(python3 -c "
import boto3, json, sys

ddb = boto3.resource('dynamodb', region_name='$REGION')
table = ddb.Table('$REGISTRY_TABLE')

# Total items (scan count only)
total_resp = table.scan(Select='COUNT')
total = total_resp.get('Count', 0)

# Query each status via the GSI
statuses = {}
for status in ['pending', 'processing', 'completed', 'failed', 'direct_extracted']:
    try:
        resp = table.query(
            IndexName='textract_status-index',
            Select='COUNT',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('textract_status').eq(status),
        )
        statuses[status] = resp.get('Count', 0)
    except Exception:
        statuses[status] = 0

print(json.dumps({
    'total': total,
    'pending': statuses.get('pending', 0),
    'processing': statuses.get('processing', 0),
    'completed': statuses.get('completed', 0),
    'failed': statuses.get('failed', 0),
    'direct_extracted': statuses.get('direct_extracted', 0),
}))
" 2>/dev/null || echo '{"total":0,"pending":0,"processing":0,"completed":0,"failed":0,"direct_extracted":0}')

    echo "$result"
}

print_stats_line() {
    local stats="$1"
    local ts
    ts=$(date "+%H:%M:%S")

    local total pending processing completed failed direct
    total=$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['total'])" 2>/dev/null || echo 0)
    pending=$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['pending'])" 2>/dev/null || echo 0)
    processing=$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['processing'])" 2>/dev/null || echo 0)
    completed=$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['completed'])" 2>/dev/null || echo 0)
    failed=$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['failed'])" 2>/dev/null || echo 0)
    direct=$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['direct_extracted'])" 2>/dev/null || echo 0)

    local extracted=$((completed + direct))

    echo -e "${DIM}[$ts]${NC} Ingested: ${GREEN}$(format_number "$total")${NC} | Textract: ${GREEN}$(format_number "$extracted") done${NC}, ${CYAN}$(format_number "$processing") processing${NC}, ${YELLOW}$(format_number "$pending") pending${NC}, ${RED}$(format_number "$failed") failed${NC}"
}

print_stats_detail() {
    local stats="$1"

    local total pending processing completed failed direct
    total=$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['total'])" 2>/dev/null || echo 0)
    pending=$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['pending'])" 2>/dev/null || echo 0)
    processing=$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['processing'])" 2>/dev/null || echo 0)
    completed=$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['completed'])" 2>/dev/null || echo 0)
    failed=$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['failed'])" 2>/dev/null || echo 0)
    direct=$(echo "$stats" | python3 -c "import sys,json; print(json.load(sys.stdin)['direct_extracted'])" 2>/dev/null || echo 0)

    echo ""
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}${BOLD}  Pipeline Status — $(date "+%Y-%m-%d %H:%M:%S")${NC}"
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  Total ingested:      ${BOLD}$(format_number "$total")${NC}"
    echo ""
    echo -e "  Textract completed:  ${GREEN}$(format_number "$completed")${NC}"
    echo -e "  Direct extracted:    ${GREEN}$(format_number "$direct")${NC}"
    echo -e "  Processing:          ${CYAN}$(format_number "$processing")${NC}"
    echo -e "  Pending:             ${YELLOW}$(format_number "$pending")${NC}"
    echo -e "  Failed:              ${RED}$(format_number "$failed")${NC}"

    # S3 object count
    local source_count extracted_count
    source_count=$(aws s3api list-objects-v2 --bucket "$S3_BUCKET" \
        --prefix "source/" --query "KeyCount" --output text 2>/dev/null || echo "?")
    extracted_count=$(aws s3api list-objects-v2 --bucket "$S3_BUCKET" \
        --prefix "extracted/" --query "KeyCount" --output text 2>/dev/null || echo "?")

    echo ""
    echo -e "  S3 source/ objects:    ${BOLD}$source_count${NC}"
    echo -e "  S3 extracted/ objects: ${BOLD}$extracted_count${NC}"
    echo ""
}

tail_logs() {
    # Use aws logs tail if available, otherwise fall-back to filter-log-events
    local log_group="$1"
    local since="${2:-5m}"

    echo -e "${YELLOW}Tailing $log_group (since $since ago)...${NC}"
    echo -e "${DIM}Press Ctrl+C to stop${NC}"
    echo ""

    aws logs tail "$log_group" --since "$since" --follow --format short 2>/dev/null || {
        echo -e "${YELLOW}aws logs tail not available, using filter-log-events...${NC}"
        local start_ms
        start_ms=$(python3 -c "
import time
print(int((time.time() - 300) * 1000))
" 2>/dev/null)

        while true; do
            aws logs filter-log-events \
                --log-group-name "$log_group" \
                --start-time "$start_ms" \
                --query "events[].message" \
                --output text 2>/dev/null | while IFS= read -r line; do
                    [ -n "$line" ] && echo "$line"
                done

            start_ms=$(python3 -c "import time; print(int(time.time() * 1000))" 2>/dev/null)
            sleep 5
        done
    }
}

# ===================================================================
# Main
# ===================================================================

echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}${BOLD}  Bulk Ingestion Monitor${NC}"
echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Check prerequisites
if ! command -v aws &>/dev/null; then
    echo -e "${RED}ERROR: aws CLI not found${NC}"
    exit 1
fi

if ! python3 -c "import boto3" 2>/dev/null; then
    echo -e "${RED}ERROR: boto3 not available for Python3${NC}"
    echo "  Install with: pip install boto3"
    exit 1
fi

# Check DynamoDB connectivity
echo -e "${DIM}Connecting to DynamoDB...${NC}"
INITIAL_STATS=$(query_registry_stats)
print_stats_detail "$INITIAL_STATS"

case "$MODE" in
    stats)
        # Stats-only mode: poll every interval
        echo -e "${YELLOW}Refreshing every ${STATS_INTERVAL}s (Ctrl+C to stop)${NC}"
        echo ""

        PREV_TOTAL=0
        while true; do
            sleep "$STATS_INTERVAL"
            STATS=$(query_registry_stats)

            CURR_TOTAL=$(echo "$STATS" | python3 -c "import sys,json; print(json.load(sys.stdin)['total'])" 2>/dev/null || echo 0)
            DELTA=$((CURR_TOTAL - PREV_TOTAL))
            PREV_TOTAL=$CURR_TOTAL

            print_stats_line "$STATS"
            if [ "$DELTA" -gt 0 ]; then
                echo -e "  ${DIM}(+$DELTA since last check)${NC}"
            fi
        done
        ;;

    logs)
        # Logs-only: tail the bulk ingest log group
        # Try the custom log group first, fall back to Lambda log groups
        LOG_GROUP_EXISTS=$(aws logs describe-log-groups \
            --log-group-name-prefix "$BULK_LOG_GROUP" \
            --query "length(logGroups)" --output text 2>/dev/null || echo "0")

        HAS_STREAMS="0"
        if [ "$LOG_GROUP_EXISTS" -gt 0 ] 2>/dev/null; then
            HAS_STREAMS=$(aws logs describe-log-streams \
                --log-group-name "$BULK_LOG_GROUP" --order-by LastEventTime --descending --max-items 1 \
                --query "length(logStreams)" --output text 2>/dev/null || echo "0")
        fi

        if [ "$HAS_STREAMS" -gt 0 ] 2>/dev/null; then
            tail_logs "$BULK_LOG_GROUP" "10m"
        else
            echo -e "${YELLOW}No streams in $BULK_LOG_GROUP, tailing Lambda logs instead...${NC}"
            # Tail all Lambda log groups interleaved via a simple polling loop
            SINCE_MS=$(python3 -c "import time; print(int((time.time() - 600) * 1000))")
            while true; do
                for lg in "${LAMBDA_LOG_GROUPS[@]}"; do
                    aws logs filter-log-events \
                        --log-group-name "$lg" \
                        --start-time "$SINCE_MS" \
                        --query "events[].{t:timestamp,m:message}" \
                        --output json 2>/dev/null \
                    | python3 -c "
import sys, json
from datetime import datetime, timezone
events = json.load(sys.stdin)
for e in events:
    ts = datetime.fromtimestamp(e['t']/1000, tz=timezone.utc).strftime('%H:%M:%S')
    msg = e['m'].strip()[:200]
    print(f'  [{ts}] {msg}')
" 2>/dev/null || true
                done
                SINCE_MS=$(python3 -c "import time; print(int(time.time() * 1000))")
                sleep 5
            done
        fi
        ;;

    both)
        # Combined mode: stats loop in foreground, periodic log check
        echo -e "${YELLOW}Monitoring pipeline (stats every ${STATS_INTERVAL}s, Ctrl+C to stop)${NC}"
        echo ""

        PREV_TOTAL=$(echo "$INITIAL_STATS" | python3 -c "import sys,json; print(json.load(sys.stdin)['total'])" 2>/dev/null || echo 0)

        # Check for completion marker
        check_completion() {
            local marker
            marker=$(aws s3 ls "s3://$S3_BUCKET/_logs/" 2>/dev/null \
                | grep "complete.json" | sort | tail -1 | awk '{print $4}') || true
            echo "$marker"
        }

        ITERATION=0
        while true; do
            sleep "$STATS_INTERVAL"
            ITERATION=$((ITERATION + 1))

            STATS=$(query_registry_stats)
            CURR_TOTAL=$(echo "$STATS" | python3 -c "import sys,json; print(json.load(sys.stdin)['total'])" 2>/dev/null || echo 0)
            DELTA=$((CURR_TOTAL - PREV_TOTAL))
            PREV_TOTAL=$CURR_TOTAL

            print_stats_line "$STATS"
            if [ "$DELTA" -gt 0 ]; then
                echo -e "  ${DIM}(+$(format_number "$DELTA") new since last check, ~$(( DELTA * 60 / STATS_INTERVAL ))/min)${NC}"
            fi

            # Every 5 iterations, show detailed stats
            if [ $((ITERATION % 5)) -eq 0 ]; then
                print_stats_detail "$STATS"
            fi

            # Check for completion
            COMPLETION=$(check_completion)
            if [ -n "$COMPLETION" ]; then
                echo ""
                echo -e "${GREEN}${BOLD}  Bulk ingestion completed!${NC}"
                echo -e "  ${CYAN}Completion marker: s3://$S3_BUCKET/_logs/$COMPLETION${NC}"
                echo ""

                # Fetch and display
                aws s3 cp "s3://$S3_BUCKET/_logs/$COMPLETION" - 2>/dev/null \
                    | python3 -m json.tool 2>/dev/null || true
                echo ""
                echo -e "  Teardown: ${CYAN}./scripts/run-bulk-ingest.sh teardown${NC}"
                break
            fi
        done
        ;;
esac
