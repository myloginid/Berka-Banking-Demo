#!/bin/bash
################################################################################
# NiFi integration script for triggering and monitoring CSV ingestion
#
# This script:
# 1. Writes a job.START file to ADLS landing_zone_flag/
# 2. Waits for NiFi to create job.SUCCESS or job.FAIL
# 3. Returns exit code 0 on success, 1 on failure
#
# Usage:
#   ./test.sh [job_id]
#
# Example:
#   ./test.sh job_fetch_csv
################################################################################

set -euo pipefail

# ============================= Configuration ==================================
CONTAINER_NAME="data"
FLAG_DIR="landing_zone_flag"
STORAGE_ACCOUNT_NAME="maybank1stor40086938"
STORAGE_ACCOUNT_KEY="<My Password>"


# Job control settings
POLL_INTERVAL_SECONDS=10  # Check every 30 seconds
MAX_WAIT_SECONDS=3600     # Timeout after 1 hour (60 minutes)

# Job ID (default or from argument)
JOB_ID="${1:-job_fetch_csv}"

# ============================= Helper Functions ================================

log_separator() {
    echo "============================================================"
}

# Upload a blob to ADLS using Azure CLI
upload_blob() {
    local blob_path="$1"
    local content="$2"
    
    echo "[UPLOAD BLOB] Uploading: $blob_path"
    
    # Create temp file with content
    local temp_file=$(mktemp)
    echo -e "$content" > "$temp_file"
    
    # Upload using az storage blob upload
    az storage blob upload \
        --account-name "$STORAGE_ACCOUNT_NAME" \
        --account-key "$STORAGE_ACCOUNT_KEY" \
        --container-name "$CONTAINER_NAME" \
        --name "$blob_path" \
        --file "$temp_file" \
        --overwrite true \
        --output none
    
    rm -f "$temp_file"
    echo "✅ [UPLOAD BLOB] Successfully uploaded: $blob_path"
}

# Check if a blob exists in ADLS
check_blob_exists() {
    local blob_path="$1"
    
    echo "[CHECK FILE] Checking existence of: $blob_path"
    
    if az storage blob exists \
        --account-name "$STORAGE_ACCOUNT_NAME" \
        --account-key "$STORAGE_ACCOUNT_KEY" \
        --container-name "$CONTAINER_NAME" \
        --name "$blob_path" \
        --output json \
        --query exists 2>/dev/null | grep -q "true"; then
        echo "✅ [CHECK FILE] File exists: $blob_path"
        return 0
    else
        echo "[CHECK FILE] File does not exist: $blob_path"
        return 1
    fi
}

# List all blobs matching a prefix for debugging
list_matching_blobs() {
    local prefix="$1"
    
    echo "[LIST BLOBS] Listing all blobs with prefix: $prefix"
    
    local blob_list=$(az storage blob list \
        --account-name "$STORAGE_ACCOUNT_NAME" \
        --account-key "$STORAGE_ACCOUNT_KEY" \
        --container-name "$CONTAINER_NAME" \
        --prefix "$prefix" \
        --output tsv \
        --query "[].name" 2>/dev/null)
    
    if [ -z "$blob_list" ]; then
        echo "[LIST BLOBS] No blobs found with prefix: $prefix"
    else
        local count=$(echo "$blob_list" | wc -l | tr -d ' ')
        echo "[LIST BLOBS] Found $count blob(s):"
        echo "$blob_list" | while read -r blob_name; do
            echo "  📄 $blob_name"
        done
    fi
}

# Delete a blob from ADLS
delete_blob() {
    local blob_path="$1"
    
    echo "[DELETE BLOB] Attempting to delete: $blob_path"
    
    if az storage blob delete \
        --account-name "$STORAGE_ACCOUNT_NAME" \
        --account-key "$STORAGE_ACCOUNT_KEY" \
        --container-name "$CONTAINER_NAME" \
        --name "$blob_path" \
        --output none 2>/dev/null; then
        echo "🗑️  [DELETE BLOB] Deleted: $blob_path"
        return 0
    else
        echo "⚠️  [DELETE BLOB] Could not delete: $blob_path"
        return 1
    fi
}

# ============================= Main Functions ==================================

write_start_flag() {
    log_separator
    echo "[WRITE FLAG] Starting flag file creation process..."
    log_separator
    
    local execution_date=$(date '+%Y%m%d_%H%M%S')
    local blob_path="${FLAG_DIR}/${JOB_ID}.START"
    
    echo "[WRITE FLAG] Job ID: $JOB_ID"
    echo "[WRITE FLAG] Execution Date: $execution_date"
    echo "[WRITE FLAG] Target blob path: $blob_path"
    
    local content="execution_date=${execution_date}\nstatus=START"
    
    upload_blob "$blob_path" "$content"
    
    log_separator
    echo "✅ [WRITE FLAG] Flag file creation completed successfully"
    log_separator
}

wait_for_nifi_completion() {
    log_separator
    echo "[WAIT NIFI] Starting NiFi completion polling..."
    log_separator
    
    local blob_prefix="${FLAG_DIR}/${JOB_ID}"
    local success_path="${blob_prefix}.SUCCESS"
    local fail_path="${blob_prefix}.FAIL"
    local working_path="${blob_prefix}.WORKING"
    
    echo "[WAIT NIFI] Job ID: $JOB_ID"
    echo "[WAIT NIFI] Blob prefix: $blob_prefix"
    echo "⏳ [WAIT NIFI] Waiting for NiFi to complete job: $JOB_ID"
    echo "[WAIT NIFI] Will check for: $success_path or $fail_path"
    echo "[WAIT NIFI] Poll interval: ${POLL_INTERVAL_SECONDS}s"
    echo "[WAIT NIFI] Max wait time: ${MAX_WAIT_SECONDS}s ($((MAX_WAIT_SECONDS/60)) minutes)"
    
    local elapsed=0
    local first_check=true
    local debug_done=false
    
    while [ $elapsed -lt $MAX_WAIT_SECONDS ]; do
        # Debug: List all matching blobs on first iteration
        if [ "$debug_done" = false ]; then
            echo ""
            log_separator
            echo "🔍 [DEBUG] Listing all files matching job prefix..."
            log_separator
            list_matching_blobs "$blob_prefix"
            log_separator
            echo ""
            debug_done=true
        fi
        
        # Check for SUCCESS
        if check_blob_exists "$success_path"; then
            log_separator
            echo "✅ [WAIT NIFI] NiFi completed successfully: $success_path"
            echo "[WAIT NIFI] Total wait time: ${elapsed}s"
            log_separator
            return 0
        fi
        
        # Check for FAIL
        if check_blob_exists "$fail_path"; then
            log_separator
            echo "❌ [WAIT NIFI] NiFi reported failure: $fail_path"
            echo "[WAIT NIFI] Total wait time: ${elapsed}s"
            log_separator
            return 1
        fi
        
        # Check WORKING status on first iteration
        if [ "$first_check" = true ] && check_blob_exists "$working_path"; then
            echo "📝 [WAIT NIFI] NiFi started processing: $working_path"
        fi
        
        if [ "$first_check" = true ]; then
            echo "[WAIT NIFI] No completion signal yet, starting polling loop..."
            first_check=false
        fi
        
        # Wait and retry
        sleep $POLL_INTERVAL_SECONDS
        elapsed=$((elapsed + POLL_INTERVAL_SECONDS))
        
        # Log every 5 minutes
        if [ $((elapsed % 300)) -eq 0 ]; then
            echo "⏳ [WAIT NIFI] Still waiting... (${elapsed}s / ${MAX_WAIT_SECONDS}s elapsed)"
        fi
    done
    
    # Timeout
    log_separator
    echo "❌ [WAIT NIFI] Timeout after ${MAX_WAIT_SECONDS}s"
    echo "[WAIT NIFI] Expected files not found:"
    echo "  - Success: $success_path"
    echo "  - Fail: $fail_path"
    log_separator
    return 1
}

cleanup_flag_files() {
    log_separator
    echo "[CLEANUP] Starting flag file cleanup..."
    log_separator
    
    local blob_prefix="${FLAG_DIR}/${JOB_ID}"
    echo "[CLEANUP] Blob prefix: $blob_prefix"
    
    local deleted_count=0
    local failed_count=0
    
    for suffix in .START .WORKING .SUCCESS .FAIL; do
        local blob_path="${blob_prefix}${suffix}"
        if delete_blob "$blob_path"; then
            deleted_count=$((deleted_count + 1))
        else
            failed_count=$((failed_count + 1))
        fi
    done
    
    log_separator
    echo "✅ [CLEANUP] Cleanup completed: ${deleted_count} deleted, ${failed_count} failed"
    log_separator
}

# ============================= Main Execution ==================================

main() {
    echo ""
    log_separator
    echo "Starting NiFi Integration Workflow"
    echo "Job ID: $JOB_ID"
    log_separator
    echo ""
    
    # Step 1: Write START flag
    if ! write_start_flag; then
        echo ""
        log_separator
        echo "❌ WORKFLOW FAILED: Could not write START flag"
        log_separator
        return 1
    fi
    
    # Step 2: Wait for NiFi completion
    if ! wait_for_nifi_completion; then
        echo ""
        log_separator
        echo "❌ WORKFLOW FAILED: NiFi processing failed or timed out"
        log_separator
        return 1
    fi
    
    # Step 3: Cleanup (optional)
    cleanup_flag_files
    
    echo ""
    log_separator
    echo "✅ WORKFLOW COMPLETED SUCCESSFULLY"
    log_separator
    
    return 0
}

# Run main function
main
exit $?

