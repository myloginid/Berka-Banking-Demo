#!/usr/bin/env python3
"""
NiFi integration script for triggering and monitoring CSV ingestion.

This script:
1. Writes a job.START file to ADLS landing_zone_flag/
2. Waits for NiFi to create job.SUCCESS or job.FAIL
3. Returns exit code 0 on success, 1 on failure

Can be executed:
- Standalone: python nifi_landing_zone_utils.py
- In Airflow: PythonOperator(python_callable=main)

Dependencies:
  pip install azure-storage-blob
"""

import sys
import time
from datetime import datetime
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from azure.storage.blob import BlobServiceClient, BlobClient


# ADLS Configuration
CONTAINER_NAME = "data"
FLAG_DIR = "landing_zone_flag"

# Job control settings
POLL_INTERVAL_SECONDS = 10  # Check every 30 seconds
MAX_WAIT_SECONDS = 3600     # Timeout after 1 hour
STORAGE_ACCOUNT_NAME = "maybank1stor40086938"
STORAGE_ACCOUNT_KEY = "<YOUR-Azure-Key>"


def get_blob_client(blob_name: str):
    """Create Azure Blob client for a specific blob."""
    # Lazy import to avoid timeout during Airflow DAG parsing
    from azure.storage.blob import BlobServiceClient
    
    print(f"[BLOB CLIENT] Creating blob client for: {blob_name}")
    
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={STORAGE_ACCOUNT_NAME};"
        f"AccountKey={STORAGE_ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    
    print(f"[BLOB CLIENT] Connecting to storage account: {STORAGE_ACCOUNT_NAME}")
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
    
    print(f"[BLOB CLIENT] Blob client created successfully for container: {CONTAINER_NAME}")
    return blob_client


def write_start_flag(job_id: str) -> str:
    """Write job.START file to ADLS to trigger NiFi processing."""
    print("=" * 60)
    print("[WRITE FLAG] Starting flag file creation process...")
    print("=" * 60)
    
    execution_date = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    print(f"[WRITE FLAG] Job ID: {job_id}")
    print(f"[WRITE FLAG] Execution Date: {execution_date}")
    
    # File path: landing_zone_flag/job_<id>.START
    blob_path = f"{FLAG_DIR}/{job_id}.START"
    print(f"[WRITE FLAG] Target blob path: {blob_path}")
    
    try:
        blob_client = get_blob_client(blob_path)
        
        # Write minimal content (timestamp)
        content = f"execution_date={execution_date}\nstatus=START\n"
        print(f"[WRITE FLAG] Content length: {len(content)} bytes")
        
        print(f"[WRITE FLAG] Uploading blob to ADLS...")
        blob_client.upload_blob(content, overwrite=True)
        
        print(f"✅ [WRITE FLAG] Created flag file: {blob_path}")
        print("=" * 60)
        print("✅ [WRITE FLAG] Flag file creation completed successfully")
        print("=" * 60)
        return blob_path
        
    except Exception as e:
        print(f"❌ [WRITE FLAG] Failed to create flag file: {type(e).__name__}: {e}")
        raise


def check_file_exists(blob_path: str) -> bool:
    """Check if a blob exists in ADLS."""
    try:
        print(f"[CHECK FILE] Checking existence of: {blob_path}")
        blob_client = get_blob_client(blob_path)
        blob_client.get_blob_properties()
        print(f"✅ [CHECK FILE] File exists: {blob_path}")
        return True
    except Exception as e:
        print(f"[CHECK FILE] File does not exist: {blob_path} ({type(e).__name__})")
        return False


def wait_for_nifi_completion(job_id: str) -> str:
    """
    Poll ADLS for NiFi completion signal (job.SUCCESS or job.FAIL).
    
    Returns:
        "SUCCESS" if job.SUCCESS found
        Raises exception if job.FAIL found or timeout
    """
    print("=" * 60)
    print("[WAIT NIFI] Starting NiFi completion polling...")
    print("=" * 60)
    
    blob_prefix = f"{FLAG_DIR}/{job_id}"
    print(f"[WAIT NIFI] Job ID: {job_id}")
    print(f"[WAIT NIFI] Blob prefix: {blob_prefix}")
    
    success_path = f"{blob_prefix}.SUCCESS"
    fail_path = f"{blob_prefix}.FAIL"
    working_path = f"{blob_prefix}.WORKING"
    
    print(f"⏳ [WAIT NIFI] Waiting for NiFi to complete job: {job_id}")
    print(f"[WAIT NIFI] Will check for: {success_path} or {fail_path}")
    print(f"[WAIT NIFI] Poll interval: {POLL_INTERVAL_SECONDS}s")
    print(f"[WAIT NIFI] Max wait time: {MAX_WAIT_SECONDS}s ({MAX_WAIT_SECONDS//60} minutes)")
    
    elapsed = 0
    
    while elapsed < MAX_WAIT_SECONDS:
        # Check for SUCCESS
        if check_file_exists(success_path):
            print("=" * 60)
            print(f"✅ [WAIT NIFI] NiFi completed successfully: {success_path}")
            print(f"[WAIT NIFI] Total wait time: {elapsed}s")
            print("=" * 60)
            return "SUCCESS"
        
        # Check for FAIL
        if check_file_exists(fail_path):
            print("=" * 60)
            print(f"❌ [WAIT NIFI] NiFi reported failure: {fail_path}")
            print(f"[WAIT NIFI] Total wait time: {elapsed}s")
            print("=" * 60)
            raise Exception(f"NiFi processing failed for {job_id}")
        
        # Optional: log if WORKING status detected
        if elapsed == 0 and check_file_exists(working_path):
            print(f"📝 [WAIT NIFI] NiFi started processing: {working_path}")
        
        # Wait and retry
        if elapsed == 0:
            print(f"[WAIT NIFI] No completion signal yet, starting polling loop...")
        
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS
        
        if elapsed % 300 == 0:  # Log every 5 minutes
            print(f"⏳ [WAIT NIFI] Still waiting... ({elapsed}s / {MAX_WAIT_SECONDS}s elapsed)")
    
    # Timeout
    print("=" * 60)
    print(f"❌ [WAIT NIFI] Timeout after {MAX_WAIT_SECONDS}s")
    print(f"[WAIT NIFI] Expected files not found:")
    print(f"  - Success: {success_path}")
    print(f"  - Fail: {fail_path}")
    print("=" * 60)
    raise Exception(
        f"Timeout waiting for NiFi completion after {MAX_WAIT_SECONDS}s. "
        f"Expected {success_path} or {fail_path}"
    )


def cleanup_flag_files(job_id: str) -> None:
    """Optional: Clean up all flag files after completion."""
    print("=" * 60)
    print("[CLEANUP] Starting flag file cleanup...")
    print("=" * 60)
    
    blob_prefix = f"{FLAG_DIR}/{job_id}"
    print(f"[CLEANUP] Blob prefix: {blob_prefix}")
    
    deleted_count = 0
    failed_count = 0
    
    for suffix in ['.START', '.WORKING', '.SUCCESS', '.FAIL']:
        blob_path = f"{blob_prefix}{suffix}"
        try:
            print(f"[CLEANUP] Attempting to delete: {blob_path}")
            blob_client = get_blob_client(blob_path)
            blob_client.delete_blob()
            print(f"🗑️  [CLEANUP] Deleted: {blob_path}")
            deleted_count += 1
        except Exception as e:
            print(f"⚠️  [CLEANUP] Could not delete {blob_path}: {type(e).__name__}: {e}")
            failed_count += 1
    
    print("=" * 60)
    print(f"✅ [CLEANUP] Cleanup completed: {deleted_count} deleted, {failed_count} failed")
    print("=" * 60)


def main(job_id: str = "job_fetch_csv") -> int:
    """
    Main function to trigger NiFi and wait for completion.
    
    Args:
        job_id: Identifier for this job (default: job_fetch_csv)
    
    Returns:
        0 on success, 1 on failure
    """
    try:
        # Step 1: Write START flag
        write_start_flag(job_id)
        
        # Step 2: Wait for NiFi completion
        result = wait_for_nifi_completion(job_id)
        
        # Step 3: Cleanup (optional)
        cleanup_flag_files(job_id)
        
        print("\n" + "=" * 60)
        print("✅ WORKFLOW COMPLETED SUCCESSFULLY")
        print("=" * 60)
        return 0
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ WORKFLOW FAILED: {type(e).__name__}: {e}")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    # Run standalone
    import argparse
    
    parser = argparse.ArgumentParser(description="Trigger NiFi ingestion and wait for completion")
    parser.add_argument(
        "--job-id",
        default="job_fetch_csv",
        help="Job identifier (default: job_fetch_csv)"
    )
    
    args = parser.parse_args()
    
    exit_code = main(args.job_id)
    sys.exit(exit_code)

