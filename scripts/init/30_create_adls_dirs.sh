#!/usr/bin/env bash
#
# Create ADLS containers and directories for Berka lakehouse + checkpoints.
# Usage:
#   AZURE_STORAGE_ACCOUNT=<account> az login ...
#   ./30_create_adls_dirs.sh

set -euo pipefail

ACCOUNT="${ACCOUNT:-mystorageaccount}"
RAW_CONTAINER="${RAW_CONTAINER:-berka-raw}"
WH_CONTAINER="${WH_CONTAINER:-berka-warehouse}"

echo "Using storage account: ${ACCOUNT}"

# Create containers (ADLS Gen2-enabled)
az storage container create --account-name "${ACCOUNT}" --name "${RAW_CONTAINER}" >/dev/null
az storage container create --account-name "${ACCOUNT}" --name "${WH_CONTAINER}" >/dev/null

# Raw Berka CSV landing
az storage fs directory create \
  --account-name "${ACCOUNT}" \
  --file-system "${RAW_CONTAINER}" \
  --name "berka/data" >/dev/null

# Lakehouse zones
for zone in bronze silver gold; do
  az storage fs directory create \
    --account-name "${ACCOUNT}" \
    --file-system "${WH_CONTAINER}" \
    --name "${zone}" >/dev/null
done

# Streaming checkpoints
for job in fact_loan fact_order fact_trans customer360; do
  az storage fs directory create \
    --account-name "${ACCOUNT}" \
    --file-system "${WH_CONTAINER}" \
    --name "checkpoints/${job}" >/dev/null
done

echo "ADLS directories created under containers ${RAW_CONTAINER} and ${WH_CONTAINER}"

