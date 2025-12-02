# Deployment Steps

This document captures the key steps to deploy and run the Berka demo in CDP Public Cloud, including authentication setup for CDE.

## 1. CDE CLI layout & setup

The repo now keeps CDE tooling and secrets under `tools/` so the project
root stays clean:

- `tools/cde` – CDE CLI binary (executable).
- `tools/.cde/config.yaml` – virtual cluster endpoint and CDP control plane endpoint.
- `tools/credentials` – CDP access key + private key (never commit to git).
- `tools/cde-pass.txt` – local password file for the CDE user (never commit to git).

### 1.1 Place the CLI binary

1. Copy the `cde` CLI binary for your CDE virtual cluster into `tools/`.
2. Make it executable:
   ```bash
   cd /Users/manish/data/work/code/bankdemo
   mkdir -p tools
   mv /path/to/downloaded/cde tools/cde
   chmod +x tools/cde
   ```

### 1.2 CDP credentials file

Create the CDP credentials file under `tools/` (already present in this repo for convenience):
   ```bash
   mkdir -p tools
   cat > tools/credentials << 'EOF'
   [default]
   cdp_access_key_id = e08748cc-b28d-44a3-8146-206c57bb0967
   cdp_private_key   = rN5hRimXrukw2WWdzzeYq7XMjBRKYVHu5DrIK0lPMI4=
   EOF
   ```

### 1.3 CDE endpoint config

Ensure the CDE endpoint config exists at `tools/.cde/config.yaml`:
   ```yaml
   user: manishm
   vcluster-endpoint: https://l9j5mkt8.cde-fdsrx9qm.maybank1.xfaz-gdb4.cloudera.site/dex/api/v1
   cdp-endpoint: https://api.us-west-1.cdp.cloudera.com
   ```

## 2. Non-interactive password file for CDE

To avoid interactive password prompts in automation (scripts, CI, etc.), use a password file for the CDE user:

1. Create a local password file under `tools/`:
   ```bash
   cd /Users/manish/data/work/code/bankdemo

   cat > tools/cde-pass.txt << 'EOF'
   Cloudera@123
   EOF

   chmod 600 cde-pass.txt
   ```
2. Make sure the password file is excluded from git (see `.gitignore`).

## 3. CDE command prefix

From the project root, use this prefix for all CDE CLI commands:

```bash
cd /Users/manish/data/work/code/bankdemo

BASE_CDE="tools/cde \
  --credentials-file tools/credentials \
  --cdp-endpoint https://api.us-west-1.cdp.cloudera.com \
  --vcluster-endpoint https://l9j5mkt8.cde-fdsrx9qm.maybank1.xfaz-gdb4.cloudera.site/dex/api/v1"
```

### 3.1 Testing connectivity

```bash
$BASE_CDE job list
```

Expected result for a fresh environment is an empty JSON array:

```text
[]
```

This confirms that:
- The `cde` CLI is working.
- The credentials and password file are valid.
- The CDE endpoint is reachable and accepting requests.

## 4. Common CDE commands used in this POC

This section summarizes the key commands used to manage resources and jobs for the Berka demo.

### 4.1 Resources

- List resources:
  ```bash
  $BASE_CDE resource list
  ```

- Upload / refresh the code resource:
  ```bash
  $BASE_CDE resource upload \
    --name berka-code \
  --local-path scripts/etl/dim_account_bronze_to_silver.py \
    --resource-path dim_account_bronze_to_silver.py
  ```

  Repeat the `--local-path / --resource-path` pair for other scripts as needed (e.g. `fact_loan_streaming.py`, `customer_360_hbase_rest_streaming.py`).

### 4.2 Jobs

- List jobs:
  ```bash
  $BASE_CDE job list
  ```

- Describe a job:
  ```bash
  $BASE_CDE job describe --name customer-360-hbase-rest-streaming
  ```

- Create a Spark job:
  ```bash
  $BASE_CDE job create \
    --name customer-360-hbase-rest-streaming \
    --type spark \
    --mount-1-prefix / \
    --mount-1-resource berka-code \
    --application-file customer_360_hbase_rest_streaming.py \
    --driver-cores 1 \
    --driver-memory 1g \
    --executor-cores 1 \
    --executor-memory 1g \
    --conf spark.executor.instances=1 \
    --conf spark.security.credentials.hiveserver2.enabled=false
  ```

- Run a job with a fresh checkpoint and Kafka consumer group:
  ```bash
  $BASE_CDE job run \
    --name customer-360-hbase-rest-streaming \
    --arg --checkpoint-location \
    --arg /tmp/berka_customer360_hbase_rest_checkpoint_v1 \
    --arg --kafka-group-id \
    --arg berka_customer360_hbase_rest_cg_v1
  ```

Use the same patterns for the other ETL jobs:
- Dimension Bronze→Silver and Silver→Gold jobs.
- Fact streaming jobs (`fact_loan_streaming`, `fact_order_streaming`, `fact_trans_streaming`).
- Validation job (`impala_pk_fk_checks_spark.py`).

## 5. Airflow DAG for the dimension pipeline

The Airflow DAG that orchestrates the dimension jobs lives in
`scripts/cml/dimensions_airflow_dag.py`. The DAG id inside the file is
`berka_dimensions_pipeline`.

### 5.1 Package the DAG as a CDE files resource

You first need to put the DAG file into a CDE files resource so the
Airflow job can see it.

```bash
cd /Users/manish/data/work/code/bankdemo

$BASE_CDE resource create \
  --name berka-airflow \
  --type files

$BASE_CDE resource upload \
  --name berka-airflow \
  --local-path scripts/cml/dimensions_airflow_dag.py \
  --resource-path dimensions_airflow_dag.py
```

Verify the DAG file is present:

```bash
$BASE_CDE resource describe --name berka-airflow
```

You should see `dimensions_airflow_dag.py` under `files`.

### 5.2 Create the Airflow job in CDE

Create an Airflow job that mounts the DAG resource and points at the DAG file:

```bash
$BASE_CDE job create \
  --name berka-dimensions-pipeline \
  --type airflow \
  --mount-1-resource berka-airflow \
  --mount-1-prefix / \
  --dag-file dimensions_airflow_dag.py \
  --schedule-enabled false \
  --schedule-paused false
```

Notes:
- `--mount-1-resource` mounts the `berka-airflow` resource at `/` inside
  the Airflow environment.
- `--dag-file` is the path to the DAG file *inside* the mounted resource.
- Setting `--schedule-enabled false` and `--schedule-paused false`
  creates the job with scheduling disabled but not paused, so you can
  trigger manual runs from the CLI.

You can confirm the job definition with:

```bash
$BASE_CDE job describe --name berka-dimensions-pipeline
```

### 5.3 Trigger an Airflow job run

To run the full Bronze→Silver→Gold dimension pipeline via Airflow:

```bash
$BASE_CDE job run \
  --name berka-dimensions-pipeline
```

If you ever see an error like:

```text
job 'berka-dimensions-pipeline' is paused, resume the schedule before triggering the run
```

then open the CDE UI, locate the `berka-dimensions-pipeline` Airflow job,
and unpause/enable its schedule once. After that, manual `job run`
invocations will succeed.

From the CDE UI you can also monitor the Airflow job run and see each Spark
dimension job triggered in the dependency order defined by the DAG.

## 6. Customer 360 with HBase REST

The original attempts to write Customer 360 data to COD HBase used the
native HBase client and the Spark 3 HBase connector, but ran into
classpath and dependency issues. For the demo we switched to using the
HBase REST API from Spark.

The REST-based job is implemented in
`scripts/etl/customer_360_hbase_rest_streaming.py` and deployed in CDE
as `customer-360-hbase-rest-streaming`.

High‑level behavior:
- Reads three Kafka topics (`berka_loans`, `berka_orders`, `berka_trans`)
  with SASL_SSL/PLAIN authentication configured in the Spark job.
- Uses Spark SQL to:
  - Parse JSON payloads from Kafka.
  - Join with `gold.dim_client` and `gold.dim_account` (current SCD rows).
  - Build a per‑client “Customer 360” view with the latest loan, order and
    transaction attributes.
- For each micro‑batch, collects a bounded number of rows on the driver
  (`--max-rows-per-batch`) and writes them into COD HBase via REST.
- HBase REST details:
  - Base URL: `--hbase-rest-url`, defaulting to  
    `https://cod-29b4gj7gde0p-gateway0.maybank1.xfaz-gdb4.cloudera.site/cod-29b4gj7gde0p/cdp-proxy-api/hbase`
  - Target table: `<namespace>:<table>` from `--hbase-namespace` and
    `--hbase-table` (defaults `default:customer360`).
  - Column family: `--hbase-column-family` (default `f`).
  - Row key: `client_id` as string.
  - Authentication: basic auth with `--hbase-rest-user` /
    `--hbase-rest-password` (for the POC: `manishm` / `Cloudera@123`).
  - Payload format: HBase REST JSON with base64‑encoded row key, column
    names and values.

The job is created and run in CDE as a normal Spark job (see section 4.2
for the pattern), with `application-file` set to
`customer_360_hbase_rest_streaming.py` and appropriate `--checkpoint-location`
and `--kafka-group-id` arguments when running.

## 7. Bronze → Silver fixes for account and card

Early runs of the Bronze → Silver dimension jobs showed:
- `silver.account_silver` and `silver.card_silver` with 0 rows.
- All account rows ending up in `silver.dq_account` / `silver.dq_card`.

The root causes were:
- `account_bronze.frequency` values wrapped in double quotes in the CSV.
- Some basic format checks missing for account/card identifiers and dates.

To fix this, we adjusted the Bronze → Silver jobs to use stricter DQ
filters and proper casting.

### 7.1 Account Bronze → Silver (`dim_account_bronze_to_silver.py`)

Key changes:
- The job is SQL‑only (no DataFrame API), following the repo convention.
- New DQ table: `silver.dq_account`:
  - Captures records where:
    - `account_id` or `district_id` is not purely numeric, or
    - `frequency` is null/empty, or
    - `created_date` is null/empty.
  - Adds a `dq_date` (current_date) and `dq_reason`.
- Silver table `silver.account_silver` is created with typed columns:
  - `account_id INT`
  - `district_id INT`
  - `frequency STRING`
  - `open_date DATE`
- The `INSERT OVERWRITE` into `silver.account_silver`:
  - Filters to rows where `account_id` and `district_id` match `^[0-9]+$`
    and `frequency` is non‑empty.
  - Casts `account_id`/`district_id` to `INT`.
  - Converts `created_date` (e.g. `250101`) to a proper date using
    `TO_DATE(created_date, 'yyMMdd')` and stores it as `open_date`.

Result: `silver.account_silver` now contains 4,500 clean rows, and
`silver.dq_account` holds the rejected rows for audit (54,000 in this POC).

### 7.2 Card Bronze → Silver (`dim_card_bronze_to_silver.py`)

Key changes:
- The job mirrors the account pattern:
  - Pure Spark SQL with Hive support.
  - Separate DQ and Silver tables.
- DQ table: `silver.dq_card`:
  - Captures rows where:
    - `card_id` or `disp_id` is not numeric, or
    - `type` or `issued` is null/empty.
  - Adds `dq_date` and `dq_reason` for traceability.
- Silver table `silver.card_silver` is created with:
  - `card_id INT`
  - `disp_id INT`
  - `card_type STRING` (from the bronze `type` column)
  - `issued_at TIMESTAMP`
- The `INSERT OVERWRITE` into `silver.card_silver`:
  - Filters by numeric `card_id`/`disp_id` and non‑empty `type`.
  - Casts identifiers to `INT`.
  - Parses `issued` into a timestamp using
    `TO_TIMESTAMP(issued, 'yyMMdd HH:mm:ss')`.

Result: `silver.card_silver` now correctly contains 892 rows, and any bad
records are routed to `silver.dq_card`.
