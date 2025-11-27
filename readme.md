# Berka Banking Dataset (`./data`)

This folder contains the Berka Czech bank dataset used for the CDP Public Cloud POC. Each CSV represents an entity in the retail banking domain (clients, accounts, products, and transactions).

## Files and Schemas

- `account.csv`  
  - **Purpose:** Bank accounts held by clients.  
  - **Key fields:**  
    - `account_id` – unique account identifier.  
    - `district_id` – link to the client’s geographic district (see `district.csv`).  
    - `frequency` – statement frequency (e.g. monthly fee payments).  
    - `date` – account creation/opening date.

- `card.csv`  
  - **Purpose:** Payment cards issued on accounts.  
  - **Key fields:**  
    - `card_id` – unique card identifier.  
    - `disp_id` – link to disposition/role on an account (see `disp.csv`).  
    - `type` – card type (e.g. classic, gold).  
    - `issued` – card issuance timestamp.

- `client.csv`  
  - **Purpose:** Bank clients (customers).  
  - **Key fields:**  
    - `client_id` – unique client identifier.  
    - `birth_number` – encoded birth date / demographic key.  
    - `district_id` – home district (links to `district.csv`).

- `disp.csv`  
  - **Purpose:** Dispositions – the relationship between clients and accounts.  
  - **Key fields:**  
    - `disp_id` – unique disposition identifier.  
    - `client_id` – link to `client.csv`.  
    - `account_id` – link to `account.csv`.  
    - `type` – role of the client on the account (e.g. OWNER, DISPOSER).

- `district.csv`  
  - **Purpose:** Reference data describing geographic districts.  
  - **Notes:**  
    - Columns `A1`–`A16` capture attributes such as region name, population, unemployment, crime statistics, and other socio‑economic indicators used for segmentation and analytics.

- `loan.csv`  
  - **Purpose:** Loans associated with accounts.  
  - **Key fields:**  
    - `loan_id` – unique loan identifier.  
    - `account_id` – link to `account.csv`.  
    - `date` – loan start/approval date.  
    - `amount` – principal amount of the loan.  
    - `duration` – loan duration (in months).  
    - `payments` – regular installment amount.  
    - `status` – loan status/quality flag (e.g. paid, default).

- `order.csv`  
  - **Purpose:** Payment orders initiated from accounts.  
  - **Key fields:**  
    - `order_id` – unique payment order identifier.  
    - `account_id` – source account.  
    - `bank_to` – destination bank code.  
    - `account_to` – destination account identifier.  
    - `amount` – transfer amount.  
    - `k_symbol` – payment/contract code (e.g. SIPO, UVER).

- `trans.csv`  
  - **Purpose:** Detailed account transaction history.  
  - **Key fields:**  
    - `trans_id` – unique transaction identifier.  
    - `account_id` – link to `account.csv`.  
    - `date` – transaction posting date.  
    - `type` – direction of transaction (e.g. PRIJEM = credit, VYDAJ = debit).  
    - `operation` – operation type (e.g. deposit, withdrawal, transfer).  
    - `amount` – transaction amount.  
    - `balance` – resulting account balance after the transaction.  
    - `k_symbol` – purpose code / reference.  
    - `bank`, `account` – counterparty bank and account (if applicable).

These structures form the core relational model for the POC: clients live in districts, hold accounts, are linked via dispositions, can have cards and loans, and generate orders and transactions that drive downstream analytics, BI, and ML use cases in CDP.

## Date Conventions

- Historic dates in the original Berka CSVs have been re-based to the year 2025 so that the dataset looks “current” for the POC (for example, `930101` → `250101`, corresponding to 2025‑01‑01).  
- Date fields remain encoded as `YYMMDD` (or `YYMMDD hh:mm:ss` for card issue timestamps), preserving original month/day patterns while shifting the calendar year.

## Logical ER Model

At a logical level:
- `district` defines geographic areas for both `client` and `account`.  
- `client` is linked to `account` through `disp` (disposition), which captures each client’s role on an account.  
- `card` is issued at the `disp` level (one disposition may have zero or more cards).  
- `loan`, `order`, and `trans` are all financial events tied directly to an `account`.

```mermaid
erDiagram
    DISTRICT ||--o{ CLIENT : "has residents"
    DISTRICT ||--o{ ACCOUNT : "serves"

    CLIENT ||--o{ DISP : "has disposition"
    ACCOUNT ||--o{ DISP : "is held by"

    DISP ||--o{ CARD : "has card"

    ACCOUNT ||--o{ LOAN : "has loan"
    ACCOUNT ||--o{ ORDER : "creates order"
    ACCOUNT ||--o{ TRANS : "has transaction"

    DISTRICT {
        int district_id
        string name
        string region
    }
    CLIENT {
        int client_id
        string birth_number
        int district_id
    }
    ACCOUNT {
        int account_id
        int district_id
        string frequency
        date opened_date
    }
    DISP {
        int disp_id
        int client_id
        int account_id
        string type
    }
    CARD {
        int card_id
        int disp_id
        string type
        datetime issued
    }
    LOAN {
        int loan_id
        int account_id
        date start_date
        decimal amount
        int duration_months
        decimal payments
        string status
    }
    ORDER {
        int order_id
        int account_id
        string bank_to
        string account_to
        decimal amount
        string k_symbol
    }
    TRANS {
        int trans_id
        int account_id
        date date
        string type
        string operation
        decimal amount
        decimal balance
        string k_symbol
        string bank
        string account
    }
```

## Streaming Data Generator (Kafka)

- The Berka streaming generator sends synthetic loan, order, and transaction events to Kafka topics.  
- Script: `scripts/kafka_data_generator/berka_data_generator.py` (requires `kafka-python` and a reachable Kafka broker).

Example:
- `python3 scripts/kafka_data_generator/berka_data_generator.py \`
  ` --bootstrap-servers localhost:9092 \`
  ` --loan-topic berka_loans --order-topic berka_orders --trans-topic berka_trans \`
  ` --interval-seconds 10 --batch-size 10`

## Dimension ETL Jobs (Bronze → Silver → Gold)

All dimension ETL jobs live under `scripts/etl` and use Spark SQL only.

Bronze → Silver (Parquet, Snappy):
- District: `scripts/etl/dim_district_bronze_to_silver.py`
- Client:   `scripts/etl/dim_client_bronze_to_silver.py`
- Account:  `scripts/etl/dim_account_bronze_to_silver.py`
- Disp:     `scripts/etl/dim_disp_bronze_to_silver.py`
- Card:     `scripts/etl/dim_card_bronze_to_silver.py`

Silver → Gold (Iceberg, SCD2, Parquet, Snappy):
- District: `scripts/etl/dim_district_silver_to_gold.py`
- Client:   `scripts/etl/dim_client_silver_to_gold.py`
- Account:  `scripts/etl/dim_account_silver_to_gold.py`
- Disp:     `scripts/etl/dim_disp_silver_to_gold.py`
- Card:     `scripts/etl/dim_card_silver_to_gold.py`

Examples:
- Bronze → Silver:  
  `spark-submit scripts/etl/dim_client_bronze_to_silver.py \`
  ` --bronze-db bronze --silver-db silver \`
  ` --bronze-table client_bronze --silver-table client_silver`

- Silver → Gold (Iceberg SCD2):  
  `spark-submit scripts/etl/dim_client_silver_to_gold.py \`
  ` --silver-db silver --gold-db gold \`
  ` --silver-table client_silver --gold-table dim_client`

Run the bronze→silver jobs after NiFi has landed raw CSVs into the bronze tables; then run the silver→gold jobs to build the curated Iceberg dimensions.

## Fact Streaming Jobs (Kafka → Silver → Gold)

Three Spark Structured Streaming jobs consume Kafka topics and build fact tables:

- Loan fact: `scripts/etl/fact_loan_streaming.py`  
  - Reads from `--loan-topic` (default `berka_loans`).  
  - Writes to `silver.fact_loan_silver` (Parquet, Snappy) and `gold.fact_loan` (Iceberg).

- Order fact: `scripts/etl/fact_order_streaming.py`  
  - Reads from `--order-topic` (default `berka_orders`).  
  - Writes to `silver.fact_order_silver` and `gold.fact_order`.

- Transaction fact: `scripts/etl/fact_trans_streaming.py`  
  - Reads from `--trans-topic` (default `berka_trans`).  
  - Writes to `silver.fact_trans_silver` and `gold.fact_trans`.

Each job:
- Uses Structured Streaming with a micro-batch trigger (`--trigger-seconds`, default `30`).  
- Uses `foreachBatch` and Spark SQL to load silver and join to `gold.dim_account` before inserting into the Iceberg gold fact.  
- Requires a checkpoint location (`--checkpoint-location`) for state and exactly-once guarantees.

Example (loan fact):
- `spark-submit scripts/etl/fact_loan_streaming.py \`
  ` --bootstrap-servers localhost:9092 --loan-topic berka_loans \`
  ` --silver-db silver --silver-table fact_loan_silver \`
  ` --gold-db gold --gold-table fact_loan \`
  ` --checkpoint-location /tmp/berka_fact_loan_chk \`
  ` --trigger-seconds 30`

Recommended order:
1. Run NiFi to populate bronze tables from `./data`.  
2. Run dimension bronze→silver and silver→gold jobs to build Iceberg dimensions.  
3. Start the Kafka data generator for loans/orders/trans.  
4. Start the three fact streaming jobs to continuously populate silver and gold fact tables.

## Customer 360 (HBase)

To demonstrate a real-time Customer 360 view, a dedicated streaming job consumes live loan, order, and transaction events, enriches them with Iceberg dimensions, and writes one row per customer into HBase via REST:

- Script: `scripts/etl/customer_360_hbase_streaming.py`  
  - Reads from `--loan-topic`, `--order-topic`, and `--trans-topic`.  
  - Joins against `gold.dim_client` and `gold.dim_account` (current SCD2 versions).  
  - Produces a “latest activity” snapshot per `client_id` and upserts to HBase.

Example:
- `spark-submit scripts/etl/customer_360_hbase_streaming.py \`
  ` --bootstrap-servers localhost:9092 \`
  ` --loan-topic berka_loans --order-topic berka_orders --trans-topic berka_trans \`
  ` --gold-db gold \`
  ` --checkpoint-location /tmp/berka_customer360_chk \`
  ` --hbase-rest-url http://hbase-rest-host:8080 \`
  ` --hbase-table customer360 \`
  ` --trigger-seconds 30`
