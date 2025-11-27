Cloudera Data Platform POC Script – Banking Data Pipeline on Azure

Introduction

This proof-of-concept (POC) demonstrates an end-to-end data pipeline for a banking use case using Cloudera Data Platform (CDP) Public Cloud on Azure. It covers batch and real-time data ingestion, data lakehouse storage with Apache Iceberg, data processing with Apache Spark (orchestration via Apache Airflow), analytical querying with Apache Impala and Trino (including federated queries), business intelligence (BI) reporting with Cloudera Data Visualization, and a machine learning workflow (data analysis, model training, batch and real-time scoring, with experiment tracking using MLflow). Security, governance, and monitoring features of Cloudera (such as Schema Registry, Ranger, Atlas, etc.) are incorporated throughout to align with enterprise requirements.

We use a banking domain dataset (publicly available) that simulates typical bank data: customers, accounts, transactions, branches, etc. For example, the Wisabi Bank synthetic dataset or a similar open dataset provides multiple tables including customer details, account information, transaction logs, ATM/branch info, and reference data for transaction types. These tables contain fields such as customer demographics, account types, transaction dates/amounts, balances, branch codes, and so on ￼. This realistic dataset will drive our POC pipeline, showing how data flows from raw ingestion to final insights.

Architecture Overview: The solution follows a layered approach:
	•	Ingestion Layer: Batch ingestion is handled by Apache NiFi (via Cloudera DataFlow) for moving data from source systems or files into the platform. Apache Kafka ingests streaming data (real-time transactions) which NiFi or Spark can consume.
	•	Processing Layer: Apache Spark (via Cloudera Data Engineering) performs data cleaning, transformations, and enrichment. Workflows are scheduled and managed using Apache Airflow. Data quality checks (using Great Expectations) and schema evolution handling (via Schema Registry) are applied here.
	•	Storage & Lakehouse Layer: Processed data is stored in an Apache Iceberg table format (managed by Cloudera’s Lakehouse). Iceberg provides ACID transactions, partitioning, and time-travel for versioned data. The Cloudera Iceberg REST Catalog is used to enable external engines to access the metadata, facilitating data sharing across tools.
	•	Analytics & BI Layer: Apache Impala (in Cloudera Data Warehouse) and Trino are used for SQL analytics on the curated data (Impala for high-performance BI queries, Trino for federated querying across Iceberg and external RDBMS). Cloudera Data Visualization (CDV) is used for interactive dashboards and reports.
	•	Machine Learning Layer: Cloudera Machine Learning (CML) is utilized for developing and deploying ML models. Data scientists can explore the curated data, build models (e.g. for customer churn or fraud detection), and track experiments with MLflow. Batch scoring jobs and real-time model inference services are included to demonstrate AI consumption of the data.
	•	Governance & Security: Throughout, Cloudera SDX components ensure governance: Apache Ranger for access control and data masking, Apache Atlas (via Data Catalog) for metadata and lineage, Schema Registry for schema management, TLS encryption for data in motion, and Ranger KMS for encryption at rest.

Below, we break down the POC into stages with sample data, configuration steps, and expected outcomes.

1. Dataset and Sources (Banking Domain Sample Data)

For this POC, we assume a synthetic banking dataset with multiple related tables, for example:
	•	Customers: Basic customer info (CustomerID, Name, Age, Gender, etc.).
	•	Accounts: Account details (AccountID, CustomerID, Account Type – e.g. savings or checking, Opening Date, BranchCode). Each customer might have one or more accounts.
	•	Transactions: Transaction log (TransactionID, AccountID/CustomerID, Transaction Date, Transaction Type, Amount, resulting Balance, Description). This includes deposits, withdrawals, transfers, etc., and links to accounts and branches.
	•	Branches: Reference table for branch codes (BranchCode, Branch Name, Location).
	•	TransactionTypes: Reference table for transaction type codes (TypeID, Type Description – e.g. “ATM Withdrawal”, “Online Transfer”, “Deposit”).

These tables can be represented as CSV files or a small relational database, simulating sources like core banking systems or CSV exports. The dataset is anonymized and representative (no real PII). For instance, a sample transaction record might look like:

CustomerID: C12345  
AccountID: A-98765  
TransactionDate: 2025-01-15 10:35:22  
TransactionType: ATM Withdrawal  
Amount: 200.00  
PostBalance: 800.00  
BranchCode: BR002  
Description: "ATM cash withdrawal"

The POC will ingest and integrate such data from multiple sources:
	•	Batch source: Static tables (Customers, Accounts, Branches, etc.) as CSV files stored in Azure Data Lake Storage (ADLS) or coming from a relational database dump.
	•	Real-time source: A stream of new Transactions events (e.g., simulated in Kafka, representing real-time transaction feed from ATM network or core system).

By combining these, we can demonstrate a 360° view of the banking data through the pipeline.

2. Batch Ingestion with Apache NiFi (Cloudera DataFlow)

Apache NiFi will be used to ingest batch data from various sources into the data lake:
	•	We configure NiFi flows for each static data source (e.g., Customers and Accounts CSV files, Branches reference data). NiFi’s processors like GetFile/FetchFTP (for files) or QueryDatabaseTable (for relational DB extracts) read the source data.
	•	Data is routed through NiFi to land in the Bronze layer (raw zone) of our storage, which in this Azure-based scenario is ADLS Gen2 or directly into Hive/Impala tables. For example, NiFi can deliver the CSV data into HDFS/ADLS and then trigger a Hive table load, or use a PutHiveStreaming/PutDatabaseRecord processor to insert into a managed table. In our case, we land raw data as initial CSVs in ADLS under a “bronze” path.

Schema Enforcement: We integrate NiFi with Schema Registry (part of Cloudera DataFlow) for managing schemas of incoming data. Each dataset’s schema (e.g., the structure of the Customer table) is stored in the registry. This ensures that if the source schema changes (e.g., a new column “customer_segment” is added to Customers), NiFi will detect a new schema version:
	•	The Schema Registry enforces compatibility rules (we typically set backward compatibility so consumers can handle older data). For instance, if a new field os_type is added to a Kafka event schema, we would register a new version with a default value for that field ￼. This allows producers to send events with the new field while older consumers (like our pipeline) can continue reading without failing, using the default for missing fields.
	•	NiFi processors like ConvertRecord can fetch the schema from the registry to parse incoming data and convert to the desired format (e.g., Avro/JSON to ORC/Parquet for Hive).

Data Quality Checks: As data flows through NiFi, we incorporate Great Expectations validations:
	•	We add a custom processor or a script (via ExecuteScript) that runs Great Expectations tests on the data (for example, verifying that account numbers match a certain pattern, or transaction amounts are non-negative, etc.).
	•	If a batch of data fails an expectation (e.g., null values in a field that should be not-null), NiFi routes those records to a separate error queue (perhaps a different file location or Kafka topic for bad records). We also log or send alerts for these events. This ensures only high-quality data passes through.

Encryption in Transit & At Rest: Cloudera DataFlow (NiFi) is configured with end-to-end security:
	•	Data in transit between NiFi and other services (Kafka, HDFS, etc.) is encrypted using TLS. NiFi runs within a secured CDP environment, and all its processor communications (HTTP, RPC, etc.) use HTTPS.
	•	For data at rest, we rely on Azure’s encryption for ADLS and also Cloudera’s Ranger KMS for key management. NiFi’s flow definitions and any sensitive parameters are stored encrypted in the Cloudera DataFlow catalog. This means that if our NiFi flow credentials or config are persisted, they remain encrypted on disk.
	•	Each dataset landing in storage can also be encrypted at rest (transparent to NiFi) via Ranger KMS-managed keys. This adheres to banking security standards.

NiFi Flow Monitoring: We enable NiFi’s built-in monitoring to ensure the batch jobs run correctly:
	•	NiFi UI provides back-pressure indicators and stats on how many records have been processed. We verify there are no stuck queues after ingestion.
	•	We use the MonitorActivity processor to watch critical parts of the flow (for example, if no files have been picked up in X minutes when expected, or if no data is transferred, indicating a potential hang). The MonitorActivity can send an alert (e.g., via email or put a message to an “alerts” Kafka topic) if a flow is inactive or if any processor reports an error.
	•	Additionally, NiFi’s Data Provenance tracking is enabled, so we can trace every record’s path—from input to final storage—providing lineage for audit and debugging.
	•	We will regularly check NiFi bulletin board for any error messages. For the POC, we might also set up simple Slack/email notifications for failures. (In a production setup, this can be integrated with Cloudera Manager or external monitoring tools for centralized alerts.)

After this step, our static data (customers, accounts, branches, etc.) should be available in the data lake in raw form. We register these raw datasets in the Cloudera Data Catalog (which is backed by Apache Atlas) automatically, capturing technical metadata (schema, file location) and initial lineage (source -> target details).

3. Real-Time Ingestion with Apache Kafka

For streaming data (e.g., real-time transactions), we use Apache Kafka as the ingestion backbone. The POC sets up one or more Kafka topics, for example:
	•	transactions_stream: receives a continuous stream of transaction events (in JSON or Avro format) from a simulated source (could be a dummy generator or an upstream system publishing events). Each event contains fields like transaction ID, account, timestamp, amount, etc.

We produce messages to this topic to simulate live transactions. Now, there are two ways to bring this streaming data into our platform:
	1.	NiFi Kafka Consumers: NiFi can act as a Kafka consumer using the ConsumeKafkaRecord_2_0 (or similar) processor. It will subscribe to transactions_stream, retrieve messages in batches, decode them (with Schema Registry’s help for Avro schemas), and then route them to the storage layer. NiFi could, for instance, write these incoming transactions directly into an Apache Kudu table or append to an Iceberg table (via a staging file and commit process).
	2.	Spark Structured Streaming: Alternatively, we can use Spark streaming jobs running in Cloudera Data Engineering to consume the Kafka topic. In this POC, we’ll demonstrate using Spark for transformation, so we could have a Spark job that reads from Kafka in micro-batches, does some minimal processing, and writes to the Silver layer (cleaned transaction table).

Handling Schema Evolution in Kafka: As noted, the Schema Registry is key for streaming. We register the Transaction event schema (with fields like date, amount, etc.) in the registry. If the schema evolves (e.g., a new optional field is added), Kafka producers will send events with a schema ID referencing the new version. Both NiFi and Spark can fetch the schema by ID and decode accordingly:
	•	Our consumer logic ensures backward compatibility: if an older consumer has not yet been updated to know the new field, the Schema Registry (with Avro) ensures the new field has a default (or is optional) so deserialization doesn’t break. In our POC, we simulate this by adding a dummy field in a later batch of events to show that the pipeline continues smoothly (with the new field perhaps just ignored or logged until we explicitly handle it in code).

Streaming Ingestion to Bronze Layer: Initially, we treat the incoming Kafka data as Bronze (raw) and then will refine it. For quick ingestion, NiFi or Spark can append raw events into a data lake store:
	•	If using NiFi: it could accumulate a certain number of messages or size and then write out a file (e.g., hourly) to ADLS, or directly insert into a Hive external table partition (like one partition per day or hour).
	•	If using Spark: we can have a Structured Streaming job writing to an Iceberg table incrementally. Iceberg supports streaming upserts or inserts; in our case, each transaction is an insert (no updates to past events).

After this step, we have a continuous feed of transaction data being landed. This simulates real-time data availability for downstream consumption.

4. Data Lakehouse Storage with Apache Iceberg

All ingested data (batch and streaming) will be stored using Apache Iceberg table format in our Silver and Gold layers. Iceberg is a modern table format that works on cloud storage (like ADLS) and supports ACID operations, partitioning, schema evolution, and time travel. In the Cloudera environment, Iceberg tables can be managed by Hive/Impala and accessed by different engines.

Bronze, Silver, Gold Definition:
	•	Bronze Layer: Raw data as ingested, stored in landing tables or files (could be considered the initial Hive/Iceberg tables reflecting raw source with minimal or no transformation).
	•	Silver Layer: Cleaned and conformed data. We will transform raw data to Silver using Spark jobs (next section). This includes data cleaning, joining reference data, and enforcing a unified schema (e.g., ensuring data types and formats are consistent). The Silver layer is typically in a Hive database like bank_silver, containing tables like customers_clean, accounts_clean, transactions_clean.
	•	Gold Layer: Aggregated or enriched data ready for analytics. For example, a Gold table might be customer_transactions_summary (one row per customer with KPIs like total balance, number of transactions last month, etc.), or fraud_alerts if we were flagging suspicious transactions. Gold layer tables are what BI and ML primarily query.

In our POC, we define an Iceberg table for each of the key data entities. For instance, transactions_clean is an Iceberg table partitioned by date (e.g., transaction_date or month) for performance. Partitioning by date allows query engines to prune data and scan only relevant partitions when analyzing a specific time range, drastically improving performance on large volumes. We also partition possibly by branch or region if that’s a common filter in analytics.

Creating and Configuring Iceberg Tables: We use Impala or Spark SQL to create the Iceberg tables in our Hive Metastore, for example:

CREATE TABLE bank_silver.transactions_clean (
    transaction_id STRING,
    account_id STRING,
    customer_id STRING,
    transaction_timestamp TIMESTAMP,
    transaction_type STRING,
    amount DECIMAL(15,2),
    post_balance DECIMAL(15,2),
    branch_code STRING,
    description STRING
)
PARTITIONED BY (date(transaction_timestamp))  -- partition by transaction date (e.g., day)
STORED AS ICEBERG;

We choose Parquet as the file format for Iceberg (the default), which gives columnar storage and compression. For optimization, we enabled spark SQL configurations for writing:

SET spark.sql.catalog.mycat.dynamic.partition=true;
SET spark.sql.catalog.mycat.dynamic.partition.mode=nonstrict;

(similar to Hive’s dynamic partitioning) so that as we load data, partitions are created on the fly for each date.

Data Ingestion into Iceberg: Now, using Spark or NiFi, we load data into these tables:
	•	For batch data (Customers, Accounts), we can do a simple insert-overwrite into the Iceberg tables from the raw files. For example, a Spark batch reads the raw CSV, applies transformations (like trimming strings, validating referential integrity between accounts and customers), then writes to customers_clean table.
	•	For streaming transactions, if we set up Spark Structured Streaming on the transactions_stream, we can write stream output directly into the transactions_clean Iceberg table. Iceberg’s format will handle concurrent writes by creating new data files and transaction snapshots.

Schema Evolution in Iceberg: One advantage of Iceberg is easy schema evolution. If we later decide to add a new column (say, transaction_channel to mark ATM vs online), we can ALTER TABLE to add it and Iceberg will track schema versions. Old data files won’t have that column (implicitly null) and new data will. Both Impala and Trino can query the table and see the latest schema.

Iceberg Table Features:
	•	ACID and Snapshots: Each commit to an Iceberg table creates a new snapshot. We can use time travel queries to access historical data states. For example, to validate data or reproduce a report from last week, one could query SELECT * FROM transactions_clean FOR SYSTEM_TIME AS OF '2025-11-10 00:00:00' to see the table as it was on Nov 10, 2025 (if snapshots are kept). This is valuable for audit and debugging.
	•	Partition Pruning: Query engines will automatically skip irrelevant partitions. For example, if a BI query filters WHERE transaction_date >= '2025-11-01' AND transaction_date < '2025-12-01', only November 2025 data files are read, not the entire dataset.
	•	Optimized Storage: We can periodically compact small files in Iceberg (especially if streaming creates many small files) using Spark jobs or Iceberg actions, to maintain read efficiency. We can also sort data within partitions for better compression.

Cloudera Lakehouse (Iceberg) Integration: Cloudera’s platform provides the Lakehouse Optimizer which helps manage these Iceberg tables and ensures Impala and other tools can use them effectively. In CDP, Impala now has support for Iceberg tables (as external tables). We also deploy Cloudera Iceberg REST Catalog as a service. This REST Catalog exposes our Iceberg table metadata through a REST API, enabling external clients or other platforms to securely access the table. For instance, Trino can be configured to use the Iceberg REST Catalog to query the same Iceberg tables without needing direct access to the Hive Metastore. This also means if another team or a different compute platform (like Databricks or AWS Athena) needed access to the data, they could use this REST interface to share the data easily ￼.

By this point, our Silver layer is populated: we have clean Customers, Accounts, Branches, Transactions tables in Iceberg format. The data is ready for analytical queries and further processing.

5. Data Processing & Workflow Orchestration (Spark & Airflow)

The transformation of raw data to the curated Silver/Gold layers is done using Apache Spark for heavy data lifting, and Apache Airflow for orchestration. Cloudera Data Engineering (CDE) allows us to run Spark jobs on demand or on a schedule, and it integrates Airflow for pipeline orchestration.

ETL with Spark: We implement data transformation jobs as PySpark or Spark SQL applications:
	•	A job to process Customers & Accounts: Reads raw customers and accounts from Bronze (maybe landing CSVs or initial tables), joins or cleans them (e.g., ensure each account’s customer exists, unify date formats, categorize ages into groups if needed for analytics). Writes out to the customers_clean and accounts_clean Iceberg tables (Silver). Possibly also create a customer_account combined view for convenience.
	•	A job to process Transactions: Could be a streaming job as described, or we may also run a batch job that takes daily new transactions (from a staging area) and performs enrichment. Enrichment could include: looking up branch details to add branch location, deriving additional columns (e.g., flag large transactions over some threshold, or calculate running totals per account). This job writes to transactions_clean (Silver).

These Spark jobs are developed in CDE and we utilize Cloudera’s built-in ML experiments tracking for the data processing as well (though typically MLflow is for model experiments, not needed here).

Quality and Error Handling: Within Spark ETL, we also use Great Expectations as needed. For example, after transforming transactions, we may include an Expectation Suite to verify the distribution of transaction amounts or that each transaction’s customer and account exist in the dimension tables. If an expectation fails, the Spark job can either log an error and continue (if non-critical) or raise an exception to fail the job (triggering a retry, see below). All data quality metrics can be sent to a dashboard for monitoring (see Pipeline Monitoring section).

Airflow Orchestration: We create an Airflow DAG (Directed Acyclic Graph) to coordinate the steps:
	•	Step 1: Run the NiFi batch ingestion (this could be just a sensor to ensure NiFi completed or NiFi could trigger downstream jobs via an API call).
	•	Step 2: Run the Spark job for Customers/Accounts. If those depend on NiFi output, ensure to schedule after ingestion is done or use a dependency.
	•	Step 3: Run the Spark job for Transactions (batch or ensure streaming job is up).
	•	Step 4: Once Silver layer tables are updated, run any Gold layer aggregation jobs. For example, an Airflow task for a Spark job that aggregates the latest data into summary tables (like daily transaction aggregates per branch or customer).
	•	Step 5: Optionally, run a machine learning batch scoring job (if we have a trained model, apply it to new data; more on ML below) or any validation reports.

We parameterize this pipeline for reusability. For instance, the DAG could accept a parameter for processing_date to easily rerun for a particular date or to backfill data.

Scheduling: The Airflow DAG is scheduled to run daily for batch parts (e.g., end-of-day processing for the daily new transactions and to refresh aggregates). Meanwhile, the real-time streaming job is continuously running in the background (not on a schedule but as a long-lived job consuming Kafka).

Error Handling & Retries: In Cloudera Data Engineering, we enable automatic job retries for Spark jobs:
	•	If a Spark job fails (e.g., due to a transient issue like a connection timeout or a data anomaly causing an exception), the platform will retry it up to a configured maximum (say 3 retries) with a delay (maybe a few minutes)【?】. We set the retry parameters in the job configuration. For Airflow tasks, we also configure retries and retry_delay at the DAG task level.
	•	We allow concurrent retries if needed (though Airflow by default will not run the next scheduled run if one is still in progress; but we can configure separate execution slots for retry attempts). This ensures that a stuck job doesn’t block the pipeline longer than necessary.
	•	Any failures or timeouts also send alerts (via email or monitoring systems). CDE’s UI and logs allow us to quickly identify the root cause (we can inspect Spark driver logs, etc.).

Monitoring Workflows: We use Cloudera’s monitoring and Azure integration:
	•	The Cloudera Management Console shows us job statuses and historical runs. We check there to see if jobs are succeeding within expected time. It also provides metrics like runtime, which we use to detect slowdowns (if a job that usually takes 5 min suddenly takes 15, that’s a flag to investigate).
	•	We also integrate Azure Monitor for infrastructure metrics (CPU, memory of the Spark cluster, etc.). We set up Azure Monitor alerts for unusually high resource usage or node failures.
	•	Within Airflow, the web UI provides a quick glance at task success/failure, and we set up email on failure for critical tasks.

By the end of this processing stage, our Silver and Gold datasets are consistently updated and ready for consumption. We have implemented a robust ETL pipeline with scheduling, error recovery, and data validation, ensuring reliable data each day.

6. Analytical Queries with Impala and Trino (BI and Federated Queries)

With curated data in the Iceberg tables, business analysts and BI tools can query the data using SQL. We utilize two engines for demonstrating flexibility:
	•	Apache Impala (via Cloudera Data Warehouse): for fast BI analytics directly on the Iceberg tables.
	•	Trino (formerly PrestoSQL): for federated querying, accessing both the Iceberg data and an external data source (another RDBMS) simultaneously.

Using Impala on Iceberg:
Impala is configured to use the same Hive Metastore (and the Iceberg tables we created). In Cloudera CDP, we can spin up an Impala Virtual Warehouse with Iceberg support enabled. Analysts can connect via Hue (the SQL editor) or JDBC (from tools like Power BI or Cloudera Data Viz) to run queries. For example:

-- Query total deposits per branch in the last 7 days
SELECT b.branch_name, COUNT(*) as num_deposits, SUM(t.amount) as total_deposited
FROM bank_silver.transactions_clean t
JOIN bank_silver.branches b on t.branch_code = b.branch_code
WHERE t.transaction_type = 'Deposit'
  AND t.transaction_timestamp >= DATE_SUB(current_date(), 7)
GROUP BY b.branch_name
ORDER BY total_deposited DESC;

Impala will leverage the partitioning on transaction_timestamp to only scan recent data, and use its MPP capabilities to compute the aggregates quickly. Thanks to Iceberg, this query sees the latest committed data, even if streaming writes were happening (snapshot isolation ensures readers don’t see partial data).

We also demonstrate Impala’s auto-scaling: if multiple analysts run heavy queries concurrently, the Impala service can scale out. Cloudera’s Workload XM/auto-scaling feature (when enabled with the COMPUTE_PROCESSING_COST query option) will monitor query resource utilization ￼. In our POC, we might simulate 10 concurrent queries via a script; the Impala Autoscaling Dashboard would show additional executors being provisioned to handle the load, and scale back down when idle. This ensures consistent performance without over-provisioning resources permanently.

Federated Querying with Trino:
While Impala excels at querying data within our lakehouse, Trino allows combining data from disparate sources. We set up a Trino cluster (either within CDP if supported or externally) and configure catalogs:
	•	An Iceberg catalog pointing to our Iceberg REST Catalog (or directly to the Hive Metastore). This gives Trino access to the same bank_silver.transactions_clean table and others.
	•	A JDBC catalog for an external relational database. For example, suppose the bank has a legacy Oracle or MySQL database containing some customer metadata or reference data not yet moved to the lake. We configure Trino to connect to that RDBMS (using the appropriate connector). In the POC, we can simulate this with a small MySQL instance containing, say, a vip_customers table or additional customer info.

Using Trino, we can perform a join across Iceberg and the external DB in one SQL query. For example:

SELECT c.customer_id, c.name, c.total_balance, ext.credit_score, ext.risk_segment
FROM hive.bank_silver.customers_agg c
JOIN mysql.customer_risk ext ON c.customer_id = ext.customer_id
WHERE c.total_balance > 1000000 AND ext.risk_segment = 'High';

This query might find high-value customers who are also high risk, by joining data in our cloud warehouse (customers_agg from Iceberg, served via Hive catalog) with data still in a MySQL (customer_risk via the MySQL connector). Trino will fetch data from both sources in parallel and perform the join, giving a seamless view. This showcases data federation – the ability to query data without consolidating it physically first.

Trino can also query other sources like S3 data, other Hive metastores, or NoSQL, but our focus is the RDBMS federation. The Cloudera environment supports connecting external sources through SDX as well (e.g., using Cloudera Shared Data Experience to register and secure external datasets, then query via Trino or Impala if external tables are defined).

Performance Note: For large joins, performance depends on network and data volumes. In practice, we’d aim to offload as much as possible to the lakehouse, but the POC demonstrates the capability. We ensure that appropriate filters are pushed down (Trino will attempt to push predicates to the MySQL source where possible, such as filtering risk_segment on the DB side, to minimize data transfer).

All queries executed (both Impala and Trino) are logged and captured in Cloudera Navigator/Atlas lineage metadata. This means we can trace which tables and columns were used by which reports or queries – valuable for impact analysis and compliance.

7. BI Reporting with Cloudera Data Visualization

With the Gold layer prepared, we set up interactive dashboards using Cloudera Data Visualization (CDV), which is part of Cloudera’s platform and provides rich BI capabilities. In this POC:
	•	We connect CDV to our Impala Virtual Warehouse (or directly to the Iceberg table through Impala or a dataset in CDV). We create a dataset in CDV based on, say, a view or table that contains analytics-ready data (for example, a daily transactions summary or customer portfolio view).
	•	We design a few dashboard widgets. For instance:
	•	Transaction Volume by Type – a bar chart or time series showing counts of withdrawals, deposits, transfers over time.
	•	Total Deposits by Branch – a geo-map or tree map highlighting which branches or regions handle the most deposits.
	•	Customer Demographics – a pie chart of customer segments (if such info is available, e.g., number of customers by age group or by account type).

CDV allows us to filter and drill down into data. We ensure some charts show near real-time updates:
	•	By enabling auto-refresh on certain visuals, the dashboard can poll for new data every 60 seconds (this is configurable). For example, a real-time transactions ticker can update to show the last 5 minutes of transactions continuously.
	•	We also consider using Materialized Views in Impala for expensive aggregations. Suppose we have a widget for “Monthly transaction volume by branch (last 1 year)”. Computing this on the fly might be heavy, so we create a Materialized View in Impala to pre-aggregate transactions by branch and month. Impala will automatically use this MV to answer the query quickly, and we refresh the MV as part of our pipeline (maybe nightly after loading new data).

Near Real-Time Data Display: If truly low-latency updates are needed (sub-minute), another approach is using Cloudera DataFlow data previews or building a small web app. But in our case, CDV with 1-minute auto-refresh is sufficient to showcase streaming data integration.

Power BI Integration (Optional): While CDV is primary, we note that external BI tools like Power BI or Tableau could also connect using the Impala or Trino JDBC drivers. Our Iceberg-based tables are just another data source to them. In a hybrid scenario, one could use Power BI connected live to Trino, and thus combine results from the cloud data and on-prem data in one Power BI report as well. However, for simplicity, we demonstrate using CDV for a unified experience within Cloudera.

8. Machine Learning Pipeline with Cloudera Machine Learning (CML)

Beyond BI, we want to showcase how the curated data can fuel AI/ML use cases. Cloudera Machine Learning provides an interactive environment (workbench) for data scientists to develop, train, and deploy models using the data in our lakehouse. Key steps in the ML workflow:
	•	Data Analysis & Feature Engineering: In CML, we launch a Python notebook (Jupyter-style). We use Spark or Python libraries (Pandas, etc.) to explore the Gold layer data. For example, we might create a training dataset of customers with features like average monthly deposit, number of transactions, account age, etc., to predict something like customer churn or the likelihood to buy a new product.
We can directly query Impala from Python (via Impyla or JDBC) or use the Spark session to load data. The data is large, but since it’s in the cluster, operations are efficient. We ensure any sensitive data (like customer IDs) are properly masked or anonymized via Ranger policies so that the data scientist only sees permitted columns (e.g., no raw SSN or similar if it existed).
	•	Model Training: We implement a machine learning model using scikit-learn or PySpark MLlib. For instance, train a classification model that predicts whether a customer will convert on a marketing offer (using our marketing or transaction history data), or a fraud detection model that flags anomalous transactions. The training code would split data into train/test, train the model (e.g., Random Forest or XGBoost), and evaluate performance (AUC, accuracy, etc.).
We enable experiment tracking with MLflow: each time we run a training experiment, MLflow (which CML can use under the hood) logs parameters, metrics, and the model artifact. This is done by calling mlflow.start_run() in the code and logging relevant info. Cloudera’s ML workspace has an Experiments UI where we can compare runs, since it leverages MLflow Tracking server internally.
	•	Model Registry & Deployment: After selecting the best model, we register it (either in MLflow Model Registry or CML’s model catalog). The POC then demonstrates two ways of using the model:
	1.	Batch Scoring: We create a Spark or Python job to apply the model to a batch of data. For example, score all customers and output a risk score or churn probability for each (writing results to a new Gold table such as customer_churn_scores). This job can be scheduled via Airflow as part of a nightly process or triggered on-demand.
	2.	Real-Time Scoring: We deploy the model as a REST API using CML’s REST Deployment feature or as a lightweight Flask server. In Cloudera ML, it’s straightforward to deploy a trained model: the service will containerize it and expose an endpoint. For example, a “Fraud Detection API” that, given transaction details, returns a fraud probability. We integrate this with the streaming pipeline: NiFi or Spark can call the API for each incoming transaction (perhaps using NiFi InvokeHTTP processor or Spark’s foreachBatch calling the REST endpoint) to get a score. If the score exceeds a threshold, NiFi could route that transaction to an “alerts” Kafka topic or send an email. This demonstrates real-time inference.
	•	Monitoring Models: We track the performance of the model in production. For example, log the prediction vs actual outcomes (when actuals become known) to detect model drift. CML/MLflow can log these and we could visualize with CDV or Grafana. We ensure that lineage is captured: Atlas will show that the customer_churn_scores table was produced by a certain ML model which in turn was trained from these source tables, providing end-to-end traceability from raw data to AI insight.
	•	Security in ML: Using Apache Ranger, we enforce that the data scientists have only access to the needed data and nothing more (table/column level controls as mentioned). If PII is present, we apply tokenization or masking policies – e.g., mask full account numbers except last 4 digits when data is accessed in ML or BI contexts. This way models can be built without exposing sensitive info. The data used for ML remains governed.

The ML part of the POC shows how CDP can accelerate AI development by bringing the computation to the data (no need to export data to external ML environments). The integration of MLflow ensures reproducibility and experiment tracking which is crucial in regulated environments (we can always know which model version was used to score customers on a given day, aiding audit).

9. Governance, Security, and Lineage

Throughout the POC scenario, we adhere to strict governance which is critical in banking:
	•	Metadata and Catalog: As data is ingested and processed, Cloudera Data Catalog (Atlas) automatically captures metadata. All tables in Bronze/Silver/Gold are registered with their schema, descriptions, and tags (we add business descriptions, e.g., mark transactions_clean as containing financial transaction records). Data Catalog also captures lineage graphs – for example, one can visualize that the transactions_clean table is derived from transactions_raw (ingested by NiFi from Kafka) and enriched with branches and accounts data, and further feeds the monthly_branch_stats table and the ML model. This end-to-end lineage view can be seen in Atlas or via Cloudera’s partnership tooling like Cloudera Octopai for more advanced lineage analytics. With lineage, we can perform impact analysis: if the schema of transactions_raw changes or a data quality issue is found, we can quickly identify all downstream processes (ETL jobs, reports, models) that use that data ￼ ￼.
	•	Role-Based Access Control (RBAC): Apache Ranger is configured with fine-grained policies:
	•	Table-level and database-level permissions ensure, for instance, that analysts can only SELECT from Gold/Silver tables, not drop or alter them, and they cannot see Bronze raw data unless authorized (perhaps raw data might contain sensitive elements).
	•	Column-level masking is applied for PII. For example, if our customers table had a column for full name or SSN, we could apply a masking policy for any user not in the compliance role – showing only the last 4 characters or a hashed value. In our case, since the data is anonymized, we can simulate this by masking something like an email or phone number field if it existed ￼. This demonstrates that even if a user queries the customer table, they cannot retrieve personal identifiers without proper role clearance.
	•	Ranger auditing is turned on: every access (query) to sensitive tables is logged. If someone without permission tries to access, Ranger will block it and log an “access denied” event. Data Stewards can review these logs to ensure no unauthorized access attempts and to meet compliance reporting (who accessed what data when).
	•	Encryption: We already discussed TLS and at-rest encryption. Additionally, all credentials (database passwords, API keys for ML deployment etc.) are stored securely (never in plaintext in code or NiFi flows). We leverage Azure Key Vault integrated with Ranger KMS where applicable.
	•	Audit & Stewardship: We assign a Data Steward for each domain (e.g., a steward for Customer data, another for Transaction data). Using Atlas, they tag sensitive fields and define data classification (public, confidential, etc.). They regularly audit Atlas and Ranger reports. For example, Ranger’s audit might show that a particular user’s query was denied access to transactions_clean.amount field due to masking – a steward can investigate if that access was appropriate or if policy changes are needed.
Stewardship also involves verifying lineage and quality: e.g., verifying that all expected data sources arrived (if a day’s transactions are missing, that’s alerted). Cloudera provides tools and services to help set these up, but in POC we simulate by showing the logs and Atlas UI screenshot (if possible).
	•	Data Lineage Visualization: We generate a lineage diagram from source to consumption. This can be done in Atlas or extracted via an API. It would show boxes for “Kafka topic -> NiFi flow -> transactions_raw table -> Spark ETL -> transactions_clean Iceberg table -> Impala view -> Dashboard chart” etc. This visual confirms compliance that we know the provenance of all data appearing in a report. In an actual demo, we could present Atlas’s lineage graph for one of the Gold tables.

All these governance measures ensure that the pipeline not only delivers data, but does so in a controlled, secure, and auditable manner — a must for banking data.

10. Additional Considerations and Next Steps

High Availability and Disaster Recovery: In a production scenario, we would deploy this across multiple Azure Availability Zones for resilience. Kafka clusters would be set up with brokers in different AZs and replication factor such that if one AZ goes down, consumers can read from the other replicas (ensuring no data loss). NiFi can be run in a stateless mode or with clustering to avoid downtime. Impala Virtual Warehouses can also be configured with multiple coordinators for HA. For cross-region disaster recovery (e.g., failover from Azure Singapore to Azure Malaysia region), we would rely on backup/restore and possibly streaming replication of data (Cloudera does not auto-failover across regions out of the box). However, snapshots of HDFS/Iceberg and database backups can be used to meet RPO/RTO requirements. For example, we might schedule daily snapshots of the Iceberg tables (metadata and data) and log them to a DR storage. Our RPO could be a few hours and RTO maybe a couple of hours to restore services in another region, depending on the strategy. Cloudera’s platform recovery tools like HBase snapshots (if we used HBase) and Cloudera Backup & Disaster Recovery service would come into play for stateful parts of the stack.

Cost Management: Running such a pipeline in the cloud incurs cost, so we enable cost tracking. The Cloudera Management Console provides visibility into CPU/memory hours consumed by each service (translated into CDP resource credits, if using CDP Public Cloud). We use this to attribute cost to projects – e.g., how much of the compute was used by BI vs ML. Additionally, we hook into Azure Cost Management to get the overall picture including VM, storage, and network egress costs. If needed, we implement a simple chargeback scheme: e.g., project A used X nodes for Y hours for ML training – that cost is allocated to that department. With the insight from Cloudera and Azure, we ensure the POC stays within budget and can scale efficiently.

Infrastructure as Code: The entire environment setup can be automated with Terraform templates. For instance, we define Azure resources (VNets, subnets, security groups for the cluster), and use Cloudera’s Terraform provider or API to deploy the CDP services (DataHub clusters for NiFi, Kafka, etc., and CML workspace). This IaC approach makes the deployment repeatable across Dev, UAT, Prod. We containerize custom code where possible (for example, if we had a custom NiFi processor or a specific ML serving code, those could be containerized for portability). Using Terraform and Kubernetes (for CML and potentially to deploy something like Trino as a containerized service) provides cloud abstraction and easier scaling. The POC environment is minimal (perhaps 3 nodes for NiFi/Kafka, a small Data Engineering cluster for Spark, etc.), but Terraform can later scale this to a larger prod environment seamlessly.

Dev → UAT → Prod Promotion: We treat this POC as the Dev phase. For promotion, we would:
	•	Use Git for version controlling NiFi flow definitions (export flows to JSON templates) and Spark/Airflow code.
	•	Set up a UAT environment in CDP (maybe in a separate tenant or just logically separate) where we deploy the same artifacts and run tests with sample data.
	•	After user acceptance, deploy to Prod environment (with larger clusters, real data connections). Cloudera’s tools support exporting and importing Data Visualization dashboards, NiFi flows, and even Ranger policies between environments (via API or backing up the SDX policies). We ensure the configurations (like connection endpoints, scaling configs) are adjusted for prod. This CI/CD approach ensures reliability when moving pipelines to production.

Scalability & Future Roadmap: In the first iteration (year 1), the focus is on getting all components working (ingestion, basic transformations, BI, ML). As data volume and use cases grow (years 2-3), we would optimize and expand:
	•	Optimize partitioning strategies, maybe adopt bucketting or clustering on certain keys if queries often look at specific customers.
	•	Leverage more streaming analytics (e.g., using Kafka Streams or Flink for real-time aggregations to detect anomalies within seconds).
	•	Introduce new data sources like clickstream data or mobile app logs to enrich our 360° customer view.
	•	By year 4-5, incorporate advanced analytics: perhaps deploying real-time dashboards on streaming data (with technologies like Druid or directly using Kafka + CDV for live data), and more complex AI (like using NLP on transaction descriptions to classify expenses, or integrating with a GenAI service for a chatbot that answers customer queries based on data, etc. since Cloudera is bringing AI to data anywhere).
	•	Multi-cloud or on-prem integration: possibly extend the platform to also ingest data from on-prem systems via Cloudera DataFlow and use Azure Arc or similar for a hybrid setup, if needed for compliance (some sensitive data might remain on-prem but can be virtually integrated via our pipeline).

This POC is built with production in mind, so each component can be scaled out. For instance, the Kafka and NiFi flows can handle higher TPS by adding more partitions and NiFi nodes; the Spark jobs can run on larger clusters or using autoscaling; Impala and Trino can be scaled for more concurrency or data volume.

Conclusion:
In summary, this 4-8 page POC script has walked through a comprehensive scenario of a banking data pipeline on Cloudera CDP Public Cloud (Azure). We covered everything from raw data ingestion (NiFi, Kafka) to data lakehouse management (Iceberg, Impala, Trino), to delivering insights via BI dashboards and machine learning models, all under a framework of strong data governance and security. The chosen sample dataset and use cases illustrate how a bank can integrate multiple data sources and analytic workloads on a single platform. With this POC, stakeholders can visualize how Cloudera’s modern data platform meets their needs for streaming and batch processing, real-time analytics, and AI, while ensuring compliance in a highly regulated industry. The next steps would be to execute this plan in a controlled environment, measure its performance, and refine it for a production rollout.