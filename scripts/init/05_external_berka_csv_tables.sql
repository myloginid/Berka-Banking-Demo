-- External tables on ADLS Gen2 (ABFS) for raw Berka CSV files.
-- All columns are typed as STRING and the header row is skipped.

CREATE DATABASE IF NOT EXISTS csv;

DROP TABLE IF EXISTS csv.district_external;

CREATE EXTERNAL TABLE csv.district_external (
  A1   STRING,
  A2   STRING,
  A3   STRING,
  A4   STRING,
  A5   STRING,
  A6   STRING,
  A7   STRING,
  A8   STRING,
  A9   STRING,
  A10  STRING,
  A11  STRING,
  A12  STRING,
  A13  STRING,
  A14  STRING,
  A15  STRING,
  A16  STRING
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/tablespace/external/hive/district'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS csv.client_external;

CREATE EXTERNAL TABLE csv.client_external (
  client_id    STRING,
  birth_number STRING,
  district_id  STRING
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/tablespace/external/hive/client'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS csv.account_external;

CREATE EXTERNAL TABLE csv.account_external (
  account_id   STRING,
  district_id  STRING,
  frequency    STRING,
  created_date STRING
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/tablespace/external/hive/account'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS csv.disp_external;

CREATE EXTERNAL TABLE csv.disp_external (
  disp_id    STRING,
  client_id  STRING,
  account_id STRING,
  type       STRING
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/tablespace/external/hive/disp'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS csv.card_external;

CREATE EXTERNAL TABLE csv.card_external (
  card_id STRING,
  disp_id STRING,
  type    STRING,
  issued  STRING
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/tablespace/external/hive/card'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS csv.loan_external;

CREATE EXTERNAL TABLE csv.loan_external (
  loan_id    STRING,
  account_id STRING,
  loan_date  STRING,
  amount     STRING,
  duration   STRING,
  payments   STRING,
  status     STRING
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/tablespace/external/hive/loan'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS csv.order_external;

CREATE EXTERNAL TABLE csv.order_external (
  order_id   STRING,
  account_id STRING,
  bank_to    STRING,
  account_to STRING,
  amount     STRING,
  k_symbol   STRING
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/tablespace/external/hive/order'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS csv.trans_external;

CREATE EXTERNAL TABLE csv.trans_external (
  trans_id   STRING,
  account_id STRING,
  trans_date STRING,
  type       STRING,
  operation  STRING,
  amount     STRING,
  balance    STRING,
  k_symbol   STRING,
  bank       STRING,
  account    STRING
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION 'abfss://data@maybank1stor40086938.dfs.core.windows.net/tablespace/external/hive/trans'
TBLPROPERTIES ('skip.header.line.count'='1');
