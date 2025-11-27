In this project we will use CDP Public Cloud Deployed in Azure to ingest the The Berka Dataset available in ./data folder and do everything that is described in ./POC Steps.md to show the end to end working to CDP Public Cloud.

The Berka dataset is a collection of financial information from a Czech bank. The dataset deals with over 5,300 bank clients with approximately 1,000,000 transactions. Additionally, the bank represented in the dataset has extended close to 700 loans and issued nearly 900 credit cards, all of which are represented in the data.

Dataset files in `./data`:
- trans.csv
- order.csv
- loan.csv
- district.csv
- disp.csv
- client.csv
- card.csv
- account.csv

Spark coding convention for this repo:
- For Spark ETL jobs (for example under `scripts/etl`), always express transformations and writes using Spark SQL via `spark.sql(...)` instead of the DataFrame API.
