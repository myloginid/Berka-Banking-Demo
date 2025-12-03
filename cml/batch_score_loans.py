#!/usr/bin/env python3
"""
Batch scoring script for the Berka loan default model.

This script:
  - Loads a trained PyTorch model saved by `train_loan_default_pytorch.py`.
  - Reads loan features directly from the gold Iceberg table `fact_loan` via Spark SQL.
  - Writes run-wise predictions back into a gold table (`loan_default_scores`)
    so they can be compared to actuals over time.

Usage:
  python3 cml/batch_score_loans.py \
    --model-path cml/models/loan_default_model_run_0.pt \
    --loan-table gold.fact_loan \
    --scores-table gold.loan_default_scores \
    --run-id "2025-01-01_batch_01"
"""

import argparse
import os
from datetime import datetime
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import torch
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch score loans using a trained PyTorch model (features from gold.fact_loan via Spark)."
    )
    parser.add_argument(
        "--model-path",
        required=True,
        help="Path to the saved model .pt file.",
    )
    parser.add_argument(
        "--loan-table",
        default="gold.fact_loan",
        help="Fully-qualified Spark table name containing loan features. Default: gold.fact_loan",
    )
    parser.add_argument(
        "--scores-table",
        default="gold.loan_default_scores",
        help="Fully-qualified Spark table name to store batch scores. Default: gold.loan_default_scores",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Identifier for this scoring run (e.g., date or batch ID). Default: auto-generated timestamp.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of loans to score in one batch. Default: 1000",
    )
    return parser.parse_args()


class LoanNet(torch.nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int, output_dim: int) -> None:
        super().__init__()
        self.net = torch.nn.Sequential(
            torch.nn.Linear(input_dim, hidden_dim),
            torch.nn.ReLU(),
            torch.nn.Linear(hidden_dim, hidden_dim),
            torch.nn.ReLU(),
            torch.nn.Linear(hidden_dim, output_dim),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


def load_model(model_path: str) -> torch.nn.Module:
    checkpoint = torch.load(model_path, map_location="cpu")
    idx_to_status: Dict[int, str] = checkpoint["idx_to_status"]
    hidden_dim = checkpoint["hidden_dim"]

    num_features = 3  # amount, duration, payments
    num_classes = len(idx_to_status)

    model = LoanNet(num_features, hidden_dim, num_classes)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()
    return model


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.security.credentials.hiveserver2.enabled", "false")

        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()

    spark = create_spark_session("Berka Loan Batch Scoring")

    loan_table = args.loan_table
    sql = f"""
    SELECT
      loan_id,
      amount,
      duration,
      payments,
      status
    FROM {loan_table}
    WHERE amount IS NOT NULL
      AND duration IS NOT NULL
      AND payments IS NOT NULL
      AND status IS NOT NULL
    """
    if args.limit and args.limit > 0:
        sql += f"\n    LIMIT {args.limit}"

    df = spark.sql(sql).toPandas()
    feature_cols = ["amount", "duration", "payments"]
    X = df[feature_cols].astype(float).values

    X_tensor = torch.tensor(X, dtype=torch.float32)

    checkpoint = torch.load(args.model_path, map_location="cpu")
    idx_to_status = checkpoint["idx_to_status"]
    hidden_dim = checkpoint["hidden_dim"]

    num_features = X_tensor.shape[1]
    num_classes = len(idx_to_status)
    model = LoanNet(num_features, hidden_dim, num_classes)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()

    with torch.no_grad():
        logits = model(X_tensor)
        probs = torch.softmax(logits, dim=1).numpy()
        preds = probs.argmax(axis=1)

    pred_labels = [idx_to_status[int(i)] for i in preds]

    run_id = args.run_id or datetime.utcnow().isoformat(timespec="seconds")
    score_timestamp = datetime.utcnow().isoformat(timespec="seconds")

    def esc(val: str) -> str:
        return val.replace("'", "''")

    scores_table = args.scores_table

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {scores_table} (
      loan_id BIGINT,
      score_timestamp TIMESTAMP,
      run_id STRING,
      actual_status STRING,
      pred_status STRING,
      probabilities_json STRING
    )
    USING iceberg
    TBLPROPERTIES (
      'write.format.default' = 'parquet',
      'write.parquet.compression-codec' = 'snappy'
    )
    """
    spark.sql(create_sql)

    rows: List[Tuple[int, str, str, str, str, str]] = []
    for i, row in df.iterrows():
        loan_id = int(row["loan_id"])
        actual_status = str(row["status"])
        pred_status = pred_labels[i]
        prob_dict = {idx_to_status[j]: float(probs[i, j]) for j in range(num_classes)}
        prob_json = esc(pd.io.json.dumps(prob_dict)) if hasattr(pd.io, "json") else esc(str(prob_dict))
        rows.append(
            (
                loan_id,
                score_timestamp,
                run_id,
                actual_status,
                pred_status,
                prob_json,
            )
        )

    if not rows:
        print("No rows to score; nothing written.")
        return

    scores_df = spark.createDataFrame(
        rows,
        schema=[
            "loan_id",
            "score_timestamp",
            "run_id",
            "actual_status",
            "pred_status",
            "probabilities_json",
        ],
    )

    scores_df = scores_df.selectExpr(
        "CAST(loan_id AS BIGINT) AS loan_id",
        "TO_TIMESTAMP(score_timestamp) AS score_timestamp",
        "CAST(run_id AS STRING) AS run_id",
        "CAST(actual_status AS STRING) AS actual_status",
        "CAST(pred_status AS STRING) AS pred_status",
        "CAST(probabilities_json AS STRING) AS probabilities_json",
    )

    scores_df.write.mode("append").saveAsTable(scores_table)
    print(f"Wrote {len(rows)} prediction rows to {scores_table} (run_id={run_id}).")


if __name__ == "__main__":
    main()
