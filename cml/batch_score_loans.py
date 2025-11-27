#!/usr/bin/env python3
"""
Batch scoring script for the Berka loan default model.

This script:
  - Loads a trained PyTorch model saved by `train_loan_default_pytorch.py`.
  - Reads loan features directly from the gold Iceberg table `fact_loan` via Trino.
  - Writes run-wise predictions back into a gold table (`loan_default_scores`)
    so they can be compared to actuals over time.

Usage:
  python3 cml/batch_score_loans.py \
    --model-path cml/models/loan_default_model_run_0.pt \
    --trino-host trino-host --trino-port 8080 --trino-user cml \
    --trino-catalog iceberg --trino-schema gold \
    --run-id "2025-01-01_batch_01"
"""

import argparse
import os
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
import torch
import trino
from datetime import datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch score loans using a trained PyTorch model (features from gold.fact_loan via Trino)."
    )
    parser.add_argument(
        "--model-path",
        required=True,
        help="Path to the saved model .pt file.",
    )
    parser.add_argument(
        "--trino-host",
        required=True,
        help="Trino coordinator host.",
    )
    parser.add_argument(
        "--trino-port",
        type=int,
        default=8080,
        help="Trino coordinator port. Default: 8080",
    )
    parser.add_argument(
        "--trino-user",
        default="cml",
        help="Trino user. Default: cml",
    )
    parser.add_argument(
        "--trino-catalog",
        default="iceberg",
        help="Trino catalog for Iceberg tables. Default: iceberg",
    )
    parser.add_argument(
        "--trino-schema",
        default="gold",
        help="Trino schema for gold tables. Default: gold",
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


def main() -> None:
    args = parse_args()

    conn = trino.dbapi.connect(
        host=args.trino_host,
        port=args.trino_port,
        user=args.trino_user,
        catalog=args.trino_catalog,
        schema=args.trino_schema,
    )

    sql = f"""
    SELECT
      loan_id,
      amount,
      duration,
      payments,
      status
    FROM fact_loan
    WHERE amount IS NOT NULL
      AND duration IS NOT NULL
      AND payments IS NOT NULL
      AND status IS NOT NULL
    LIMIT {args.limit}
    """
    df = pd.read_sql(sql, conn)
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

    idx_to_status = {int(k): v for k, v in idx_to_status.items()}
    pred_labels = [idx_to_status[int(i)] for i in preds]

    # Prepare run metadata.
    run_id = args.run_id or datetime.utcnow().isoformat(timespec="seconds")
    score_timestamp = datetime.utcnow().isoformat(timespec="seconds")

    # Build INSERT statements to write into gold.loan_default_scores.
    # Table schema:
    #   loan_id BIGINT,
    #   score_timestamp TIMESTAMP,
    #   run_id VARCHAR,
    #   actual_status VARCHAR,
    #   pred_status VARCHAR,
    #   probabilities_json VARCHAR

    def esc(val: str) -> str:
        return val.replace("'", "''")

    cur = conn.cursor()
    create_sql = """
    CREATE TABLE IF NOT EXISTS loan_default_scores (
      loan_id BIGINT,
      score_timestamp TIMESTAMP,
      run_id VARCHAR,
      actual_status VARCHAR,
      pred_status VARCHAR,
      probabilities_json VARCHAR
    )
    """
    cur.execute(create_sql)

    values = []
    for i, row in df.iterrows():
        loan_id = int(row["loan_id"])
        actual_status = str(row["status"])
        pred_status = pred_labels[i]
        prob_dict = {idx_to_status[j]: float(probs[i, j]) for j in range(num_classes)}
        prob_json = esc(pd.io.json.dumps(prob_dict)) if hasattr(pd.io, "json") else esc(str(prob_dict))
        values.append(
            f"({loan_id}, TIMESTAMP '{score_timestamp}', '{esc(run_id)}', "
            f"'{esc(actual_status)}', '{esc(pred_status)}', '{prob_json}')"
        )

    if values:
        insert_sql = (
            "INSERT INTO loan_default_scores "
            "(loan_id, score_timestamp, run_id, actual_status, pred_status, probabilities_json) VALUES "
            + ",\n".join(values)
        )
        cur.execute(insert_sql)
        print(f"Wrote {len(values)} prediction rows to gold.loan_default_scores (run_id={run_id}).")
    else:
        print("No rows to score; nothing written.")


if __name__ == "__main__":
    main()
