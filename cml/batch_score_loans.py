#!/usr/bin/env python3
"""
Batch scoring script for the Berka loan default model.

This script:
  - Loads a trained PyTorch model saved by `train_loan_default_pytorch.py`.
  - Reads loan features directly from the gold Iceberg table `fact_loan` via Trino.
  - Writes a simple CSV with predicted class and probabilities.

Usage:
  python3 cml/batch_score_loans.py \
    --model-path cml/models/loan_default_model_run_0.pt \
    --trino-host trino-host --trino-port 8080 --trino-user cml \
    --trino-catalog iceberg --trino-schema gold \
    --output-path cml/output/loan_scores.csv
"""

import argparse
import os
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
import torch
import trino


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
        "--output-path",
        default="cml/output/loan_scores.csv",
        help="Path to write scored CSV. Default: cml/output/loan_scores.csv",
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

    os.makedirs(Path(args.output_path).parent, exist_ok=True)
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
      payments
    FROM fact_loan
    WHERE amount IS NOT NULL
      AND duration IS NOT NULL
      AND payments IS NOT NULL
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

    out_df = df.copy()
    out_df["pred_status"] = pred_labels

    # Add probability columns.
    for class_idx, class_label in idx_to_status.items():
        out_df[f"prob_{class_label}"] = probs[:, class_idx]

    out_df.to_csv(args.output_path, index=False)
    print(f"Wrote batch scores to {args.output_path}")


if __name__ == "__main__":
    main()
