#!/usr/bin/env python3
"""
Quick & dirty ML training script for the Berka loan default use case.

This script:
  - Queries the `gold.fact_loan` Iceberg table via Spark SQL.
  - Builds a small PyTorch feed‑forward network to predict loan status.
  - Runs a few experimental runs with different hyperparameters.
  - Logs parameters, metrics and the best model to MLflow.

Usage (example):
  python3 cml/train_loan_default_pytorch.py \
    --loan-table gold.fact_loan \
    --mlflow-experiment "berka_loan_default" \
    --max-runs 3

Requirements:
  - pandas
  - numpy
  - torch
  - mlflow
  - pyspark
"""

import argparse
import logging
import os
from pathlib import Path
from typing import Dict, Tuple

import mlflow
import mlflow.pytorch
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from impala.dbapi import connect as impala_connect
from torch.utils.data import DataLoader, TensorDataset, random_split

from cml.model_definitions import LoanNet

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a simple PyTorch model to predict loan status on the Berka dataset (from gold.fact_loan via Spark)."
    )
    parser.add_argument(
        "--loan-table",
        default="gold.fact_loan",
        help="Fully-qualified Spark table name containing loan features. Default: gold.fact_loan",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=None,
        help="Optional maximum number of rows to train on. Default: all rows.",
    )
    parser.add_argument(
        "--mlflow-experiment",
        default="berka_loan_default",
        help="MLflow experiment name. Default: berka_loan_default",
    )
    parser.add_argument(
        "--mlflow-tracking-uri",
        default=None,
        help="MLflow tracking URI. Default: MLflow default (local ./mlruns).",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=3,
        help="Number of experimental runs with different hyperparameters. Default: 3",
    )
    return parser.parse_args()
def load_and_prepare_data(args: argparse.Namespace) -> Tuple[TensorDataset, Dict[int, str]]:
    base_query = f"""
    SELECT
      amount,
      duration,
      payments,
      status
    FROM {args.loan_table}
    WHERE amount IS NOT NULL
      AND duration IS NOT NULL
      AND payments IS NOT NULL
      AND status IS NOT NULL
    """
    if args.sample_limit is not None and args.sample_limit > 0:
        base_query += f"\n    LIMIT {args.sample_limit}"

    conn = None
    impala_host = os.getenv(
        "IMPALA_HOST", "coordinator-maybank-impala.dw-maybank1-cdp-env.xfaz-gdb4.cloudera.site"
    )
    impala_port = int(os.getenv("IMPALA_PORT", "443"))
    impala_user = os.getenv("IMPALA_USERNAME", "manishm")
    impala_password = os.getenv("IMPALA_PASSWORD", "Cloudera@123")
    http_path = os.getenv("IMPALA_HTTP_PATH", "cliservice")

    try:
        conn = impala_connect(
            host=impala_host,
            port=impala_port,
            use_ssl=True,
            auth_mechanism="LDAP",
            user=impala_user,
            password=impala_password,
            use_http_transport=True,
            http_path=f"/{http_path}" if not http_path.startswith("/") else http_path,
        )
        df = pd.read_sql(base_query, conn)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as close_err:  # pragma: no cover - defensive guard
                logger.warning("Failed to close Impala connection cleanly: %s", close_err)

    feature_cols = ["amount", "duration", "payments"]

    status_values = sorted(df["status"].unique())
    status_to_idx = {s: i for i, s in enumerate(status_values)}
    idx_to_status = {i: s for s, i in status_to_idx.items()}

    X = df[feature_cols].astype(float).values
    y = df["status"].map(status_to_idx).astype(int).values

    X_tensor = torch.tensor(X, dtype=torch.float32)
    y_tensor = torch.tensor(y, dtype=torch.long)

    dataset = TensorDataset(X_tensor, y_tensor)
    return dataset, idx_to_status


def train_one_run(
    dataset: TensorDataset,
    idx_to_status: Dict[int, str],
    hidden_dim: int,
    learning_rate: float,
    batch_size: int,
    epochs: int,
) -> Tuple[float, nn.Module]:
    num_classes = len(idx_to_status)
    num_features = dataset.tensors[0].shape[1]

    # Simple 80/20 train/val split.
    train_size = int(0.8 * len(dataset))
    val_size = len(dataset) - train_size
    train_ds, val_ds = random_split(dataset, [train_size, val_size])

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=batch_size, shuffle=False)

    model = LoanNet(num_features, hidden_dim, num_classes)
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)

    for epoch in range(epochs):
        model.train()
        for xb, yb in train_loader:
            optimizer.zero_grad()
            logits = model(xb)
            loss = criterion(logits, yb)
            loss.backward()
            optimizer.step()

    # Validation accuracy
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for xb, yb in val_loader:
            logits = model(xb)
            preds = logits.argmax(dim=1)
            correct += (preds == yb).sum().item()
            total += yb.size(0)

    val_accuracy = correct / total if total > 0 else 0.0
    return val_accuracy, model


def main() -> None:
    args = parse_args()

    if args.mlflow_tracking_uri:
        mlflow.set_tracking_uri(args.mlflow_tracking_uri)

    mlflow.set_experiment(args.mlflow_experiment)

    dataset, idx_to_status = load_and_prepare_data(args)

    runs = []
    # Simple hyperparameter grid.
    candidate_hidden_dims = [16, 32, 64]
    candidate_lrs = [1e-3, 5e-4]

    os.makedirs("cml/models", exist_ok=True)

    run_idx = 0
    for hidden_dim in candidate_hidden_dims:
        for lr in candidate_lrs:
            if run_idx >= args.max_runs:
                break

            with mlflow.start_run(run_name=f"run_{run_idx}"):
                mlflow.log_param("hidden_dim", hidden_dim)
                mlflow.log_param("learning_rate", lr)
                mlflow.log_param("batch_size", 64)
                mlflow.log_param("epochs", 10)

                val_acc, model = train_one_run(
                    dataset=dataset,
                    idx_to_status=idx_to_status,
                    hidden_dim=hidden_dim,
                    learning_rate=lr,
                    batch_size=64,
                    epochs=10,
                )

                mlflow.log_metric("val_accuracy", val_acc)

                model_path = Path("cml/models") / f"loan_default_model_run_{run_idx}.pt"
                torch.save(
                    {
                        "model_state_dict": model.state_dict(),
                        "hidden_dim": hidden_dim,
                        "idx_to_status": idx_to_status,
                    },
                    model_path,
                )
                mlflow.log_artifact(str(model_path))
                mlflow.pytorch.log_model(model, artifact_path="pytorch-model")

                runs.append((val_acc, str(model_path)))
                run_idx += 1

        if run_idx >= args.max_runs:
            break

    if not runs:
        print("No runs executed; check configuration.")
        return

    # Pick the best model by validation accuracy.
    best_acc, best_model_path = max(runs, key=lambda x: x[0])
    print(f"Best run: val_accuracy={best_acc:.4f}, model_path={best_model_path}")


if __name__ == "__main__":
    main()
