#!/usr/bin/env python3
"""
Quick & dirty ML training script for the Berka loan default use case.

This script:
  - Queries the `gold.fact_loan` Iceberg table via Trino as a tabular dataset.
  - Builds a small PyTorch feed‑forward network to predict loan status.
  - Runs a few experimental runs with different hyperparameters.
  - Logs parameters, metrics and the best model to MLflow.

Usage (example):
  python3 cml/train_loan_default_pytorch.py \
    --trino-host trino-host --trino-port 8080 --trino-user cml \
    --trino-catalog iceberg --trino-schema gold \
    --mlflow-experiment "berka_loan_default" \
    --max-runs 3

Requirements:
  - pandas
  - numpy
  - torch
  - mlflow
"""

import argparse
import os
from pathlib import Path
from typing import Dict, Tuple

import mlflow
import mlflow.pytorch
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset, random_split

import trino


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a simple PyTorch model to predict loan status on the Berka dataset (from gold.fact_loan via Trino)."
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


class LoanNet(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int, output_dim: int) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, output_dim),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


def load_and_prepare_data(args: argparse.Namespace) -> Tuple[TensorDataset, Dict[int, str]]:
    conn = trino.dbapi.connect(
        host=args.trino_host,
        port=args.trino_port,
        user=args.trino_user,
        catalog=args.trino_catalog,
        schema=args.trino_schema,
    )

    sql = """
    SELECT
      amount,
      duration,
      payments,
      status
    FROM fact_loan
    WHERE amount IS NOT NULL
      AND duration IS NOT NULL
      AND payments IS NOT NULL
      AND status IS NOT NULL
    """
    df = pd.read_sql(sql, conn)

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
