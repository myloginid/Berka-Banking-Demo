#!/usr/bin/env python3
"""
Very simple real-time scoring service for the Berka loan default model.

This "poor man's" service:
  - Loads a trained PyTorch model at startup.
  - Exposes an HTTP endpoint `/score` using Flask.
  - Accepts JSON payloads with `amount`, `duration`, and `payments`.
  - Returns predicted status and probabilities in JSON.

Usage (example):
  python3 cml/realtime_scoring_service.py \
    --model-path cml/models/loan_default_model_run_0.pt \
    --host 0.0.0.0 --port 5000

Then:
  curl -X POST http://localhost:5000/score \
    -H 'Content-Type: application/json' \
    -d '{"amount": 100000, "duration": 24, "payments": 4500}'
"""

import argparse
import json
from typing import Dict, Any

import torch
from flask import Flask, jsonify, request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start a simple Flask-based real-time scoring service."
    )
    parser.add_argument(
        "--model-path",
        required=True,
        help="Path to the saved model .pt file from train_loan_default_pytorch.py.",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host interface to bind. Default: 0.0.0.0",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="Port to listen on. Default: 5000",
    )
    return parser.parse_args()


def load_model(model_path: str):
    checkpoint = torch.load(model_path, map_location="cpu")
    idx_to_status = {int(k): v for k, v in checkpoint["idx_to_status"].items()}
    hidden_dim = checkpoint["hidden_dim"]

    from train_loan_default_pytorch import LoanNet  # type: ignore

    num_features = 3
    num_classes = len(idx_to_status)
    model = LoanNet(num_features, hidden_dim, num_classes)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()

    return model, idx_to_status


def create_app(model_path: str) -> Flask:
    app = Flask(__name__)
    model, idx_to_status = load_model(model_path)

    @app.route("/health", methods=["GET"])
    def health() -> Any:
        return jsonify({"status": "ok"})

    @app.route("/score", methods=["POST"])
    def score() -> Any:
        try:
            payload = request.get_json(force=True)
        except Exception:
            return jsonify({"error": "Invalid JSON"}), 400

        try:
            amount = float(payload["amount"])
            duration = float(payload["duration"])
            payments = float(payload["payments"])
        except (KeyError, ValueError, TypeError):
            return jsonify({"error": "Payload must contain numeric amount, duration, payments"}), 400

        features = torch.tensor([[amount, duration, payments]], dtype=torch.float32)

        with torch.no_grad():
            logits = model(features)
            probs = torch.softmax(logits, dim=1).numpy()[0]
            pred_idx = int(probs.argmax())

        pred_status = idx_to_status[pred_idx]
        prob_dict: Dict[str, float] = {idx_to_status[i]: float(p) for i, p in enumerate(probs)}

        response = {
            "prediction": pred_status,
            "probabilities": prob_dict,
        }
        return jsonify(response)

    return app


def main() -> None:
    args = parse_args()
    app = create_app(args.model_path)
    app.run(host=args.host, port=args.port)


if __name__ == "__main__":
    main()

