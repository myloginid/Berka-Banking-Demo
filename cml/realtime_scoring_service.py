#!/usr/bin/env python3
"""
Very simple real-time scoring service for the Berka loan default model, compatible
with Cloudera CML model deployment.

This "poor man's" service:
  - Provides a `predict(args)` function that CML can call directly.
  - Loads a trained PyTorch model (from `MODEL_PATH` env var or an explicit argument).
  - Accepts JSON payloads with `amount`, `duration`, and `payments`.
  - Returns predicted status and probabilities in JSON-serializable format.

You can also run it as a standalone Flask service for local testing:

  python3 cml/realtime_scoring_service.py \\
    --model-path cml/models/loan_default_model_run_0.pt \\
    --host 0.0.0.0 --port 5000

Then:
  curl -X POST http://localhost:5000/score \\
    -H 'Content-Type: application/json' \\
    -d '{"amount": 100000, "duration": 24, "payments": 4500}'
"""

import argparse
import os
from typing import Any, Dict, List, Union

import torch
from flask import Flask, jsonify, request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start a simple Flask-based real-time scoring service."
    )
    parser.add_argument(
        "--model-path",
        required=False,
        default=None,
        help="Path to the saved model .pt file from train_loan_default_pytorch.py. "
        "If omitted, MODEL_PATH environment variable is used.",
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


# Global model state for CML-style scoring.
MODEL = None
IDX_TO_STATUS: Dict[int, str] = {}


def init_model(model_path: str | None = None) -> None:
    """
    Initialize the global model/label mapping.

    In CML, MODEL_PATH is typically provided as an environment variable.
    """
    global MODEL, IDX_TO_STATUS
    if MODEL is not None:
        return

    path = model_path or os.environ.get("MODEL_PATH")
    if not path:
        raise RuntimeError("Model path must be provided via argument or MODEL_PATH env var.")

    MODEL, IDX_TO_STATUS = load_model(path)


def _score_one(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Score a single record using the global model.
    """
    if MODEL is None:
        init_model()

    try:
        amount = float(payload["amount"])
        duration = float(payload["duration"])
        payments = float(payload["payments"])
    except (KeyError, ValueError, TypeError):
        raise ValueError("Payload must contain numeric amount, duration, payments")

    features = torch.tensor([[amount, duration, payments]], dtype=torch.float32)

    with torch.no_grad():
        logits = MODEL(features)
        probs = torch.softmax(logits, dim=1).numpy()[0]
        pred_idx = int(probs.argmax())

    pred_status = IDX_TO_STATUS[pred_idx]
    prob_dict: Dict[str, float] = {IDX_TO_STATUS[i]: float(p) for i, p in enumerate(probs)}

    return {
        "prediction": pred_status,
        "probabilities": prob_dict,
    }


def predict(args: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """
    CML entrypoint-style predict function.

    Accepts either a single JSON-like dict or a list of dicts with
    keys: amount, duration, payments.
    """
    if isinstance(args, list):
        return [_score_one(item) for item in args]
    return _score_one(args)


def create_app(model_path: str | None) -> Flask:
    app = Flask(__name__)
    # Initialize model for Flask usage (falls back to env if needed).
    init_model(model_path)

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
            result = predict(payload)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        return jsonify(result)

    return app


def main() -> None:
    args = parse_args()
    # Prefer CLI model path, fallback to env var.
    model_path = args.model_path or os.environ.get("MODEL_PATH")
    if not model_path:
        raise RuntimeError("Model path must be provided via --model-path or MODEL_PATH env var.")
    app = create_app(model_path)
    app.run(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
