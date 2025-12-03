#!/usr/bin/env python3
"""
Simple CML application demonstrating how to call the Customer360 HBase table via REST.
"""

from __future__ import annotations

import argparse
import os
from typing import Any, Dict, List, Optional

import requests
from flask import Flask, jsonify, render_template_string, request

from cml.customer360_hbase import (
    HBaseConfig,
    HBaseQueryResult,
    fetch_customer360_record,
)


def _default_port() -> int:
    """Prefer CDSW-provided ports so the app works in the workspace/App launcher."""
    for env_var in ("CDSW_APP_PORT", "PORT"):
        value = os.environ.get(env_var)
        if not value:
            continue
        try:
            return int(value)
        except ValueError:
            continue
    return 8080


DEFAULT_APP_PORT = _default_port()


CUSTOMER360_FIELDS = [
    ("client_id", "Client ID"),
    ("birth_number", "Birth Number"),
    ("client_district_id", "Client District ID"),
    ("account_id", "Account ID"),
    ("account_district_id", "Account District ID"),
    ("loan_id", "Loan ID"),
    ("last_loan_amount", "Last Loan Amount"),
    ("last_loan_status", "Last Loan Status"),
    ("last_loan_ts", "Last Loan Timestamp"),
    ("last_order_amount", "Last Order Amount"),
    ("last_order_k_symbol", "Last Order K Symbol"),
    ("last_order_ts", "Last Order Timestamp"),
    ("last_trans_amount", "Last Transaction Amount"),
    ("last_trans_type", "Last Transaction Type"),
    ("last_trans_operation", "Last Transaction Operation"),
    ("last_trans_ts", "Last Transaction Timestamp"),
]


HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Customer360 Lookup</title>
    <style>
        :root {
            --cloudera-navy: #0c141f;
            --cloudera-orange: #f96702;
            --cloudera-slate: #1f2a37;
            --text-light: #f5f7fa;
        }
        body {
            font-family: "Helvetica Neue", Arial, sans-serif;
            margin: 0;
            background: var(--cloudera-navy);
            color: var(--text-light);
            min-height: 100vh;
        }
        .hero {
            padding: 2.5rem 3rem;
            background: linear-gradient(120deg, var(--cloudera-slate), var(--cloudera-navy));
            border-bottom: 4px solid var(--cloudera-orange);
        }
        h1 { margin: 0 0 0.5rem; font-size: 2.4rem; }
        p.subhead { margin: 0; font-size: 1.1rem; line-height: 1.4; max-width: 720px; }
        .content { padding: 2.5rem 3rem 3.5rem; }
        form {
            margin: 1.5rem 0 2rem;
            display: flex;
            gap: 1rem;
            flex-wrap: wrap;
            align-items: flex-end;
        }
        label { font-weight: bold; display: block; margin-bottom: 0.5rem; }
        input[type="text"] {
            padding: 0.65rem 0.85rem;
            border-radius: 4px;
            border: none;
            min-width: 240px;
        }
        button {
            background: var(--cloudera-orange);
            border: none;
            border-radius: 4px;
            color: var(--text-light);
            padding: 0.7rem 1.5rem;
            font-size: 1rem;
            cursor: pointer;
        }
        button:hover { opacity: 0.9; }
        table {
            border-collapse: collapse;
            width: 100%;
            max-width: 960px;
            margin-top: 1.5rem;
        }
        th, td {
            border: 1px solid rgba(255, 255, 255, 0.15);
            padding: 0.75rem;
            text-align: left;
        }
        th {
            background: rgba(255, 255, 255, 0.06);
            width: 28%;
        }
        .error, .success, .latency {
            margin-top: 1rem;
            padding: 0.75rem 1rem;
            border-radius: 4px;
        }
        .error { background: rgba(255, 77, 79, 0.2); color: #ffdede; }
        .success { background: rgba(34, 197, 94, 0.2); color: #d3f9d8; }
        .latency {
            background: rgba(249, 103, 2, 0.15);
            border-left: 3px solid var(--cloudera-orange);
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 1rem;
            margin-top: 2rem;
        }
        .metric-card {
            background: rgba(255, 255, 255, 0.04);
            border-radius: 6px;
            padding: 1rem;
            border: 1px solid rgba(255, 255, 255, 0.08);
        }
        .metric-card h3 {
            margin: 0;
            font-size: 0.95rem;
            color: rgba(255, 255, 255, 0.75);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        .metric-card p {
            margin: 0.45rem 0 0;
            font-size: 1.5rem;
            color: var(--text-light);
        }
    </style>
</head>
<body>
    <section class="hero">
        <h1>Customer360 Real-Time Lookup</h1>
        <p class="subhead">
            HBase REST delivers millisecond-level access to Customer360 attributes, making it ideal for
            latency-sensitive servicing and decisioning APIs. This sample app issues live requests through
            the Cloudera Data Platform gateway and surfaces the response profile instantly.
        </p>
    </section>
    <section class="content">
        <form method="POST">
            <div>
                <label for="client_id">Client ID</label>
                <input id="client_id" name="client_id" type="text" value="{{ client_id|default('') }}" required>
            </div>
            <button type="submit">Search</button>
        </form>

    {% if error %}
        <div class="error">{{ error }}</div>
    {% elif result %}
        <div class="success">
            Customer360 record retrieved successfully via HBase REST.
        </div>
        {% if latency_ms %}
            <div class="latency">
                HBase REST responded in <strong>{{ "%.1f"|format(latency_ms) }} ms</strong>, showing how easily
                it can serve real-time API traffic.
            </div>
        {% endif %}
        <table>
            <tbody>
            {% for field in result %}
                <tr>
                    <th>{{ field.label }}</th>
                    <td>{{ field.value if field.value is not none else "—" }}</td>
                </tr>
            {% endfor %}
            </tbody>
        </table>
        <div class="metrics-grid">
            <div class="metric-card">
                <h3>Serving Pattern</h3>
                <p>Interactive API</p>
            </div>
            <div class="metric-card">
                <h3>Transport</h3>
                <p>HBase REST (CDP Gateway)</p>
            </div>
            <div class="metric-card">
                <h3>Last Latency</h3>
                <p>{{ latency_ms and ("%.1f ms"|format(latency_ms)) or "—" }}</p>
            </div>
        </div>
    {% endif %}
    </section>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Customer360 HBase lookup application.")
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host interface to bind. Default: 127.0.0.1 (required for CDSW Apps).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_APP_PORT,
        help=f"Port to listen on. Default: {DEFAULT_APP_PORT}",
    )
    parser.add_argument(
        "--hbase-rest-url",
        default=(
            "https://cod-29b4gj7gde0p-gateway0.maybank1.xfaz-gdb4.cloudera.site"
            "/cod-29b4gj7gde0p/cdp-proxy-api/hbase"
        ),
        help="Base URL for the HBase REST endpoint.",
    )
    parser.add_argument(
        "--hbase-namespace",
        default="default",
        help="HBase namespace for the customer360 table. Default: default",
    )
    parser.add_argument(
        "--hbase-table",
        default="customer360",
        help="HBase table name (without namespace). Default: customer360",
    )
    parser.add_argument(
        "--hbase-column-family",
        default="f",
        help="HBase column family for Customer 360 attributes. Default: f",
    )
    parser.add_argument(
        "--hbase-rest-user",
        default="manishm",
        help="Username for HBase REST basic auth.",
    )
    parser.add_argument(
        "--hbase-rest-password",
        default="Cloudera@123",
        help="Password for HBase REST basic auth.",
    )
    args, _ = parser.parse_known_args()
    return args


def format_customer360_fields(row: Dict[str, str]) -> List[Dict[str, Optional[str]]]:
    formatted = []
    for qualifier, label in CUSTOMER360_FIELDS:
        formatted.append({"label": label, "value": row.get(qualifier)})
    return formatted


def create_app(hbase_config: HBaseConfig) -> Flask:
    app = Flask(__name__)

    @app.route("/", methods=["GET", "POST"])
    def index() -> Any:
        error = None
        result = None
        latency_ms: Optional[float] = None
        client_id = (request.values.get("client_id") or "").strip()

        if request.method == "POST":
            if not client_id:
                error = "Please provide a client_id."
            else:
                try:
                    query_result = fetch_customer360_record(client_id, hbase_config)
                    latency_ms = query_result.elapsed_ms
                    if not query_result.record:
                        error = f"No Customer360 record found for client_id {client_id}."
                    else:
                        result = format_customer360_fields(query_result.record)
                except requests.RequestException as exc:
                    error = f"Network error while reaching HBase REST API: {exc}"
                except RuntimeError as exc:
                    error = str(exc)
                except ValueError as exc:
                    error = str(exc)

        return render_template_string(
            HTML_TEMPLATE,
            client_id=client_id,
            error=error,
            result=result,
            latency_ms=latency_ms,
        )

    @app.route("/health", methods=["GET"])
    def health() -> Any:
        return jsonify({"status": "ok"})

    return app


def main() -> None:
    args = parse_args()
    hbase_config = HBaseConfig(
        rest_url=args.hbase_rest_url,
        namespace=args.hbase_namespace,
        table=args.hbase_table,
        column_family=args.hbase_column_family,
        username=args.hbase_rest_user,
        password=args.hbase_rest_password,
    )
    app = create_app(hbase_config)
    app.run(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
