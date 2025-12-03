"""
Utility helpers for querying the Customer360 HBase table via the REST (Stargate) API.
"""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import quote

import requests


@dataclass
class HBaseConfig:
    """
    Encapsulates connection details for the Customer360 HBase table.
    """

    rest_url: str
    namespace: str = "default"
    table: str = "customer360"
    column_family: str = "f"
    username: str = ""
    password: str = ""

    @property
    def qualified_table(self) -> str:
        if self.namespace:
            return f"{self.namespace}:{self.table}"
        return self.table

    def build_row_url(self, row_key: str) -> str:
        base = self.rest_url.rstrip("/")
        encoded_row = quote(row_key, safe="")
        return f"{base}/{self.qualified_table}/{encoded_row}"


@dataclass
class HBaseQueryResult:
    record: Optional[Dict[str, str]]
    elapsed_ms: float


def _decode_base64(value: Optional[str]) -> str:
    if value is None:
        return ""
    raw = base64.b64decode(value)
    return raw.decode("utf-8", errors="ignore")


def parse_customer360_response(payload: Dict[str, Any], column_family: str) -> Dict[str, str]:
    """
    Convert an HBase REST JSON payload into a dictionary keyed by qualifier.
    """
    results: Dict[str, str] = {}
    rows = payload.get("Row", [])
    if not rows:
        return results

    for cell in rows[0].get("Cell", []):
        column = _decode_base64(cell.get("column"))
        if ":" in column:
            family, qualifier = column.split(":", 1)
        else:
            family, qualifier = column_family, column
        if family != column_family:
            continue
        value = cell.get("$")
        results[qualifier] = _decode_base64(value) if value is not None else ""
    return results


def fetch_customer360_record(client_id: str, config: HBaseConfig) -> HBaseQueryResult:
    """
    Retrieve a single Customer360 row by its row key (client_id) using the REST API.
    """
    if not client_id:
        raise ValueError("client_id is required.")

    url = config.build_row_url(client_id)
    headers = {"Accept": "application/json"}
    auth = (config.username, config.password) if config.username or config.password else None
    start = time.perf_counter()
    response = requests.get(url, headers=headers, auth=auth, timeout=15)
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    if response.status_code == 404:
        return HBaseQueryResult(record=None, elapsed_ms=elapsed_ms)
    if not response.ok:
        raise RuntimeError(f"HBase REST error {response.status_code}: {response.text}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("HBase REST returned invalid JSON payload.") from exc
    record = parse_customer360_response(payload, config.column_family)
    return HBaseQueryResult(record=record or None, elapsed_ms=elapsed_ms)


__all__ = [
    "HBaseConfig",
    "HBaseQueryResult",
    "fetch_customer360_record",
    "parse_customer360_response",
]
