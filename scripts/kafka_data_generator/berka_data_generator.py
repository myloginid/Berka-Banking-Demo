#!/usr/bin/env python3
"""
Berka dataset streaming data generator.

Generates synthetic loan, order, and transaction events every N seconds
and publishes them to configurable Kafka topics.

Usage (example):
  python scripts/berka_data_generator.py \
    --bootstrap-servers localhost:9092 \
    --loan-topic berka_loans \
    --order-topic berka_orders \
    --trans-topic berka_trans \
    --interval-seconds 10 \
    --batch-size 10
"""

import argparse
import csv
import json
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
import ssl


DEFAULT_BOOTSTRAP_SERVERS = ",".join(
    [
        "kafka-demo-corebroker0.maybank1.xfaz-gdb4.cloudera.site:9093",
        "kafka-demo-corebroker1.maybank1.xfaz-gdb4.cloudera.site:9093",
        "kafka-demo-corebroker2.maybank1.xfaz-gdb4.cloudera.site:9093",
    ]
)
# Default to the directory that contains this script so the nearby Berka CSVs are found without flags.
DEFAULT_DATA_DIR = str(Path(__file__).resolve().parent)


def non_negative_int(value: str) -> int:
    ivalue = int(value)
    if ivalue < 0:
        raise argparse.ArgumentTypeError("Value must be >= 0")
    return ivalue


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic Berka loan/order/transaction events and send to Kafka."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help=(
            "Kafka bootstrap servers (host:port, comma separated). "
            f"Default: {DEFAULT_BOOTSTRAP_SERVERS}"
        ),
    )
    parser.add_argument(
        "--loan-topic",
        default="berka_loans",
        help="Kafka topic name for loan events. Default: berka_loans",
    )
    parser.add_argument(
        "--order-topic",
        default="berka_orders",
        help="Kafka topic name for order events. Default: berka_orders",
    )
    parser.add_argument(
        "--trans-topic",
        default="berka_trans",
        help="Kafka topic name for transaction events. Default: berka_trans",
    )
    parser.add_argument(
        "--security-protocol",
        default="SASL_SSL",
        choices=["PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL", "SSL"],
        help=(
            "Kafka security protocol. Use SASL_* when authenticating via Kerberos. "
            "Default: SASL_SSL"
        ),
    )
    parser.add_argument(
        "--sasl-mechanism",
        default="GSSAPI",
        help="Kafka SASL mechanism. Kerberos uses GSSAPI. Default: GSSAPI",
    )
    parser.add_argument(
        "--kerberos-service-name",
        default="kafka",
        help="Kerberos service principal name (the 'primary' part). Default: kafka",
    )
    parser.add_argument(
        "--kerberos-domain-name",
        default=None,
        help="Optional Kerberos realm/domain portion if brokers require it (e.g., EXAMPLE.COM).",
    )
    parser.add_argument(
        "--ssl-cafile",
        default=None,
        help="Path to CA certificate file for verifying broker certs when using SSL.",
    )
    parser.add_argument(
        "--ssl-certfile",
        default=None,
        help="Optional client certificate file for mutual TLS.",
    )
    parser.add_argument(
        "--ssl-keyfile",
        default=None,
        help="Private key file for the provided client certificate.",
    )
    parser.add_argument(
        "--ssl-disable-hostname-check",
        action="store_true",
        help="Disable SSL hostname verification (not recommended).",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=30,
        help="Interval in seconds between batches of generated events. Default: 30",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of events of each type to generate per interval. Default: 10",
    )
    parser.add_argument(
        "--max-iterations",
        type=non_negative_int,
        default=0,
        help="Maximum number of batches to send before exiting (0 = run forever).",
    )
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATA_DIR,
        help=(
            "Directory containing Berka CSVs (account.csv, client.csv, disp.csv). "
            f"Default: {DEFAULT_DATA_DIR}"
        ),
    )
    return parser.parse_args()


def try_import_kafka_producer():
    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError as exc:  # pragma: no cover - runtime guard
        print(
            "ERROR: kafka-python is not installed.\n"
            "Install it with:\n"
            "  pip install kafka-python\n",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc
    return KafkaProducer


def random_date_within_days(days_back: int = 30) -> str:
    now = datetime.utcnow()
    delta_days = random.randint(0, days_back)
    dt = now - timedelta(days=delta_days)
    return dt.strftime("%Y-%m-%d")


def random_loan_event(
    loan_id: int,
    account_id: int,
    district_id: int,
    client_id: Optional[int],
    disp_id: Optional[int],
) -> Dict[str, Any]:
    amount = random.randint(10_000, 500_000)
    duration_months = random.choice([12, 24, 36, 48, 60, 72])
    payment = round(amount / duration_months * random.uniform(0.95, 1.05), 2)
    status = random.choice(["A", "B", "C", "D"])
    event = {
        "loan_id": loan_id,
        "account_id": account_id,
        "district_id": district_id,
        "client_id": client_id,
        "disp_id": disp_id,
        "date": random_date_within_days(365),
        "amount": amount,
        "duration": duration_months,
        "payments": payment,
        "status": status,
        "ingest_ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    return event


def random_order_event(
    order_id: int,
    account_id: int,
    district_id: int,
    client_id: Optional[int],
    disp_id: Optional[int],
) -> Dict[str, Any]:
    amount = round(random.uniform(100.0, 20_000.0), 2)
    banks = ["ST", "CS", "KB", "RB", "YZ"]
    k_symbols = ["SIPO", "UVER", "LEASING", "POJISTNE", ""]
    event = {
        "order_id": order_id,
        "account_id": account_id,
        "district_id": district_id,
        "client_id": client_id,
        "disp_id": disp_id,
        "bank_to": random.choice(banks),
        "account_to": str(random.randint(10_000_000, 99_999_999)),
        "amount": amount,
        "k_symbol": random.choice(k_symbols),
        "ingest_ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    return event


def random_trans_event(
    trans_id: int,
    starting_balance: float,
    account_id: int,
    district_id: int,
    client_id: Optional[int],
    disp_id: Optional[int],
) -> Dict[str, Any]:
    tx_type = random.choice(["PRIJEM", "VYDAJ"])
    operation_income = ["VKLAD", "PREVOD Z UCTU", "VYPLATA UROKU"]
    operation_outcome = ["VYBER", "PREVOD NA UCET", "SLUZBY"]
    if tx_type == "PRIJEM":
        operation = random.choice(operation_income)
        amount = round(random.uniform(50.0, 20_000.0), 2)
        balance = starting_balance + amount
    else:
        operation = random.choice(operation_outcome)
        amount = round(random.uniform(50.0, min(starting_balance + 5000.0, 20_000.0)), 2)
        balance = starting_balance - amount

    k_symbols = ["SIPO", "UVER", "LEASING", "POJISTNE", ""]
    banks = ["ST", "CS", "KB", "RB", "YZ", ""]

    event = {
        "trans_id": trans_id,
        "account_id": account_id,
        "district_id": district_id,
        "client_id": client_id,
        "disp_id": disp_id,
        "date": random_date_within_days(30),
        "type": tx_type,
        "operation": operation,
        "amount": amount,
        "balance": round(balance, 2),
        "k_symbol": random.choice(k_symbols),
        "bank": random.choice(banks),
        "account": str(random.randint(10_000_000, 99_999_999)),
        "ingest_ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    return event


def load_accounts(path: Path) -> Dict[int, Dict[str, Any]]:
    accounts: Dict[int, Dict[str, Any]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            try:
                account_id = int(row["account_id"])
                district_id = int(row["district_id"])
            except (KeyError, ValueError):
                continue
            accounts[account_id] = {
                "account_id": account_id,
                "district_id": district_id,
                "frequency": row.get("frequency"),
                "date": row.get("date"),
            }
    return accounts


def load_clients(path: Path) -> Dict[int, Dict[str, Any]]:
    clients: Dict[int, Dict[str, Any]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            try:
                client_id = int(row["client_id"])
                district_id = int(row["district_id"])
            except (KeyError, ValueError):
                continue
            clients[client_id] = {
                "client_id": client_id,
                "district_id": district_id,
                "birth_number": row.get("birth_number"),
            }
    return clients


def load_dispositions(path: Path) -> Dict[int, List[Dict[str, Any]]]:
    by_account: Dict[int, List[Dict[str, Any]]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            try:
                disp_id = int(row["disp_id"])
                client_id = int(row["client_id"])
                account_id = int(row["account_id"])
            except (KeyError, ValueError):
                continue
            entry = {
                "disp_id": disp_id,
                "client_id": client_id,
                "account_id": account_id,
                "type": row.get("type"),
            }
            by_account.setdefault(account_id, []).append(entry)
    return by_account


def load_reference_data(data_dir: str) -> Dict[str, Any]:
    base = Path(data_dir)
    account_path = base / "account.csv"
    client_path = base / "client.csv"
    disp_path = base / "disp.csv"

    for path in (account_path, client_path, disp_path):
        if not path.exists():
            print(f"ERROR: Required Berka data file not found: {path}", file=sys.stderr)
            raise SystemExit(1)

    accounts = load_accounts(account_path)
    clients = load_clients(client_path)
    dispositions_by_account = load_dispositions(disp_path)

    if not accounts:
        print("ERROR: No accounts loaded from account.csv; cannot maintain FK relationships.", file=sys.stderr)
        raise SystemExit(1)

    accounts_with_disp = list(dispositions_by_account.keys())
    if accounts_with_disp:
        account_ids_for_events = accounts_with_disp
    else:
        account_ids_for_events = list(accounts.keys())

    return {
        "accounts": accounts,
        "clients": clients,
        "dispositions_by_account": dispositions_by_account,
        "account_ids_for_events": account_ids_for_events,
    }


def choose_account_context(ref_data: Dict[str, Any]) -> Dict[str, Any]:
    account_ids: List[int] = ref_data["account_ids_for_events"]
    accounts: Dict[int, Dict[str, Any]] = ref_data["accounts"]
    dispositions_by_account: Dict[int, List[Dict[str, Any]]] = ref_data["dispositions_by_account"]
    clients: Dict[int, Dict[str, Any]] = ref_data["clients"]

    account_id = random.choice(account_ids)
    account = accounts[account_id]
    district_id = int(account["district_id"])

    disps = dispositions_by_account.get(account_id) or []
    disp = random.choice(disps) if disps else None
    client = clients.get(disp["client_id"]) if disp else None

    client_id: Optional[int] = client["client_id"] if client else None
    disp_id: Optional[int] = disp["disp_id"] if disp else None

    return {
        "account_id": account_id,
        "district_id": district_id,
        "client_id": client_id,
        "disp_id": disp_id,
    }


def generate_batch(
    next_loan_id: int,
    next_order_id: int,
    next_trans_id: int,
    batch_size: int,
    ref_data: Dict[str, Any],
    balances: Dict[int, float],
) -> Dict[str, Any]:
    loan_events: List[Dict[str, Any]] = []
    order_events: List[Dict[str, Any]] = []
    trans_events: List[Dict[str, Any]] = []

    for _ in range(batch_size):
        context = choose_account_context(ref_data)
        account_id = context["account_id"]
        district_id = context["district_id"]
        client_id = context["client_id"]
        disp_id = context["disp_id"]

        loan_events.append(
            random_loan_event(
                loan_id=next_loan_id,
                account_id=account_id,
                district_id=district_id,
                client_id=client_id,
                disp_id=disp_id,
            )
        )
        next_loan_id += 1

        order_events.append(
            random_order_event(
                order_id=next_order_id,
                account_id=account_id,
                district_id=district_id,
                client_id=client_id,
                disp_id=disp_id,
            )
        )
        next_order_id += 1

        starting_balance = balances.get(account_id)
        if starting_balance is None:
            starting_balance = float(random.randint(1_000, 50_000))

        trans_event = random_trans_event(
            trans_id=next_trans_id,
            starting_balance=starting_balance,
            account_id=account_id,
            district_id=district_id,
            client_id=client_id,
            disp_id=disp_id,
        )
        balances[account_id] = float(trans_event["balance"])
        trans_events.append(trans_event)
        next_trans_id += 1

    return {
        "loan_events": loan_events,
        "order_events": order_events,
        "trans_events": trans_events,
        "next_loan_id": next_loan_id,
        "next_order_id": next_order_id,
        "next_trans_id": next_trans_id,
        "balances": balances,
    }


def main() -> None:
    args = parse_args()
    ref_data = load_reference_data(args.data_dir)
    KafkaProducer = try_import_kafka_producer()

    producer_config: Dict[str, Any] = {
        "bootstrap_servers": args.bootstrap_servers.split(","),
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "key_serializer": lambda v: v.encode("utf-8") if v is not None else None,
        "linger_ms": 10,
    }

    if args.security_protocol:
        producer_config["security_protocol"] = args.security_protocol

    if args.security_protocol and args.security_protocol.upper().startswith("SASL"):
        producer_config["sasl_mechanism"] = args.sasl_mechanism
        producer_config["sasl_kerberos_service_name"] = args.kerberos_service_name
        if args.kerberos_domain_name:
            producer_config["sasl_kerberos_domain_name"] = args.kerberos_domain_name

    if args.security_protocol and "SSL" in args.security_protocol.upper():
        ssl_context = ssl.create_default_context(cafile=args.ssl_cafile)
        if args.ssl_certfile:
            ssl_context.load_cert_chain(certfile=args.ssl_certfile, keyfile=args.ssl_keyfile)
        if args.ssl_disable_hostname_check:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        producer_config["ssl_context"] = ssl_context

    producer = KafkaProducer(**producer_config)

    print(
        f"Starting Berka data generator:\n"
        f"  bootstrap_servers = {args.bootstrap_servers}\n"
        f"  loan_topic        = {args.loan_topic}\n"
        f"  order_topic       = {args.order_topic}\n"
        f"  trans_topic       = {args.trans_topic}\n"
        f"  interval_seconds  = {args.interval_seconds}\n"
        f"  batch_size        = {args.batch_size}\n"
        f"  data_dir          = {args.data_dir}\n",
        flush=True,
    )

    next_loan_id = 1
    next_order_id = 1
    next_trans_id = 1
    balances: Dict[int, float] = {}
    iterations = 0

    try:
        while True:
            batch = generate_batch(
                next_loan_id=next_loan_id,
                next_order_id=next_order_id,
                next_trans_id=next_trans_id,
                batch_size=args.batch_size,
                ref_data=ref_data,
                balances=balances,
            )

            loan_events = batch["loan_events"]
            order_events = batch["order_events"]
            trans_events = batch["trans_events"]
            next_loan_id = batch["next_loan_id"]
            next_order_id = batch["next_order_id"]
            next_trans_id = batch["next_trans_id"]
            balances = batch["balances"]

            for event in loan_events:
                key = f"loan-{event['loan_id']}"
                producer.send(args.loan_topic, key=key, value=event)

            for event in order_events:
                key = f"order-{event['order_id']}"
                producer.send(args.order_topic, key=key, value=event)

            for event in trans_events:
                key = f"trans-{event['trans_id']}"
                producer.send(args.trans_topic, key=key, value=event)

            producer.flush()

            print(
                f"[{datetime.utcnow().isoformat(timespec='seconds')}Z] "
                f"sent {len(loan_events)} loans, "
                f"{len(order_events)} orders, "
                f"{len(trans_events)} transactions.",
                flush=True,
            )

            iterations += 1
            if args.max_iterations and iterations >= args.max_iterations:
                break

            time.sleep(args.interval_seconds)

    except KeyboardInterrupt:
        print("\nStopping Berka data generator...", flush=True)
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
