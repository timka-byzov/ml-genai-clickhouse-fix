import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from itertools import islice
from pathlib import Path


ALIASES = {
    "transaction_time": ("transaction_time", "trans_date_trans_time"),
    "state": ("state", "us_state"),
    "category": ("category", "cat_id"),
    "amount": ("amount", "amt"),
}


def resolve_columns(header):
    if not header or len(header) != len(set(header)):
        raise ValueError("CSV должен содержать заголовок с уникальными именами столбцов")
    columns = {}
    for field, aliases in ALIASES.items():
        columns[field] = next((name for name in aliases if name in header), None)
        if columns[field] is None:
            raise ValueError(f"в CSV нужен столбец {' или '.join(aliases)}")
    return columns


def convert_row(row, row_id, columns):
    if None in row or any(value is None for value in row.values()):
        raise ValueError("число значений не совпадает с заголовком CSV")
    state = row[columns["state"]].strip()
    category = row[columns["category"]].strip()
    if not state or not category:
        raise ValueError("state и category не должны быть пустыми")
    amount = Decimal(row[columns["amount"]].strip())
    if not amount.is_finite() or amount.copy_abs() >= Decimal("10000000000000000"):
        raise ValueError("amount не помещается в Decimal(18,2)")
    rounded_amount = amount.quantize(Decimal("0.01"))
    if amount != rounded_amount:
        raise ValueError("amount содержит ненулевые цифры после второго знака")
    transaction_time = datetime.fromisoformat(row[columns["transaction_time"]].strip())
    if transaction_time.tzinfo is None:
        transaction_time = transaction_time.replace(tzinfo=timezone.utc)
    transaction_time = transaction_time.astimezone(timezone.utc)
    return {
        "row_id": row_id,
        "transaction_time": transaction_time.strftime("%Y-%m-%d %H:%M:%S"),
        "state": state,
        "category": category,
        "amount": format(rounded_amount, ".2f"),
    }


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("значение должно быть больше нуля")
    return number


def parse_args():
    parser = argparse.ArgumentParser(description="Загрузка CSV в Kafka")
    parser.add_argument("--csv", required=True, type=Path)
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--limit", type=positive_int)
    parser.add_argument("--encoding", default="utf-8-sig")
    return parser.parse_args()


def run(args):
    from kafka import KafkaProducer

    producer = None
    sent = 0
    pending = []
    try:
        with args.csv.open(encoding=args.encoding, newline="") as source:
            reader = csv.DictReader(source, strict=True)
            columns = resolve_columns(reader.fieldnames)
            for row_id, row in enumerate(islice(reader, args.limit), start=1):
                try:
                    record = convert_row(row, row_id, columns)
                except (ValueError, InvalidOperation) as error:
                    raise ValueError(f"строка данных {row_id}: {error}") from error
                if producer is None:
                    producer = KafkaProducer(
                        bootstrap_servers=args.bootstrap_servers.split(","),
                        acks="all",
                        bootstrap_timeout_ms=10_000,
                        max_block_ms=30_000,
                    )
                message = json.dumps(record, ensure_ascii=False).encode("utf-8")
                pending.append(producer.send(args.topic, key=str(row_id).encode("utf-8"), value=message))
                if len(pending) == 1000:
                    for future in pending:
                        future.get(timeout=30)
                    sent += len(pending)
                    pending.clear()
            for future in pending:
                future.get(timeout=30)
            sent += len(pending)
            if producer is not None:
                producer.flush(timeout=30)
        print(f"Sent {sent} messages")
        return sent
    finally:
        if producer is not None:
            producer.close(timeout=10)


def main():
    args = parse_args()
    try:
        run(args)
        return 0
    except Exception as error:
        print(f"Ошибка: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
