import argparse
import csv
import io
import hashlib
import json
import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

from csv_to_kafka import convert_row, resolve_columns
from utils import DATABASE, RESULTS, ROOT, TOPIC, query, read_sql


def csv_result(path, limit):
    result = {}
    rows = 0
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        columns = resolve_columns(reader.fieldnames)
        for row_id, row in enumerate(reader, 1):
            if limit is not None and row_id > limit:
                break
            convert_row(row, row_id, columns)
            state = row[columns["state"]].strip()
            category = row[columns["category"]].strip()
            amount = Decimal(row[columns["amount"]])
            candidate = (amount, row_id, category)
            if state not in result or candidate[:2] > result[state][:2]:
                result[state] = candidate
            rows += 1
    return rows, result


def table_counts():
    baseline = query(f"SELECT count(), uniqExact(row_id) FROM {DATABASE}.transactions").strip().split("\t")
    optimized = query(f"SELECT count(), uniqExact(row_id) FROM {DATABASE}.transactions_optimized").strip().split("\t")
    return [int(value) for value in baseline + optimized]


def check_empty_topic():
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(","),
        allow_auto_create_topics=False,
        enable_auto_commit=False,
    )
    try:
        partitions = consumer.partitions_for_topic(TOPIC)
        if partitions is None:
            raise RuntimeError(f"Kafka topic does not exist: {TOPIC}")
        offsets = consumer.end_offsets([TopicPartition(TOPIC, partition) for partition in partitions])
        if any(offsets.values()):
            raise RuntimeError("Kafka topic already has messages; check the previous load before sending again")
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, type=Path)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")
    expected_rows, expected = csv_result(args.csv, args.limit)
    if expected_rows == 0:
        raise ValueError("CSV has no data rows")
    with args.csv.open("rb") as source:
        csv_hash = hashlib.file_digest(source, "sha256").hexdigest()
    load_info = {"sha256": csv_hash, "rows": expected_rows, "limit": args.limit}
    RESULTS.mkdir(parents=True, exist_ok=True)

    for filename in ["01_schema.sql", "02_schema_optimized.sql"]:
        for statement in read_sql(filename).split(";"):
            if statement.strip():
                query(statement)

    metadata = RESULTS / "load.json"
    loaded = json.loads(metadata.read_text()) if metadata.exists() else None
    if loaded and any(loaded.get(key) != value for key, value in load_info.items()):
        raise RuntimeError("Tables already contain another CSV or another --limit")
    current_counts = table_counts()
    if current_counts == [0, 0, 0, 0]:
        if loaded:
            raise RuntimeError("Previous load was interrupted; check Kafka and ClickHouse before reloading")
        check_empty_topic()
        metadata.write_text(json.dumps({**load_info, "status": "loading"}, indent=2) + "\n")
        command = [sys.executable, str(ROOT / "scripts" / "csv_to_kafka.py"), "--csv", str(args.csv)]
        if args.limit is not None:
            command += ["--limit", str(args.limit)]
        subprocess.run(command, check=True)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            current_counts = table_counts()
            if current_counts == [expected_rows] * 4:
                break
            if max(current_counts) > expected_rows:
                raise RuntimeError(f"Extra rows in ClickHouse: {current_counts}")
            time.sleep(1)
        else:
            raise RuntimeError(f"Expected {expected_rows} rows, got {current_counts}")
    elif current_counts != [expected_rows] * 4:
        raise RuntimeError(f"ClickHouse already has data: {current_counts}; expected {expected_rows}. Check it before reloading.")
    elif not loaded:
        raise RuntimeError("Tables are not empty but load.json is missing; check the previous load before continuing")
    else:
        print("Using previously loaded data")

    output = query(read_sql("03_max_transaction_by_state.sql"), format="CSVWithNames")
    actual = {
        row["state"]: (Decimal(row["max_amount"]), int(row["row_id"]), row["category"])
        for row in csv.DictReader(io.StringIO(output))
    }
    if actual != expected:
        raise AssertionError("SQL result differs from the CSV result")
    (RESULTS / "max_transaction_by_state.csv").write_text(output)
    metadata.write_text(json.dumps({**load_info, "status": "complete"}, indent=2) + "\n")
    print(f"Checked {expected_rows} transactions and {len(actual)} states")
    print("Saved results/max_transaction_by_state.csv")
    subprocess.run([sys.executable, str(ROOT / "scripts" / "benchmark.py")], check=True)


if __name__ == "__main__":
    main()
