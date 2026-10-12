import argparse
import csv
import json
import statistics
import uuid
from utils import RESULTS, query, read_sql


QUERIES = {
    "baseline": "05_max_transaction_by_state_baseline.sql",
    "optimized_raw": "06_max_transaction_by_state_optimized_raw.sql",
    "optimized": "03_max_transaction_by_state.sql",
}
FIELDS = ["variant", "run", "query_duration_ms", "read_rows", "read_bytes", "memory_usage"]


def positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("число повторов должно быть больше нуля")
    return number


def json_rows(sql: str) -> list[dict]:
    return [json.loads(line) for line in query(sql, format="JSONEachRow").splitlines() if line]


def print_storage():
    rows = json_rows("""
        SELECT table, sum(rows) AS rows, sum(data_compressed_bytes) AS compressed_bytes,
               sum(bytes_on_disk) AS disk_bytes
        FROM system.parts
        WHERE active AND database = currentDatabase()
          AND table IN ('transactions', 'transactions_optimized', 'max_by_state')
        GROUP BY table
        ORDER BY table
    """)
    print("Хранение (активные части):")
    for row in rows:
        print(f"  {row['table']}: rows={row['rows']}, "
              f"compressed_bytes={row['compressed_bytes']}, disk_bytes={row['disk_bytes']}")
    optimized = [row for row in rows if row["table"] != "transactions"]
    print("  transactions_optimized + max_by_state: "
          f"disk_bytes={sum(int(row['disk_bytes']) for row in optimized)}")


def benchmark(repeat: int):
    sql_queries = {variant: read_sql(filename) for variant, filename in QUERIES.items()}
    warmup = {variant: query(sql, format="CSVWithNames") for variant, sql in sql_queries.items()}
    if len(set(warmup.values())) != 1:
        raise RuntimeError("Результаты трёх запросов отличаются")
    print("Результаты трёх запросов совпадают; прогрев завершён.")

    runs = []
    for run in range(1, repeat + 1):
        for variant, sql in sql_queries.items():
            query_id = str(uuid.uuid4())
            query(sql, format="CSVWithNames", query_id=query_id)
            runs.append({"variant": variant, "run": run, "query_id": query_id})

    query("SYSTEM FLUSH LOGS")
    ids = ", ".join(f"'{run['query_id']}'" for run in runs)
    logged = json_rows(f"""
        SELECT query_id, query_duration_ms, read_rows, read_bytes, memory_usage
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query_id IN ({ids})
    """)
    metrics = {row["query_id"]: row for row in logged}
    results = []
    for run in runs:
        if run["query_id"] not in metrics:
            raise RuntimeError(f"В system.query_log нет завершённого запроса {run['query_id']}")
        row = {"variant": run["variant"], "run": run["run"]}
        row.update({name: int(metrics[run["query_id"]][name]) for name in FIELDS[2:]})
        results.append(row)

    output = RESULTS / "benchmark.csv"
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(results)
    print(f"Метрики каждого запуска: {output}")
    for variant in QUERIES:
        durations = [row["query_duration_ms"] for row in results if row["variant"] == variant]
        print(f"  {variant}: median={statistics.median(durations):g} ms ({repeat} запусков)")
    print_storage()


def main():
    parser = argparse.ArgumentParser(description="Сравнить запросы на одинаковых данных")
    parser.add_argument("--repeat", type=positive_int, default=5)
    args = parser.parse_args()
    benchmark(args.repeat)


if __name__ == "__main__":
    main()
