import os
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DATABASE = os.getenv("CLICKHOUSE_DATABASE", "homework")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
ROOT = Path(__file__).resolve().parent.parent
RESULTS = Path(os.getenv("RESULTS_DIR", str(ROOT / "results")))


def query(sql, format="TabSeparated", query_id=None):
    params = {
        "default_format": format,
        "max_threads": 2,
        "use_query_cache": 0,
        "log_queries": 1,
    }
    if not sql.lstrip().upper().startswith("CREATE DATABASE"):
        params["database"] = DATABASE
    if query_id:
        params["query_id"] = query_id
    url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123") + "/?" + urlencode(params)
    request = Request(
        url,
        data=sql.encode(),
        headers={
            "X-ClickHouse-User": os.getenv("CLICKHOUSE_USER", "student"),
            "X-ClickHouse-Key": os.getenv("CLICKHOUSE_PASSWORD", "student"),
        },
    )
    with urlopen(request, timeout=120) as response:
        return response.read().decode()


def read_sql(name):
    sql = (ROOT / "sql" / name).read_text()
    sql = sql.replace("homework.", f"{DATABASE}.")
    sql = sql.replace("'homework'", f"'{DATABASE}'")
    sql = sql.replace("EXISTS homework;", f"EXISTS {DATABASE};")
    sql = sql.replace("kafka_topic_list = 'transactions'", f"kafka_topic_list = '{TOPIC}'")
    return sql.replace("kafka_group_name = 'homework_ingest'", f"kafka_group_name = '{DATABASE}_ingest'")
