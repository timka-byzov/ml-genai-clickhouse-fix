#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

usage() {
  cat <<'EOF'
Usage: ./scripts/project.sh COMMAND [ARGS]
  up                         Build Python image and start Kafka + ClickHouse
  run --csv data/file.csv     Load CSV, check SQL and benchmark
  benchmark                  Compare queries on loaded data
  producer --csv data/file.csv [--limit 100]   Run the homework producer
  shell                      Open a shell in the Python toolbox
  sql sql/file.sql           Execute SQL with clickhouse-client
  export sql/query.sql results/file.csv       Export SELECT as CSVWithNames
  status                     Show container state
  down                       Stop containers, keep their data volumes
Set CONTAINER_ENGINE=docker or podman to select the runtime explicitly.
EOF
}

command_name="${1:-help}"
if [[ "$command_name" == help || "$command_name" == --help || "$command_name" == -h ]]; then
  usage
  exit 0
fi
shift

case "${CONTAINER_ENGINE:-auto}" in
  docker) COMPOSE=(docker compose) ;;
  podman) COMPOSE=(podman compose) ;;
  auto)
    if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
      COMPOSE=(docker compose)
    elif command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
      COMPOSE=(podman compose)
    else
      echo "Start Docker Desktop or the Podman machine first." >&2
      exit 1
    fi
    ;;
  *) echo "CONTAINER_ENGINE must be docker or podman." >&2; exit 1 ;;
esac

compose() {
  "${COMPOSE[@]}" --project-directory "$ROOT" -f "$ROOT/docker-compose.yml" "$@"
}

check_sql() {
  [[ -f "$1" ]] || { echo "SQL file does not exist: $1" >&2; exit 1; }
  if ! awk '{sub(/--.*/, ""); if ($0 ~ /[^[:space:];]/) found=1} END {exit !found}' "$1"; then
    echo "SQL file contains no statements: $1" >&2
    exit 1
  fi
}

client() {
  compose exec -T clickhouse clickhouse-client --user student --password student --database homework "$@"
}

case "$command_name" in
  up)
    compose --profile tools build toolbox
    compose up -d --wait --wait-timeout 180 kafka kafka-init clickhouse
    ;;
  run) compose run --rm --no-deps -T toolbox python scripts/run.py "$@" ;;
  benchmark) compose run --rm --no-deps -T toolbox python scripts/benchmark.py "$@" ;;
  producer) compose run --rm --no-deps -T toolbox python scripts/csv_to_kafka.py "$@" ;;
  shell) compose run --rm --no-deps toolbox bash ;;
  sql)
    [[ $# -eq 1 ]] || { usage; exit 2; }
    check_sql "$1"
    client --multiquery < "$1"
    ;;
  export)
    [[ $# -eq 2 ]] || { usage; exit 2; }
    check_sql "$1"
    [[ "$2" == results/*.csv && "$2" != *..* ]] || {
      echo "Use an output path inside results/ ending in .csv." >&2; exit 2;
    }
    temp_csv="$(mktemp "$ROOT/results/.export.XXXXXX")"
    trap 'rm -f "$temp_csv"' EXIT
    client --format CSVWithNames < "$1" > "$temp_csv"
    [[ -s "$temp_csv" ]] || { echo "The query returned no CSV output." >&2; exit 1; }
    mv "$temp_csv" "$2"
    echo "Saved $2"
    ;;
  status) compose ps -a ;;
  down) compose down ;;
  *) usage; exit 2 ;;
esac
