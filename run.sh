#!/bin/bash
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

case "${1:-}" in
  read_mysql)
    PYTHON_SCRIPT=read_mysql.py
    shift
    ;;
  landing_to_bronze)
    PYTHON_SCRIPT=landing_to_bronze.py
    shift
    ;;
  *)
    echo "Usage: $0 {read_mysql|landing_to_bronze} [script arguments...]" >&2
    exit 2
    ;;
esac

docker build -t spark-mysql-driver:latest "$SCRIPT_DIR"

if [ "$PYTHON_SCRIPT" = "read_mysql.py" ]; then
  docker run --rm \
    --env-file "$SCRIPT_DIR/.env" \
    spark-mysql-driver python "$PYTHON_SCRIPT" "$@"
else
  docker run --rm \
    --env-file "$SCRIPT_DIR/.env" \
    -e DATA_DIR=/app/data \
    -v "$SCRIPT_DIR/data:/app/data" \
    spark-mysql-driver python "$PYTHON_SCRIPT" "$@"
fi
