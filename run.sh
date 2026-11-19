#!/bin/bash
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

docker build -t spark-mysql-driver:latest "$SCRIPT_DIR"

docker run --rm --env-file .env spark-mysql-driver python read_mysql.py
