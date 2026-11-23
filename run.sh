#!/bin/bash
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
COMMAND=${1:-}

if [ -z "$COMMAND" ]; then
  echo "Usage: $0 print_json | <python_script[.py]> [script arguments...]" >&2
  exit 2
fi
shift

if [ "$COMMAND" = "print_json" ]; then
  if [ "$#" -ne 0 ]; then
    echo "Usage: $0 print_json" >&2
    exit 2
  fi

  exec docker run --rm -i --env-file "$SCRIPT_DIR/.env" \
    --entrypoint sh mysql:8.0 -c '
      export MYSQL_PWD="$MYSQL_PASSWORD"
      exec mysql --protocol=TCP \
        --host="$MYSQL_HOST" --port="$MYSQL_PORT" --user="$MYSQL_USER" \
        --database="${MYSQL_DATABASE:-olympic_dataset}" \
        --batch --raw --skip-column-names
    ' <<'SQL'
SELECT JSON_OBJECT(
    'athlete_id', athlete_id,
    'sport', sport,
    'medal', medal,
    'country_noc', country_noc
)
FROM athlete_event_results
WHERE athlete_id IS NOT NULL AND sport IS NOT NULL
LIMIT 1;
SQL
fi

PYTHON_SCRIPT="${COMMAND%.py}.py"
if [[ "$COMMAND" == -* || "$COMMAND" == */* || "$COMMAND" == . || "$COMMAND" == .. ||
      "$COMMAND" == .py || ! -f "$SCRIPT_DIR/$PYTHON_SCRIPT" ]]; then
  echo "Python script not found in $SCRIPT_DIR: $COMMAND" >&2
  exit 2
fi

docker build -t spark-mysql-driver:latest "$SCRIPT_DIR"

DOCKER_ARGS=(--rm --env-file "$SCRIPT_DIR/.env")
if [ "$PYTHON_SCRIPT" != "read_mysql.py" ]; then
  DOCKER_ARGS+=(-e DATA_DIR=/app/data -v "$SCRIPT_DIR/data:/app/data")
fi

exec docker run "${DOCKER_ARGS[@]}" spark-mysql-driver python "$PYTHON_SCRIPT" "$@"
