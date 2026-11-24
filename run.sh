#!/bin/bash
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
COMMAND=${1:-}

if [ -z "$COMMAND" ]; then
  echo "Usage: $0 create_topics | print_json | <python_script[.py]> [script arguments...]" >&2
  exit 2
fi
shift

if [ "$COMMAND" = "create_topics" ]; then
  if [ "$#" -ne 0 ]; then
    echo "Usage: $0 create_topics" >&2
    exit 2
  fi

  kcat_config="$SCRIPT_DIR/.kcat-goit.conf"
  env_file="$SCRIPT_DIR/.env"
  for config_file in "$env_file" "$kcat_config"; do
    if [ ! -r "$config_file" ]; then
      echo "Cannot read $config_file" >&2
      exit 2
    fi
  done

  config_value() {
    awk -v key="$1" 'index($0, key "=") == 1 {
      value = substr($0, length(key) + 2)
      sub(/\r$/, "", value)
    } END { print value }' "$2"
  }

  input_topic=$(config_value KAFKA_INPUT_TOPIC "$env_file")
  output_topic=$(config_value KAFKA_OUTPUT_TOPIC "$env_file")
  for topic in "$input_topic" "$output_topic"; do
    if [[ ! "$topic" =~ ^[a-zA-Z0-9._-]+$ || "$topic" == . || "$topic" == .. || "$topic" == myname_* || ${#topic} -gt 249 ]]; then
      echo "Set valid topic names in KAFKA_INPUT_TOPIC and KAFKA_OUTPUT_TOPIC in .env and replace the example prefix." >&2
      exit 2
    fi
  done
  if [ "$input_topic" = "$output_topic" ]; then
    echo "KAFKA_INPUT_TOPIC and KAFKA_OUTPUT_TOPIC must be different." >&2
    exit 2
  fi

  brokers=$(config_value KAFKA_BOOTSTRAP_SERVERS "$env_file")
  brokers=${brokers:-$(config_value bootstrap.servers "$kcat_config")}
  protocol=$(config_value security.protocol "$kcat_config")
  mechanism=$(config_value sasl.mechanisms "$kcat_config")
  mechanism=${mechanism:-$(config_value sasl.mechanism "$kcat_config")}
  username=$(config_value sasl.username "$kcat_config")
  password=$(config_value sasl.password "$kcat_config")
  if [[ -z "$brokers" || -z "$username" || -z "$password" || "$mechanism" != PLAIN ||
        ( "$protocol" != SASL_PLAINTEXT && "$protocol" != SASL_SSL ) ]]; then
    echo "Provide a broker address and PLAIN SASL settings in .kcat-goit.conf." >&2
    exit 2
  fi

  kafka_topics="$SCRIPT_DIR/kafka/bin/kafka-topics.sh"
  if [ ! -x "$kafka_topics" ] && ! kafka_topics=$(command -v kafka-topics.sh); then
    if ! command -v docker >/dev/null 2>&1; then
      echo "Install Docker or make kafka-topics.sh available locally." >&2
      exit 127
    fi
  fi

  # Escape the quoted JAAS value, then escape backslashes for Java Properties.
  quote_jaas_value() {
    local value=$1
    value=${value//\\/\\\\}
    value=${value//\"/\\\"}
    printf '%s' "${value//\\/\\\\}"
  }

  client_config=$(mktemp)
  trap 'rm -f -- "$client_config"' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  {
    printf 'security.protocol=%s\nsasl.mechanism=%s\n' "$protocol" "$mechanism"
    printf 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";\n' \
      "$(quote_jaas_value "$username")" "$(quote_jaas_value "$password")"
  } > "$client_config"

  if [ -n "$kafka_topics" ]; then
    kafka_cli=("$kafka_topics")
    cli_config="$client_config"
  else
    cli_config=/tmp/kafka-client.properties
    kafka_cli=(docker run --rm --user "$(id -u):$(id -g)"
      --mount "type=bind,source=$client_config,target=$cli_config,readonly"
      --entrypoint /opt/kafka/bin/kafka-topics.sh apache/kafka:4.1.2)
  fi

  for topic in "$input_topic" "$output_topic"; do
    "${kafka_cli[@]}" --bootstrap-server "$brokers" --command-config "$cli_config" \
      --create --if-not-exists --topic "$topic"
    printf 'Topic ready: %s\n' "$topic"
  done
  exit 0
fi

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
