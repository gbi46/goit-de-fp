#!/bin/bash
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
COMMAND=${1:-}

if [ -z "$COMMAND" ]; then
  echo "Usage: $0 create_topics | print_json | stop_local_kafka | <python_script[.py]> [script arguments...]" >&2
  exit 2
fi
shift

LOCAL_KAFKA_NAME=goit-de-fp-kafka
LOCAL_KAFKA_NETWORK=goit-de-fp-net
LOCAL_KAFKA_IMAGE=apache/kafka:4.1.2

# Print the last KEY=value from a file (empty if missing).
env_value() {
  [ -r "$2" ] || return 0
  awk -v key="$1" 'index($0, key "=") == 1 {
    value = substr($0, length(key) + 2)
    sub(/\r$/, "", value)
  } END { print value }' "$2"
}

# Return success when the first broker in a bootstrap list accepts TCP.
broker_reachable() {
  local first=${1%%,*}
  local host=${first%:*} port=${first##*:}
  [ -n "$host" ] && [ -n "$port" ] && [ "$host" != "$port" ] || return 1
  timeout 5 bash -c 'exec 3<>"/dev/tcp/$0/$1"' "$host" "$port" 2>/dev/null
}

# Start a single-node PLAINTEXT Kafka in Docker and create the project topics.
start_local_kafka() {
  local topics_cli=(docker exec "$LOCAL_KAFKA_NAME" /opt/kafka/bin/kafka-topics.sh
    --bootstrap-server "$LOCAL_KAFKA_NAME:9092")

  docker network inspect "$LOCAL_KAFKA_NETWORK" >/dev/null 2>&1 ||
    docker network create "$LOCAL_KAFKA_NETWORK" >/dev/null

  if [ -z "$(docker ps -q --filter "name=^${LOCAL_KAFKA_NAME}$")" ]; then
    docker rm -f "$LOCAL_KAFKA_NAME" >/dev/null 2>&1 || true
    echo "Starting local Kafka container $LOCAL_KAFKA_NAME"
    docker run -d --rm --name "$LOCAL_KAFKA_NAME" --network "$LOCAL_KAFKA_NETWORK" \
      -e KAFKA_NODE_ID=1 \
      -e KAFKA_PROCESS_ROLES=broker,controller \
      -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
      -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://$LOCAL_KAFKA_NAME:9092" \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@$LOCAL_KAFKA_NAME:9093" \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
      -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
      "$LOCAL_KAFKA_IMAGE" >/dev/null
    # A new broker has no data, so old local stream offsets are invalid.
    rm -rf -- "$SCRIPT_DIR/data/checkpoints/streaming_pipeline_local" 2>/dev/null ||
      docker run --rm -v "$SCRIPT_DIR/data:/app/data" --entrypoint rm \
        spark-mysql-driver -rf /app/data/checkpoints/streaming_pipeline_local
  fi

  local attempt
  for attempt in $(seq 1 30); do
    "${topics_cli[@]}" --list >/dev/null 2>&1 && break
    if [ "$attempt" -eq 30 ]; then
      echo "Local Kafka did not become ready." >&2
      exit 1
    fi
    sleep 2
  done

  local topic
  for topic in "$@"; do
    "${topics_cli[@]}" --create --if-not-exists --topic "$topic" --partitions 2 >/dev/null
  done
  echo "Local Kafka ready: $LOCAL_KAFKA_NAME:9092 (topics: $*)"
}

if [ "$COMMAND" = "stop_local_kafka" ]; then
  docker stop "$LOCAL_KAFKA_NAME" >/dev/null 2>&1 && echo "Stopped $LOCAL_KAFKA_NAME" ||
    echo "$LOCAL_KAFKA_NAME is not running"
  docker network rm "$LOCAL_KAFKA_NETWORK" >/dev/null 2>&1 || true
  exit 0
fi

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

# Kafka jobs: use the remote broker, or fall back to a local one.
# KAFKA_MODE=auto (default) | remote | local, from the shell or .env.
if [ "$PYTHON_SCRIPT" = "mysql_to_kafka.py" ] || [ "$PYTHON_SCRIPT" = "streaming_pipeline.py" ]; then
  env_file="$SCRIPT_DIR/.env"
  kafka_mode=${KAFKA_MODE:-$(env_value KAFKA_MODE "$env_file")}
  kafka_mode=${kafka_mode:-auto}
  remote_brokers=$(env_value KAFKA_BOOTSTRAP_SERVERS "$env_file")

  case "$kafka_mode" in
    remote) use_local=0 ;;
    local) use_local=1 ;;
    auto)
      if broker_reachable "$remote_brokers"; then
        use_local=0
      else
        echo "Remote Kafka ${remote_brokers:-<not set>} is unreachable; using local Kafka." >&2
        use_local=1
      fi
      ;;
    *)
      echo "KAFKA_MODE must be auto, remote or local." >&2
      exit 2
      ;;
  esac

  if [ "$use_local" -eq 1 ]; then
    start_local_kafka \
      "$(env_value KAFKA_INPUT_TOPIC "$env_file")" \
      "$(env_value KAFKA_OUTPUT_TOPIC "$env_file")"
    DOCKER_ARGS+=(--network "$LOCAL_KAFKA_NETWORK"
      -e KAFKA_BOOTSTRAP_SERVERS="$LOCAL_KAFKA_NAME:9092"
      -e KAFKA_SECURITY_PROTOCOL=PLAINTEXT
      -e KAFKA_TARGET=local)
  else
    echo "Using remote Kafka $remote_brokers"
    DOCKER_ARGS+=(-e KAFKA_TARGET=remote)
  fi
fi

exec docker run "${DOCKER_ARGS[@]}" spark-mysql-driver python "$PYTHON_SCRIPT" "$@"
