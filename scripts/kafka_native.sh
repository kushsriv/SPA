#!/usr/bin/env bash
# Run a single Kafka broker (KRaft mode) WITHOUT Docker, with a small heap.
# Works on Linux, macOS and Windows-via-WSL2. Needs Java 17+ and ~300-500 MB of RAM.
#
#   scripts/kafka_native.sh start    # download (first time), start broker, create topics
#   scripts/kafka_native.sh stop
#   scripts/kafka_native.sh reset    # stop and delete all Kafka data
set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-3.8.0}"
SCALA_VERSION="2.13"
HEAP="${KAFKA_HEAP:-256M}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$ROOT/.kafka"
KAFKA_HOME="$DIR/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
DATA="$DIR/data"
CONF="$DIR/server.properties"
BOOTSTRAP="localhost:9092"

download() {
  [ -d "$KAFKA_HOME" ] && return
  mkdir -p "$DIR"
  local tgz="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
  echo "Downloading Kafka ${KAFKA_VERSION} (~120 MB, first time only)..."
  curl -fL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${tgz}" -o "$DIR/$tgz"
  tar -xzf "$DIR/$tgz" -C "$DIR" && rm "$DIR/$tgz"
}

write_config() {
  cat > "$CONF" <<CFG
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=$DATA
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
# Keep disk and memory small on laptops
log.retention.hours=24
log.segment.bytes=67108864
num.network.threads=2
num.io.threads=2
CFG
}

create_topics() {
  local k="$KAFKA_HOME/bin/kafka-topics.sh"
  for spec in "fsc.summaries 8" "fsc.raw 8" "fsc.metrics 1"; do
    set -- $spec
    "$k" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1 2>/dev/null
  done
  "$k" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic fsc.snapshots --partitions 8 --replication-factor 1 --config cleanup.policy=compact 2>/dev/null
  "$k" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic fsc.global --partitions 1 --replication-factor 1 --config cleanup.policy=compact 2>/dev/null
  "$k" --bootstrap-server "$BOOTSTRAP" --list
}

start() {
  download
  write_config
  if [ ! -f "$DATA/meta.properties" ]; then
    "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" -c "$CONF" >/dev/null
  fi
  export KAFKA_HEAP_OPTS="-Xmx${HEAP} -Xms${HEAP}"
  "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$CONF"
  echo -n "Waiting for Kafka"
  for _ in $(seq 1 60); do
    if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
      echo " ready."; create_topics; return
    fi
    echo -n "."; sleep 1
  done
  echo " failed; see $KAFKA_HOME/logs/server.log" >&2; exit 1
}

stop() { [ -d "$KAFKA_HOME" ] && "$KAFKA_HOME/bin/kafka-server-stop.sh" 2>/dev/null || true; }

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  reset) stop; sleep 3; rm -rf "$DATA" ;;
  *) echo "usage: $0 {start|stop|reset}" >&2; exit 2 ;;
esac
