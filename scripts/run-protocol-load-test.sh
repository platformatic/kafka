#!/bin/bash

# Runs the protocol version benchmarks. See benchmarks/protocol-versions/README.md.
#
# The ordering matters and is easy to get wrong by hand: the broker has to be restarted between the
# producer and consumer sweeps so log segment growth does not favour whichever sweep ran second, and
# the legacy stack binds the same ports as the modern one, so tier 2 cannot start until tier 1's
# broker is fully down.

set -euo pipefail

cd "$(dirname "$0")/.."

CLIENT_CPUS="${CLIENT_CPUS:-6,7}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.perf.yml)
RUN=(taskset -c "$CLIENT_CPUS" ./scripts/node)
TIERS="${1:-all}"

banner () {
  echo
  echo "=============================================================="
  echo "$1"
  echo "=============================================================="
}

record_environment () {
  banner "Environment"
  uname -sr
  echo "cores: $(nproc), client pinned to $CLIENT_CPUS"
  free -g | head -2
  ./scripts/node --version
  # The image tag is not the Apache Kafka version, so ask the broker itself.
  docker exec broker-single kafka-topics --version 2>/dev/null || echo 'broker not up yet'
}

if [[ "$TIERS" == "all" || "$TIERS" == "0" ]]; then
  banner "Tier 0 — codec microbenchmark (no broker)"
  "${RUN[@]}" benchmarks/protocol-versions/codecs.ts
fi

if [[ "$TIERS" == "all" || "$TIERS" == "1" ]]; then
  banner "Tier 1 — starting isolated single broker"
  "${COMPOSE[@]}" up -d --wait broker-single
  record_environment

  banner "Guard 1 — broker side record batch conversions"
  "${RUN[@]}" benchmarks/protocol-versions/guards.ts

  banner "Tier 1 — Produce sweep"
  "${RUN[@]}" benchmarks/protocol-versions/produce-versions.ts

  banner "Tier 1 — restarting broker before the consumer sweep"
  "${COMPOSE[@]}" restart broker-single
  "${COMPOSE[@]}" up -d --wait broker-single

  banner "Tier 1 — Fetch sweep"
  "${RUN[@]}" benchmarks/protocol-versions/consume-versions.ts
fi

if [[ "$TIERS" == "all" || "$TIERS" == "2" ]]; then
  banner "Tier 2 — sanity check against Apache Kafka 1.1.0"
  echo "Not a comparison: different broker, JVM and storage engine. Only checks that the client"
  echo "behaves sanely when it genuinely has no choice but to speak the old protocol."
  "${COMPOSE[@]}" down
  docker compose -f docker-compose.legacy.yml up -d --wait broker-single

  # No pinning: let the client negotiate naturally and land on the legacy codecs.
  PROTOCOL_BENCH_PIN=false "${RUN[@]}" benchmarks/protocol-versions/produce-versions.ts
  PROTOCOL_BENCH_PIN=false "${RUN[@]}" benchmarks/protocol-versions/consume-versions.ts

  docker compose -f docker-compose.legacy.yml down
fi

banner "Done — artifacts in regression/artifacts/"
ls -la regression/artifacts/ | grep -E 'tier[012]' || true
