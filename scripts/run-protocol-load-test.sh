#!/bin/bash

# Runs the protocol version benchmarks. See benchmarks/protocol-versions/README.md.
#
# The caller owns the cluster lifecycle. Run each live sweep against its own fresh broker so earlier
# workloads cannot bias its measurements. COMPOSE_FILE and COMPOSE_PROJECT_NAME select the same
# broker for diagnostics and the JMX guard; PROTOCOL_BENCH_BROKERS selects its client endpoint.

set -euo pipefail

cd "$(dirname "$0")/.."

CLIENT_CPUS="${CLIENT_CPUS:-6,7}"
if command -v taskset >/dev/null 2>&1; then
  RUN=(taskset -c "$CLIENT_CPUS" ./scripts/node)
else
  # CPU pinning is available on the Linux benchmark runner but not on macOS.
  RUN=(./scripts/node)
fi
TIER="${1:?usage: run-protocol-load-test.sh <0|1|2> [produce|consume]}"
SWEEP="${2:-}"
ARTIFACT_PREFIX="${PROTOCOL_BENCH_ARTIFACT_PREFIX:-}"

if [[ "$TIER" != 0 && "$TIER" != 1 && "$TIER" != 2 ]]; then
  echo 'Expected protocol tier 0, 1, or 2.' >&2
  exit 2
fi
if [[ "$TIER" != 0 && "$SWEEP" != produce && "$SWEEP" != consume ]]; then
  echo 'A live benchmark requires one sweep: produce or consume.' >&2
  exit 2
fi

banner () {
  echo
  echo "=============================================================="
  echo "$1"
  echo "=============================================================="
}

record_environment () {
  banner "Environment"
  uname -sr
  if command -v nproc >/dev/null 2>&1; then
    echo "cores: $(nproc), client pinned to $CLIENT_CPUS"
  else
    echo "cores: $(getconf _NPROCESSORS_ONLN), client CPU pinning unavailable"
  fi
  if command -v free >/dev/null 2>&1; then
    free -g | head -2
  fi
  ./scripts/node --version
  if [[ "$TIER" == 1 ]]; then
    # The image tag is not the Apache Kafka version, so ask the modern broker itself.
    docker compose exec -T broker-single kafka-topics --version
  else
    # Kafka 1.1.0 does not support kafka-topics --version. Record the running image instead.
    docker compose images broker-single
  fi
}

if [[ "$TIER" == 0 ]]; then
  banner "Tier 0 — codec microbenchmark (no broker)"
  "${RUN[@]}" benchmarks/protocol-versions/codecs.ts
else
  record_environment
  if [[ "$TIER" == 1 ]]; then
    export PROTOCOL_BENCH_PIN=true
    banner "Guard 1 — broker side record batch conversions"
    "${RUN[@]}" benchmarks/protocol-versions/guards.ts
  else
    banner "Tier 2 — sanity check against Apache Kafka 1.1.0"
    echo "Not a comparison: different broker, JVM and storage engine. Only checks that the client"
    echo "behaves sanely when it genuinely has no choice but to speak the old protocol."
    # Let the client negotiate naturally and land on the legacy codecs.
    export PROTOCOL_BENCH_PIN=false
  fi

  banner "Tier ${TIER} — ${SWEEP} sweep"
  PROTOCOL_BENCH_ARTIFACT="${ARTIFACT_PREFIX}tier${TIER}-${SWEEP}" \
    "${RUN[@]}" "benchmarks/protocol-versions/${SWEEP}-versions.ts"
fi

banner "Done — artifacts in regression/artifacts/"
ls -la regression/artifacts/ | grep -E 'tier[012]' || true
