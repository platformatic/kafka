#!/usr/bin/env bash

set -o pipefail

MODE="${1:?usage: run-regression-suite.sh <modern|legacy> <version> <lane>}"
VERSION="${2:?usage: run-regression-suite.sh <modern|legacy> <version> <lane>}"
LANE="${3:?usage: run-regression-suite.sh <modern|legacy> <version> <lane>}"
ARTIFACT_DIR="regression/artifacts"
REPORT="$ARTIFACT_DIR/${LANE}-report.md"

mkdir -p "$ARTIFACT_DIR"
rm -f "$REPORT"

declare -a names
declare -a statuses
overall=0

run_suite () {
  local name="$1"
  shift
  local log="$ARTIFACT_DIR/${LANE}-${name}.log"
  local status

  printf 'Running %s for Kafka %s\n' "$name" "$VERSION"
  if "$@" 2>&1 | tee "$log"; then
    status=0
  else
    status="${PIPESTATUS[0]}"
    overall=1
  fi

  names+=("$name")
  statuses+=("$status")
}

if [[ "$MODE" == modern ]]; then
  run_suite integrity pnpm run test:integrity
  run_suite memory pnpm run test:memory
  run_suite performance pnpm run test:performance
  run_suite protocol-load env PROTOCOL_BENCH_ARTIFACT_PREFIX="${LANE}-" \
    ./scripts/run-protocol-load-test.sh 1
elif [[ "$MODE" == legacy ]]; then
  if ! docker compose -f docker-compose.legacy.yml up -d --wait; then
    printf 'Unable to start the legacy Kafka broker\n' >&2
    overall=1
  fi
  run_suite compatibility pnpm run test:compat
  docker compose -f docker-compose.legacy.yml down --volumes
  run_suite protocol-load env PROTOCOL_BENCH_ARTIFACT_PREFIX="${LANE}-" \
    ./scripts/run-protocol-load-test.sh 2
else
  printf 'Unknown regression mode: %s\n' "$MODE" >&2
  exit 2
fi

{
  printf '# Regression lane: %s\n\n' "$LANE"
  printf -- '- Kafka: `%s`\n' "$VERSION"
  printf -- '- Mode: `%s`\n\n' "$MODE"
  printf '| Suite | Result | Log |\n'
  printf '| --- | --- | --- |\n'

  for index in "${!names[@]}"; do
    if [[ "${statuses[$index]}" -eq 0 ]]; then
      result='Passed'
    else
      result="Failed (exit ${statuses[$index]})"
    fi
    printf '| `%s` | %s | `%s-%s.log` |\n' "${names[$index]}" "$result" "$LANE" "${names[$index]}"
  done
} > "$REPORT"

printf '\nWrote %s\n' "$REPORT"
exit "$overall"
