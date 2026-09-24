#!/usr/bin/env bash

set -o pipefail

MODE="${1:?usage: run-regression-suite.sh <modern|redpanda|eventhubs|legacy|performance|protocol> <version> <lane> [tier] [sweep]}"
VERSION="${2:?version is required}"
LANE="${3:?lane is required}"
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

  printf 'Running %s for %s\n' "$name" "$VERSION"
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
elif [[ "$MODE" == redpanda ]]; then
  run_suite e2e pnpm run test:e2e:redpanda
elif [[ "$MODE" == eventhubs ]]; then
  run_suite e2e pnpm run test:e2e:eventhubs
elif [[ "$MODE" == legacy ]]; then
  run_suite compatibility pnpm run test:compat
elif [[ "$MODE" == performance ]]; then
  run_suite performance pnpm run test:performance
elif [[ "$MODE" == protocol ]]; then
  run_suite protocol-load env PROTOCOL_BENCH_ARTIFACT_PREFIX="${LANE}-" \
    ./scripts/run-protocol-load-test.sh "${4:?protocol tier is required}" "${5:?protocol sweep is required}"
else
  printf 'Unknown regression mode: %s\n' "$MODE" >&2
  exit 2
fi

{
  printf '# Regression lane: %s\n\n' "$LANE"
  printf -- '- Broker: `%s`\n' "$VERSION"
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
