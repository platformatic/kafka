#!/usr/bin/env bash

# Capture the selected Compose project's state before cleanup removes failed containers.
set -euo pipefail

LANE="${1:?usage: collect-kafka-diagnostics.sh <lane>}"
mkdir -p regression/artifacts
docker compose ps --all > "regression/artifacts/${LANE}-containers.log" 2>&1
docker compose logs --no-color > "regression/artifacts/${LANE}-broker.log" 2>&1
