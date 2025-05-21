#!/bin/bash

set -x -e
[ -z "$KAFKA_CONFIGURATION" ] && KAFKA_CONFIGURATION=multiple
docker-compose -f docker/compose-$KAFKA_CONFIGURATION.yml $@