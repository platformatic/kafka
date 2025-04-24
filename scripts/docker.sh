#!/bin/bash

set -x -e
[ -z "$CONFIGURATION" ] && CONFIGURATION=multiple
docker-compose -f docker/compose-$CONFIGURATION.yml $@