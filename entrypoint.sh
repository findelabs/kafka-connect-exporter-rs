#!/bin/bash
set -e
echo "$@"

exec /app/bin/kafka-connect-exporter-rs "$@"
