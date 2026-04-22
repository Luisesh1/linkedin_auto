#!/bin/sh
set -e

mkdir -p /app/data /app/static/generated /app/static/debug
chown -R app:app /app/data /app/static/generated /app/static/debug

exec gosu app "$@"
