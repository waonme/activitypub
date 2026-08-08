#!/bin/sh
set -eu

CONFIG_PATH="${CONFIG_PATH:-/app/config.yaml}"
export CONFIG_PATH

if [ ! -e "$CONFIG_PATH" ]; then
  echo "Config not found: $CONFIG_PATH" >&2
  echo "Mount config.yaml to /app/config.yaml, or set CONFIG_PATH to a config file or directory." >&2
  exit 1
fi

if [ "${MIGRATE_ON_STARTUP:-true}" = "true" ]; then
  max_attempts="${MIGRATION_ATTEMPTS:-30}"
  delay_seconds="${MIGRATION_DELAY_SECONDS:-2}"
  attempt=1

  echo "Running database migrations..."
  while ! node --import tsx ./src/migrate.ts; do
    if [ "$attempt" -ge "$max_attempts" ]; then
      echo "Database migration failed after $max_attempts attempts." >&2
      exit 1
    fi

    echo "Database migration failed. Retrying in ${delay_seconds}s ($attempt/$max_attempts)..." >&2
    attempt=$((attempt + 1))
    sleep "$delay_seconds"
  done
fi

exec "$@"
