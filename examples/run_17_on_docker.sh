#!/usr/bin/env bash
# Run example 17 (logging through c66-data-connection-layer) against the
# Postgres running in your local Docker.
#
#   cd ~/dataproducts/c66_logger
#   bash examples/run_17_on_docker.sh                 # first running container with "postgres" in its image/name
#   CONTAINER=my-pg bash examples/run_17_on_docker.sh # or pick the container
#
# What it does (nothing is dropped or overwritten):
#   1. finds the container, its user/password (POSTGRES_* env) and published port
#   2. creates database c66_sandbox in it, once, with stub app.* tables and the
#      exact logs.* DDL (tests/sql/app_stub.sql + docs/sql/logs_schema.sql)
#   3. makes .venv here and installs c66_logger + ../c66-data-connection-layer
#   4. works out which address on the Mac reaches that container. If another Postgres
#      holds the port, it runs the example inside the container's network instead
#   5. runs examples/17_data_connection_layer.py and prints what landed
#
# Then in DBeaver: the host:port printed below, database c66_sandbox, schema logs.
set -euo pipefail

cd "$(dirname "$0")/.."
SANDBOX_DB="${SANDBOX_DB:-c66_sandbox}"
DCL_DIR="${DCL_DIR:-../c66-data-connection-layer}"

command -v docker >/dev/null || { echo "docker CLI not found"; exit 1; }

# 1. container, credentials, port ------------------------------------------------
if [ -z "${CONTAINER:-}" ]; then
  CONTAINER=$(docker ps --format '{{.Names}} {{.Image}}' | awk 'tolower($0) ~ /postgres|postgis|pg/ {print $1; exit}')
fi
[ -n "${CONTAINER:-}" ] || { echo "no running Postgres container found; set CONTAINER=<name>"; docker ps; exit 1; }

env_of() { docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$CONTAINER" | sed -n "s/^$1=//p" | head -1; }
PGUSER_IN=$(env_of POSTGRES_USER); PGUSER_IN=${PGUSER_IN:-postgres}
PGPASS_IN=$(env_of POSTGRES_PASSWORD)
PORT=$(docker port "$CONTAINER" 5432/tcp 2>/dev/null | head -1 | sed 's/.*://')
[ -n "$PORT" ] || { echo "container $CONTAINER doesn't publish port 5432 to the host (docker run -p 5432:5432 ...)"; exit 1; }
echo "container: $CONTAINER   user: $PGUSER_IN   host port: $PORT   database: $SANDBOX_DB"

psql_in() { docker exec -i -e PGPASSWORD="$PGPASS_IN" "$CONTAINER" psql -v ON_ERROR_STOP=1 -q -U "$PGUSER_IN" "$@"; }

# 2. sandbox database (created once) ---------------------------------------------
if [ "$(psql_in -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname = '$SANDBOX_DB'")" != "1" ]; then
  echo "creating database $SANDBOX_DB with app.* stub tables and logs.* tables"
  psql_in -d postgres -c "CREATE DATABASE \"$SANDBOX_DB\""
  psql_in -d "$SANDBOX_DB" < tests/sql/app_stub.sql
  psql_in -d "$SANDBOX_DB" < docs/sql/logs_schema.sql
else
  echo "database $SANDBOX_DB already exists; reusing it"
fi

# 3. Python environment ----------------------------------------------------------
PY=${PYTHON:-python3}
"$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' || {
  echo "c66-data-connection-layer needs Python 3.10+; found $("$PY" --version). Set PYTHON=/path/to/python3.1x"; exit 1; }
[ -d "$DCL_DIR" ] || { echo "c66-data-connection-layer not found at $DCL_DIR (set DCL_DIR=...)"; exit 1; }
[ -x .venv/bin/python ] || "$PY" -m venv .venv
.venv/bin/python -m pip install -q --upgrade pip
.venv/bin/python -m pip install -q -e ".[postgres]" -e "$DCL_DIR[postgres,yaml]"

# 4. find the address that reaches THIS container -------------------------------
# Another Postgres on the Mac (Homebrew, Postgres.app, another container) can hold
# localhost:$PORT while Docker listens on 0.0.0.0:$PORT. Compare server identities.
CONTAINER_SID=$(psql_in -d postgres -Atc "SELECT system_identifier FROM pg_control_system()")
probe() {
  P_HOST="$1" P_PORT="$PORT" P_USER="$PGUSER_IN" P_PASS="$PGPASS_IN" P_DB="$SANDBOX_DB" .venv/bin/python - <<'PY'
import os, sys
import psycopg
try:
    with psycopg.connect(host=os.environ["P_HOST"], port=os.environ["P_PORT"], user=os.environ["P_USER"],
                         password=os.environ["P_PASS"] or None, dbname=os.environ["P_DB"], connect_timeout=3) as c:
        print(c.execute("SELECT system_identifier::text FROM pg_control_system()").fetchone()[0])
except Exception as exc:
    print(str(exc).strip().splitlines()[-1], file=sys.stderr)
    sys.exit(1)
PY
}
HOST=""
LAN_IP=$( (ipconfig getifaddr en0 || ipconfig getifaddr en1 || hostname -I | awk '{print $1}') 2>/dev/null | head -1 || true)
for candidate in localhost 127.0.0.1 $LAN_IP; do
  if sid=$(probe "$candidate" 2>/tmp/c66_probe_err) && [ "$sid" = "$CONTAINER_SID" ]; then
    HOST=$candidate; break
  fi
  echo "  $candidate:$PORT is not $CONTAINER ($( [ -s /tmp/c66_probe_err ] && cat /tmp/c66_probe_err || echo "a different Postgres server"))"
done
if [ -z "$HOST" ]; then
  echo
  echo "Nothing on this Mac's port $PORT leads to $CONTAINER. Something else is listening there:"
  (lsof -nP -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null || true) | sed 's/^/  /'
  echo
  echo "Running example 17 inside $CONTAINER's network instead (a throwaway python:3.12-slim"
  echo "container; your folders are mounted read-only, nothing on the Mac is stopped or changed)."
  echo "To use DBeaver or run from the Mac directly, free the port: stop the other Postgres"
  echo "(brew services stop postgresql@<version>, or quit Postgres.app) or publish the container"
  echo "on another port (ports: \"5434:5432\" in docker-compose)."
  echo
  DCL_ABS=$(cd "$DCL_DIR" && pwd)
  ENC_PASS=$(.venv/bin/python -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$PGPASS_IN")
  docker run --rm --network "container:$CONTAINER" \
    -v "$PWD":/work/c66_logger:ro -v "$DCL_ABS":/work/dcl:ro \
    -e PYTHONPATH=/work/c66_logger:/work/dcl/src -e PYTHONDONTWRITEBYTECODE=1 -e PIP_DISABLE_PIP_VERSION_CHECK=1 \
    -e C66_EXAMPLE_POSTGRES_DSN="postgresql://$PGUSER_IN${PGPASS_IN:+:$ENC_PASS}@localhost:5432/$SANDBOX_DB" \
    -w /work/c66_logger/examples python:3.12-slim \
    sh -c 'pip install -q --root-user-action=ignore "psycopg[binary]>=3.1" pyyaml && python 17_data_connection_layer.py'
  echo
  echo "latest rows in $SANDBOX_DB (inside $CONTAINER):"
  psql_in -d "$SANDBOX_DB" -c "SELECT run_type, status, duration_ms, started_at FROM logs.log_run ORDER BY started_at DESC LIMIT 3"
  psql_in -d "$SANDBOX_DB" -c "SELECT logger_name, level, message, logged_at FROM logs.log_entry ORDER BY logged_at DESC LIMIT 5"
  exit 0
fi
echo "reaching $CONTAINER at $HOST:$PORT"

# 5. run example 17 --------------------------------------------------------------
ENC_PASS=$(.venv/bin/python -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$PGPASS_IN")
export C66_EXAMPLE_POSTGRES_DSN="postgresql://$PGUSER_IN${PGPASS_IN:+:$ENC_PASS}@$HOST:$PORT/$SANDBOX_DB"
(cd examples && ../.venv/bin/python 17_data_connection_layer.py)

echo
echo "latest rows in $SANDBOX_DB:"
psql_in -d "$SANDBOX_DB" -c "SELECT run_type, status, duration_ms, started_at FROM logs.log_run ORDER BY started_at DESC LIMIT 3"
psql_in -d "$SANDBOX_DB" -c "SELECT logger_name, level, message, logged_at FROM logs.log_entry ORDER BY logged_at DESC LIMIT 5"
