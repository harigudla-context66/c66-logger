#!/usr/bin/env bash
# Run example 17 against the Postgres installed directly on this Mac
# (Homebrew / Postgres.app), the one listening on localhost:5432.
#
#   bash examples/run_17_on_local.sh
#
# 1. finds the running postgres on port 5432, its bin/ and data folder
# 2. tries to log in without a password (local installs usually trust localhost)
# 3. creates database c66_sandbox once (stub app.* + exact logs.* DDL)
# 4. runs examples/17_data_connection_layer.py with .venv (made by run_17_on_docker.sh)
# Nothing is changed if step 2 fails: it prints pg_hba.conf's rules and stops.
set -uo pipefail
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

cd "$(dirname "$0")/.."
PORT="${PORT:-5432}"
SANDBOX_DB="${SANDBOX_DB:-c66_sandbox}"

# 1. which postgres owns the port ------------------------------------------------
PID=""
for p in $(lsof -nP -iTCP:"$PORT" -sTCP:LISTEN -t 2>/dev/null); do
  case "$(ps -o comm= -p "$p")" in *postgres*) PID=$p; break;; esac
done
[ -n "$PID" ] || { echo "no postgres process listening on port $PORT"; exit 1; }
CMD=$(ps -o command= -p "$PID")
BIN=$(dirname "$(echo "$CMD" | awk '{print $1}')")
DATA=$(echo "$CMD" | sed -n 's/.* -D *\([^ ]*\).*/\1/p')
[ -n "$DATA" ] || DATA=$("$BIN/psql" -h localhost -p "$PORT" -w -d postgres -Atc "show data_directory" 2>/dev/null)
echo "postgres pid $PID"
echo "  command: $CMD"
echo "  bin:     $BIN"
echo "  data:    ${DATA:-unknown}"
"$BIN/postgres" --version

# 2. log in without a password ----------------------------------------------------
USER_OK=""
for u in "$(whoami)" postgres; do
  if out=$(PGCONNECT_TIMEOUT=5 "$BIN/psql" -h localhost -p "$PORT" -U "$u" -w -d postgres -Atc "select current_user || ' superuser=' || usesuper from pg_user where usename = current_user" 2>&1); then
    echo "login without password as $u: OK ($out)"
    USER_OK=$u; break
  else
    echo "login without password as $u: failed ($(echo "$out" | tail -1))"
  fi
done
if [ -z "$USER_OK" ]; then
  echo
  echo "pg_hba.conf rules (${DATA:-?}/pg_hba.conf):"
  grep -vE '^\s*(#|$)' "$DATA/pg_hba.conf" 2>/dev/null | sed 's/^/  /' || echo "  (can't read it)"
  exit 2
fi
echo "roles:"; "$BIN/psql" -h localhost -p "$PORT" -U "$USER_OK" -w -d postgres -Atc "select '  ' || rolname || case when rolsuper then ' (superuser)' else '' end from pg_roles where rolname !~ '^pg_' order by 1"

# 3. sandbox database --------------------------------------------------------------
PSQL=("$BIN/psql" -h localhost -p "$PORT" -U "$USER_OK" -w -v ON_ERROR_STOP=1 -q)
if [ "$("${PSQL[@]}" -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname = '$SANDBOX_DB'")" != "1" ]; then
  echo "creating database $SANDBOX_DB"
  "${PSQL[@]}" -d postgres -c "CREATE DATABASE \"$SANDBOX_DB\"" || exit 3
  "${PSQL[@]}" -d "$SANDBOX_DB" -f tests/sql/app_stub.sql || exit 3
  "${PSQL[@]}" -d "$SANDBOX_DB" -f docs/sql/logs_schema.sql || exit 3
else
  echo "database $SANDBOX_DB already exists; reusing it"
fi

# 4. example 17 ---------------------------------------------------------------------
[ -x .venv/bin/python ] || { echo ".venv missing: run examples/run_17_on_docker.sh once, or create it"; exit 4; }
export C66_EXAMPLE_POSTGRES_DSN="postgresql://$USER_OK@localhost:$PORT/$SANDBOX_DB"
(cd examples && ../.venv/bin/python 17_data_connection_layer.py) || exit 5
echo
"${PSQL[@]}" -d "$SANDBOX_DB" -c "SELECT run_type, status, duration_ms, started_at FROM logs.log_run ORDER BY started_at DESC LIMIT 3"
"${PSQL[@]}" -d "$SANDBOX_DB" -c "SELECT logger_name, level, message, logged_at FROM logs.log_entry ORDER BY logged_at DESC LIMIT 5"
echo "DBeaver / pgAdmin: localhost:$PORT, user $USER_OK, no password, database $SANDBOX_DB, schema logs"
