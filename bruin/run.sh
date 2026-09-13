#!/usr/bin/env bash
# Render Bruin's connection file from environment variables, then validate and
# run the pipeline. Extra arguments are passed to `bruin run` (e.g. --full-refresh).
set -euo pipefail
cd "$(dirname "$0")"

# Bruin looks for a git repository root; the container copy of the project isn't one.
if [ ! -d .git ]; then
  git init -q . && git -c user.email=bruin@local -c user.name=bruin commit -q --allow-empty -m init
fi

cat > .bruin.yml <<EOF
default_environment: default
environments:
  default:
    connections:
      postgres:
        - name: warehouse
          host: "${PG_HOST:-postgres}"
          port: ${PG_PORT:-5432}
          username: "${PG_USER}"
          password: "${PG_PASSWORD}"
          database: "${PG_DATABASE:-bikeshare}"
          ssl_mode: disable
EOF

bruin validate --config-file .bruin.yml .

# Append-strategy assets don't create their table on Postgres; the very first run
# is therefore a full refresh (which creates it).
first_run=$(python - <<'PY'
import os, psycopg2
conn = psycopg2.connect(host=os.getenv("PG_HOST", "postgres"), port=os.getenv("PG_PORT", "5432"),
                        user=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"],
                        dbname=os.getenv("PG_DATABASE", "bikeshare"))
with conn, conn.cursor() as cur:
    cur.execute("select to_regclass('bruin_mart.network_health_snapshots') is null")
    print("yes" if cur.fetchone()[0] else "no")
PY
)
extra=()
if [ "$first_run" = "yes" ]; then
  echo "first run: creating append tables with --full-refresh"
  extra+=(--full-refresh)
fi

bruin run --config-file .bruin.yml --no-color "${extra[@]}" "$@" .
