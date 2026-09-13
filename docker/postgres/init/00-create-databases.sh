#!/bin/bash
# Runs once, on first start of an empty Postgres volume.
# The warehouse DB ($POSTGRES_DB) is created by the image itself;
# here we add a separate metadata database for Kestra.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE kestra'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'kestra')\gexec
EOSQL
