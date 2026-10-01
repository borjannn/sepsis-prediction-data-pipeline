#!/bin/bash
# Runs once, on first start of an empty data volume.
# Creates the read-only role for the Data Mining team. Table-level SELECT grants
# are issued by dbt (+grants), because dbt recreates tables on every run.
# No `set -u`: the postgres entrypoint sources non-executable .sh files into its own shell.
set -eo pipefail

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$POSTGRES_DB" \
     -v dm_password="$DM_READER_PASSWORD" <<'EOSQL'
CREATE ROLE dm_reader LOGIN PASSWORD :'dm_password';
GRANT CONNECT ON DATABASE :"DBNAME" TO dm_reader;
GRANT USAGE ON SCHEMA gold, mart TO dm_reader;
EOSQL
