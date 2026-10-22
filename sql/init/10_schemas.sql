-- Runs once, on first start of an empty data volume, as POSTGRES_USER (the warehouse owner).

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- One schema per pipeline layer (see docs/PROJECT.md §3).
CREATE SCHEMA IF NOT EXISTS raw;      -- bronze: exact copy of the source + lineage
CREATE SCHEMA IF NOT EXISTS staging;  -- silver views: renamed, typed
CREATE SCHEMA IF NOT EXISTS silver;   -- split, validated, imputed, features, labels
CREATE SCHEMA IF NOT EXISTS gold;     -- star schema
CREATE SCHEMA IF NOT EXISTS mart;     -- ML features, reference stats, OLAP views
CREATE SCHEMA IF NOT EXISTS audit;    -- ETL runs and data-quality results
CREATE SCHEMA IF NOT EXISTS ts;       -- TimescaleDB add-on (Stage 8)

-- One row per pipeline stage execution.
CREATE TABLE IF NOT EXISTS audit.etl_run (
    run_id      uuid        PRIMARY KEY DEFAULT uuidv7(),
    stage       text        NOT NULL,
    status      text        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    rows_in     bigint,
    rows_out    bigint,
    error       text,
    git_sha     text,
    params      jsonb       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS etl_run_stage_started_idx ON audit.etl_run (stage, started_at DESC);

-- One row per dbt node executed within a pipeline run (parsed from run_results.json).
CREATE TABLE IF NOT EXISTS audit.dbt_model_run (
    run_id           uuid             NOT NULL REFERENCES audit.etl_run (run_id) ON DELETE CASCADE,
    unique_id        text             NOT NULL,
    status           text             NOT NULL,
    execution_time_s double precision,
    rows_affected    bigint,
    message          text,
    PRIMARY KEY (run_id, unique_id)
);

-- Profiling and data-quality metrics (long format).
CREATE TABLE IF NOT EXISTS audit.dq_results (
    dq_result_id bigint           GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id       uuid             NOT NULL REFERENCES audit.etl_run (run_id) ON DELETE CASCADE,
    layer        text             NOT NULL,
    check_name   text             NOT NULL,
    variable     text,
    metric       text             NOT NULL,
    value        double precision,
    recorded_at  timestamptz      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dq_results_run_idx ON audit.dq_results (run_id);
