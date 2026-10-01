-- Stage 8: continuous aggregate of 6-hour buckets per patient and variable (in-range values).
-- Refreshed once in full (static data). Must run in autocommit mode, as its own statement.

DROP MATERIALIZED VIEW IF EXISTS ts.cagg_patient_6h CASCADE;

-- @@ statement break
CREATE MATERIALIZED VIEW ts.cagg_patient_6h
WITH (timescaledb.continuous) AS
SELECT
    patient_key,
    variable_key,
    time_bucket(6, iculos) AS bucket,
    avg(value) AS avg_value,
    min(value) AS min_value,
    max(value) AS max_value,
    count(*) AS n
FROM ts.measurement
WHERE NOT is_out_of_range AND NOT is_unit_mismatch
GROUP BY patient_key, variable_key, time_bucket(6, iculos)
WITH NO DATA;

-- @@ statement break: the refresh cannot run inside the implicit multi-statement transaction
CALL refresh_continuous_aggregate('ts.cagg_patient_6h', NULL, NULL);
