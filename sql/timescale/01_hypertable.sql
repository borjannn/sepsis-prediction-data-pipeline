-- Stage 8 (TimescaleDB add-on): load gold.fact_measurement into a hypertable.
-- Isolated in schema ts; nothing in Stages 0-7 depends on it. Re-runnable.

DROP TABLE IF EXISTS ts.measurement CASCADE;

CREATE TABLE ts.measurement (
    patient_key      integer          NOT NULL,
    variable_key     smallint         NOT NULL,
    iculos           integer          NOT NULL,  -- time dimension: hours since ICU admission
    value            double precision NOT NULL,
    is_out_of_range  boolean          NOT NULL,
    is_unit_mismatch boolean          NOT NULL
);

-- Integer time dimension (the source has no timestamps), one chunk per 24 hours of stay.
SELECT create_hypertable('ts.measurement', by_range('iculos', 24));

-- Continuous aggregates and policies on integer time need a "now" function. The dataset is a
-- static snapshot truncated at 336 h, so "now" is the end of the longest possible stay.
CREATE OR REPLACE FUNCTION ts.integer_now() RETURNS integer
    LANGUAGE sql STABLE AS $$ SELECT 336 $$;
SELECT set_integer_now_func('ts.measurement', 'ts.integer_now');

INSERT INTO ts.measurement
SELECT patient_key, variable_key, stay_hour_key, value, is_out_of_range, is_unit_mismatch
FROM gold.fact_measurement
ORDER BY stay_hour_key, patient_key;

-- Same lookup index as gold.fact_measurement (create_hypertable already indexes iculos).
CREATE INDEX measurement_patient_iculos_idx ON ts.measurement (patient_key, iculos);
CREATE INDEX measurement_variable_idx ON ts.measurement (variable_key, iculos);

ANALYZE ts.measurement;
