-- Stage 8 comparison: the same data with segmentby = patient_key. Converted chunks cannot change
-- their segmentby, so this is a separate hypertable. Each segment then holds one patient's
-- values in one 24-h chunk (a few dozen rows), far below the ~1000-row batches the columnstore
-- compresses best, so it is expected to compress worse.

DROP TABLE IF EXISTS ts.measurement_by_patient CASCADE;

CREATE TABLE ts.measurement_by_patient (LIKE ts.measurement INCLUDING DEFAULTS);
SELECT create_hypertable('ts.measurement_by_patient', by_range('iculos', 24));

INSERT INTO ts.measurement_by_patient
SELECT patient_key, variable_key, stay_hour_key, value, is_out_of_range, is_unit_mismatch
FROM gold.fact_measurement;

ALTER TABLE ts.measurement_by_patient SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'patient_key',
    timescaledb.orderby = 'variable_key, iculos'
);

DO $$
DECLARE
    chunk regclass;
BEGIN
    FOR chunk IN SELECT show_chunks('ts.measurement_by_patient') LOOP
        CALL convert_to_columnstore(chunk);
    END LOOP;
END
$$;
