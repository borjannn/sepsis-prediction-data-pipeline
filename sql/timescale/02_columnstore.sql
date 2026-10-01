-- Stage 8: enable the columnstore (TimescaleDB 2.x compression) on ts.measurement.
-- segmentby variable_key: one segment per variable per chunk (large segments compress well);
-- orderby patient_key, iculos keeps each patient's series contiguous inside a segment.
--
-- The chunks are then converted one at a time, each in its own transaction, by
-- sepsis_pipeline/timescale/addon.py (`CALL convert_to_columnstore(chunk,
-- if_not_columnstore => true)`), so an interrupted run can resume without redoing work.

ALTER TABLE ts.measurement SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'variable_key',
    timescaledb.orderby = 'patient_key, iculos'
);
