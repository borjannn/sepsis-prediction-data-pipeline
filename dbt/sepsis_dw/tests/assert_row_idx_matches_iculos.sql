-- row_idx (used by the official utility) equals iculos - min(iculos) for every patient, because
-- hours are contiguous. 21.8 % of records start after ICU hour 1, so row_idx <> iculos - 1.
select patient_id, iculos, row_idx
from (
    select
        patient_id,
        iculos,
        row_idx,
        iculos - min(iculos) over (partition by patient_id) as expected_row_idx
    from {{ ref('stg_patient_hour') }}
) as hours
where row_idx <> expected_row_idx
