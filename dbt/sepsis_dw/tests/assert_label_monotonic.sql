-- Once SepsisLabel turns 1 it stays 1 until the last row. Returns violating rows.
with labelled as (
    select
        patient_id,
        iculos,
        sepsis_label,
        lag(sepsis_label) over (partition by patient_id order by row_idx) as previous_label
    from {{ ref('stg_patient_hour') }}
)

select patient_id, iculos, previous_label, sepsis_label
from labelled
where previous_label = 1 and sepsis_label = 0
