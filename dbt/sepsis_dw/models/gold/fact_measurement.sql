{#-
    Transaction fact: one row per observed clinical value (sparse, EAV-style), ~10.5M rows.
    Keeps every observation, including out-of-range and unit-mismatch values (flagged), so data
    quality can be analysed in the warehouse. Physically ordered by (patient, hour).
-#}
{{ config(
    indexes=[
        {'columns': ['patient_key', 'stay_hour_key']},
        {'columns': ['variable_key']},
    ],
    post_hook=[
        "{{ foreign_key('patient_key', ref('dim_patient'), 'patient_key') }}",
        "{{ foreign_key('hospital_key', ref('dim_hospital'), 'hospital_key') }}",
        "{{ foreign_key('unit_key', ref('dim_icu_unit'), 'unit_key') }}",
        "{{ foreign_key('stay_hour_key', ref('dim_stay_hour'), 'stay_hour_key') }}",
        "{{ foreign_key('variable_key', ref('dim_variable'), 'variable_key') }}",
    ],
) }}

with units as (

    select distinct on (patient_id) patient_id, unit_code
    from {{ ref('int_patient_hour_clean') }}
    order by patient_id, iculos

)

select
    patients.patient_key,
    measurement.iculos as stay_hour_key,
    variables.variable_key,
    hospitals.hospital_key,
    icu_units.unit_key,
    measurement.value,
    measurement.raw_value,
    measurement.is_out_of_range,
    measurement.is_abnormal,
    measurement.was_rescaled,
    measurement.is_unit_mismatch
from {{ ref('int_measurement_long') }} as measurement
inner join {{ ref('dim_patient') }} as patients
    on measurement.patient_id = patients.patient_id
inner join {{ ref('dim_variable') }} as variables
    on measurement.variable = variables.variable
inner join {{ ref('dim_hospital') }} as hospitals
    on measurement.source_set = hospitals.hospital_code
inner join units
    on measurement.patient_id = units.patient_id
inner join {{ ref('dim_icu_unit') }} as icu_units
    on units.unit_code = icu_units.unit_code
order by patients.patient_key, measurement.iculos
