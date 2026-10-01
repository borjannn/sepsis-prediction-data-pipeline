{#-
    Accumulating snapshot fact: one row per ICU stay. Milestones are role-playing foreign keys to
    dim_stay_hour (admission = first recorded hour, first positive label, estimated sepsis
    onset, last recorded hour). Measures are whole-stay OUTCOMES (LOS, sepsis, worst values):
    exported only with an outcome_ prefix and never used as features (docs/PLAN.md §4.2).
-#}
{{ config(
    tags=['outcome'],
    post_hook=[
        "{{ primary_key('patient_key') }}",
        "{{ foreign_key('patient_key', ref('dim_patient'), 'patient_key') }}",
        "{{ foreign_key('hospital_key', ref('dim_hospital'), 'hospital_key') }}",
        "{{ foreign_key('unit_key', ref('dim_icu_unit'), 'unit_key') }}",
        "{{ foreign_key('admit_stay_hour_key', ref('dim_stay_hour'), 'stay_hour_key') }}",
        "{{ foreign_key('first_positive_stay_hour_key', ref('dim_stay_hour'), 'stay_hour_key') }}",
        "{{ foreign_key('onset_stay_hour_key', ref('dim_stay_hour'), 'stay_hour_key') }}",
        "{{ foreign_key('last_stay_hour_key', ref('dim_stay_hour'), 'stay_hour_key') }}",
    ],
) }}

with hourly as (

    select
        clean.patient_id,
        max(clean.source_set) as source_set,
        max(clean.unit_code) as unit_code,
        max(clean.lactate) as max_lactate,
        min(clean.map) as min_map,
        max(clean.hr) as max_hr,
        max(clean.temp) as max_temp,
        min(clean.platelets) as min_platelets,
        max(features.sofa_partial) as max_sofa_partial,
        max(features.sirs_count) as max_sirs_count,
        sum(features.n_vitals_measured) as n_vital_observations,
        sum(features.n_labs_measured) as n_lab_observations,
        avg(
            (
                clean.hr is not null and clean.o2sat is not null and clean.sbp is not null
                and clean.map is not null and clean.resp is not null
            )::int
        ) as pct_hours_with_full_vitals
    from {{ ref('int_patient_hour_clean') }} as clean
    inner join {{ ref('int_patient_hour_features') }} as features
        on clean.patient_id = features.patient_id and clean.iculos = features.iculos
    group by clean.patient_id

)

select
    patients.patient_key,
    hospitals.hospital_key,
    units.unit_key,
    sepsis.first_iculos as admit_stay_hour_key,
    sepsis.first_positive_iculos as first_positive_stay_hour_key,
    sepsis.sepsis_onset_iculos as onset_stay_hour_key,
    sepsis.last_iculos as last_stay_hour_key,
    sepsis.icu_los_hours,
    sepsis.n_hours_recorded,
    sepsis.is_sepsis_patient,
    sepsis.sepsis_onset_iculos as hours_admission_to_onset,
    sepsis.onset_left_censored,
    sepsis.onset_after_record_end,
    hourly.n_vital_observations,
    hourly.n_lab_observations,
    hourly.max_lactate,
    hourly.min_map,
    hourly.max_hr,
    hourly.max_temp,
    hourly.min_platelets,
    hourly.max_sofa_partial,
    hourly.max_sirs_count,
    hourly.pct_hours_with_full_vitals
from {{ ref('int_patient_sepsis') }} as sepsis
inner join hourly
    on sepsis.patient_id = hourly.patient_id
inner join {{ ref('dim_patient') }} as patients
    on sepsis.patient_id = patients.patient_id
inner join {{ ref('dim_hospital') }} as hospitals
    on hourly.source_set = hospitals.hospital_code
inner join {{ ref('dim_icu_unit') }} as units
    on hourly.unit_code = units.unit_code
