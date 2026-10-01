{#-
    Patient dimension: descriptive attributes only (measures such as LOS and the sepsis outcome
    live in fact_icu_stay). SCD Type 1: the source is a static snapshot.
    patient_key is a deterministic surrogate (row_number over the natural key) because every
    run is a full refresh of the same snapshot.
-#}
{{ config(post_hook=[
    "{{ primary_key('patient_key') }}",
    "{{ unique_key('patient_id') }}",
]) }}

with patients as (

    select distinct on (patient_id)
        patient_id,
        age,
        gender,
        hosp_adm_time,
        static_attr_conflict
    from {{ ref('int_patient_hour_clean') }}
    order by patient_id, iculos

)

select
    (row_number() over (order by patient_id))::integer as patient_key,
    patient_id,
    age,
    age = 100 as age_top_coded,
    case
        when age < 18 then '<18'
        when age < 45 then '18-44'
        when age < 65 then '45-64'
        when age < 80 then '65-79'
        else '80+'
    end as age_band,
    case gender when 0 then 'female' when 1 then 'male' else 'unknown' end as gender_label,
    hosp_adm_time as hosp_adm_time_hours,
    static_attr_conflict
from patients
