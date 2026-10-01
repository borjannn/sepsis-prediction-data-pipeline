{#-
    OLAP cube: where is sepsis most prevalent?
    GROUP BY CUBE over hospital x ICU unit x age band x gender: every combination and every
    subtotal, labelled with GROUPING() ('ALL' marks a rolled-up dimension).
-#}
{{ config(materialized='materialized_view') }}

select
    case when grouping(hospital.hospital_code) = 1 then 'ALL' else hospital.hospital_code end
        as hospital,
    case when grouping(unit.unit_code) = 1 then 'ALL' else unit.unit_code end as icu_unit,
    case when grouping(patient.age_band) = 1 then 'ALL' else patient.age_band end as age_band,
    case when grouping(patient.gender_label) = 1 then 'ALL' else patient.gender_label end
        as gender,
    grouping(hospital.hospital_code, unit.unit_code, patient.age_band, patient.gender_label)
        as grouping_id,
    count(*) as patients,
    count(*) filter (where stay.is_sepsis_patient) as septic_patients,
    round(avg(stay.is_sepsis_patient::int) * 100, 2) as sepsis_rate_pct,
    percentile_cont(0.5) within group (order by stay.icu_los_hours) as median_los_hours
from {{ ref('fact_icu_stay') }} as stay
inner join {{ ref('dim_patient') }} as patient on stay.patient_key = patient.patient_key
inner join {{ ref('dim_hospital') }} as hospital on stay.hospital_key = hospital.hospital_key
inner join {{ ref('dim_icu_unit') }} as unit on stay.unit_key = unit.unit_key
group by cube (hospital.hospital_code, unit.unit_code, patient.age_band, patient.gender_label)
