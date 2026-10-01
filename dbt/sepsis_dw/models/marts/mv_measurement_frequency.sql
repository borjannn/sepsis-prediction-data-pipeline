{#-
    OLAP grouping sets: how much, and how differently, do the hospitals measure?
    GROUPING SETS ((category), (category, variable), (hospital, category), (hospital, variable)):
    observation counts, observations per patient-day, and out-of-range / unit-mismatch shares.
    Hospital-specific gaps (e.g. EtCO2 never recorded at A) are shortcut risks for modelling.
-#}
{{ config(materialized='materialized_view') }}

with patient_days as (

    select hospital_key, sum(n_hours_recorded) / 24.0 as patient_days
    from {{ ref('fact_icu_stay') }}
    group by hospital_key

),

observations as (

    select
        hospital.hospital_code,
        variable.category,
        variable.variable,
        fact.is_out_of_range,
        fact.is_unit_mismatch,
        fact.patient_key
    from {{ ref('fact_measurement') }} as fact
    inner join {{ ref('dim_variable') }} as variable on fact.variable_key = variable.variable_key
    inner join {{ ref('dim_hospital') }} as hospital on fact.hospital_key = hospital.hospital_key

),

grouped as (

    select
        case when grouping(hospital_code) = 1 then 'ALL' else hospital_code end as hospital,
        category,
        case when grouping(variable) = 1 then 'ALL' else variable end as variable,
        grouping(hospital_code, category, variable) as grouping_id,
        count(*) as n_observations,
        count(distinct patient_key) as n_patients_measured,
        round(avg(is_out_of_range::int) * 100, 3) as out_of_range_pct,
        round(avg(is_unit_mismatch::int) * 100, 3) as unit_mismatch_pct
    from observations
    group by grouping sets (
        (category),
        (category, variable),
        (hospital_code, category),
        (hospital_code, category, variable)
    )

)

select
    grouped.*,
    round(
        grouped.n_observations / case
            when grouped.hospital = 'ALL' then (select sum(patient_days) from patient_days)
            else (
                select pd.patient_days
                from patient_days as pd
                inner join {{ ref('dim_hospital') }} as h on pd.hospital_key = h.hospital_key
                where h.hospital_code = grouped.hospital
            )
        end,
        3
    ) as observations_per_patient_day
from grouped
