-- Row-count reconciliation between layers: no rows lost or duplicated by the gold joins.
-- Returns one row per mismatching pair.
with counts as (
    select 'fact_patient_hour' as gold_table,
           (select count(*) from {{ ref('fact_patient_hour') }}) as gold_rows,
           (select count(*) from {{ ref('int_patient_hour_clean') }}) as silver_rows
    union all
    select 'fact_measurement',
           (select count(*) from {{ ref('fact_measurement') }}),
           (select count(*) from {{ ref('int_measurement_long') }})
    union all
    select 'fact_icu_stay',
           (select count(*) from {{ ref('fact_icu_stay') }}),
           (select count(*) from {{ ref('int_patient_sepsis') }})
    union all
    select 'dim_patient',
           (select count(*) from {{ ref('dim_patient') }}),
           (select count(distinct patient_id) from {{ ref('stg_patient_hour') }})
)

select * from counts where gold_rows <> silver_rows
