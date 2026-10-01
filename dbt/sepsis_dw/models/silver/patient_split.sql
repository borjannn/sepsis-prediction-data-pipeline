{#-
    Deterministic, stratified, patient-level split (docs/PLAN.md, Stage 6a).
    Built in Stage 2, before profiling, so every data-driven decision can be made on train only.
    Reads raw directly (not staging), so the leakage_cutoff test var can never change the split.
-#}
{{ config(tags=['split']) }}

with patients as (

    select
        patient_id,
        source_set,
        max("SepsisLabel")::smallint as is_sepsis
    from {{ source('raw', 'patient_hour') }}
    group by 1, 2

),

ranked as (

    select
        patients.*,
        row_number() over (
            partition by source_set, is_sepsis
            order by md5(patient_id || '{{ var("split_seed") }}')
        ) as rn,
        count(*) over (partition by source_set, is_sepsis) as n
    from patients

)

select
    patient_id,
    source_set,
    is_sepsis,
    case
        when (rn - 1)::float8 / n < 0.70 then 'train'
        when (rn - 1)::float8 / n < 0.85 then 'validation'
        else 'test'
    end as split_random,
    case
        when (rn - 1)::float8 / n < 0.70 then (1 + (rn - 1) % 5)::smallint
    end as cv_fold,
    case
        when source_set = 'B' then 'test'
        when (rn - 1)::float8 / n < 0.70 then 'train'
        else 'validation'
    end as split_hospital_holdout,
    '{{ var("split_version") }}'::text as split_version,
    '{{ var("split_seed") }}'::text as split_seed
from ranked
