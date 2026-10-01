{#-
    What does deterioration look like before sepsis onset?
    Average vitals / labs / scores of septic patients by hour relative to the estimated onset
    (-48 .. +6, negative = before onset), next to a baseline of NON-septic patients matched on
    the same hours of ICU stay (time-in-stay strongly shapes vitals, so an unmatched baseline
    would mislead).
-#}
{{ config(materialized='materialized_view') }}

with non_septic_by_hour as (

    select
        fact.stay_hour_key,
        avg(fact.hr) as hr,
        avg(fact.map) as map,
        avg(fact.resp) as resp,
        avg(fact.temp) as temp,
        avg(fact.lactate) as lactate,
        avg(fact.wbc) as wbc,
        avg(fact.sofa_partial) as sofa_partial,
        avg(fact.sirs_count) as sirs_count
    from {{ ref('fact_patient_hour') }} as fact
    inner join {{ ref('fact_icu_stay') }} as stay on fact.patient_key = stay.patient_key
    where not stay.is_sepsis_patient
    group by fact.stay_hour_key

)

select
    -fact.label_hours_to_onset as hours_relative_to_onset,
    count(*) as septic_patient_hours,
    {%- for m in ['hr', 'map', 'resp', 'temp', 'lactate', 'wbc', 'sofa_partial', 'sirs_count'] %}
    round(avg(fact.{{ m }})::numeric, 2) as septic_{{ m }},
    round(avg(baseline.{{ m }})::numeric, 2) as baseline_{{ m }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from {{ ref('fact_patient_hour') }} as fact
inner join non_septic_by_hour as baseline on fact.stay_hour_key = baseline.stay_hour_key
where fact.label_hours_to_onset between -6 and 48
group by fact.label_hours_to_onset
