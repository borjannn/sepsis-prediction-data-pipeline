{#-
    OLAP roll-up: how do vital signs evolve over the stay, septic vs non-septic patients?
    GROUP BY ROLLUP (stay_phase, day_of_stay) within each outcome group: per-day rows, phase
    subtotals, and a grand total per group (drill-down / roll-up along the stay-hour hierarchy).
-#}
{{ config(materialized='materialized_view') }}

select
    case when stay.is_sepsis_patient then 'septic' else 'non-septic' end as patient_group,
    case when grouping(hour.stay_phase) = 1 then 'ALL' else hour.stay_phase end as stay_phase,
    min(hour.stay_phase_order) as stay_phase_order,
    case when grouping(hour.day_of_stay) = 1 then null else hour.day_of_stay end as day_of_stay,
    grouping(hour.stay_phase, hour.day_of_stay) as grouping_id,
    count(*) as patient_hours,
    round(avg(fact.hr)::numeric, 1) as avg_hr,
    round(avg(fact.map)::numeric, 1) as avg_map,
    round(avg(fact.temp)::numeric, 2) as avg_temp,
    round(avg(fact.resp)::numeric, 1) as avg_resp,
    round(avg(fact.o2sat)::numeric, 1) as avg_o2sat,
    round(avg(fact.sofa_partial)::numeric, 2) as avg_sofa_partial
from {{ ref('fact_patient_hour') }} as fact
inner join {{ ref('dim_stay_hour') }} as hour on fact.stay_hour_key = hour.stay_hour_key
inner join {{ ref('fact_icu_stay') }} as stay on fact.patient_key = stay.patient_key
group by stay.is_sepsis_patient, rollup (hour.stay_phase, hour.day_of_stay)
