{#-
    Stay-hour dimension: replaces a calendar date dimension, because the source has only
    relative time (hours since ICU admission, ICULOS), no absolute timestamps.
    Range 1 .. max(ICULOS) + 6, so the estimated sepsis onset (first positive hour + 6) of
    patients whose onset falls after their last recorded hour still has a member.
    hour_of_stay_day is the hour within the stay day (0-23), NOT clock time.
-#}
{{ config(post_hook="{{ primary_key('stay_hour_key') }}") }}

with bounds as (

    select max(iculos) + 6 as max_hour from {{ ref('stg_patient_hour') }}

)

select
    hour::integer as stay_hour_key,
    ((hour - 1) / 24 + 1)::integer as day_of_stay,
    ((hour - 1) % 24)::smallint as hour_of_stay_day,
    case
        when hour <= 6 then '0-6h'
        when hour <= 24 then '6-24h'
        when hour <= 72 then '1-3d'
        when hour <= 168 then '3-7d'
        else '7d+'
    end as stay_phase,
    case
        when hour <= 6 then 1
        when hour <= 24 then 2
        when hour <= 72 then 3
        when hour <= 168 then 4
        else 5
    end::smallint as stay_phase_order,
    hour <= 24 as is_first_24h
from bounds
cross join generate_series(1, bounds.max_hour) as hour
