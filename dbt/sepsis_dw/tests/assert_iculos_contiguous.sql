-- Hours of stay are contiguous per patient (+1 per row). Returns patients with gaps.
-- Rolling windows use RANGE frames on iculos, so gaps would not corrupt features; this is a
-- data-format check, see docs/data_profile.md.
{{ config(severity='warn') }}

select
    patient_id,
    min(iculos) as min_iculos,
    max(iculos) as max_iculos,
    count(*) as n_rows
from {{ ref('stg_patient_hour') }}
group by patient_id
having max(iculos) - min(iculos) + 1 <> count(*)
