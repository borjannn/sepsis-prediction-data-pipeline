{#-
    One row per patient: sepsis outcome and stay milestones. OUTCOME data (known only at the end
    of the stay): never a feature, never upstream of a feature model (docs/PLAN.md §4.2).

    t_sepsis follows the official scoring: first positive row + 6 (labels turn 1 six hours
    before t_sepsis). Patients positive from row 0 are left-censored: given the dataset's
    "t_sepsis >= 4 h" exclusion, their true onset can be up to ~3 h earlier than estimated.
-#}
{{ config(tags=['outcome']) }}

with hours as (

    select patient_id, source_set, iculos, row_idx, sepsis_label
    from {{ ref('stg_patient_hour') }}

),

patients as (

    select
        patient_id,
        source_set,
        max(sepsis_label) = 1 as is_sepsis_patient,
        min(row_idx) filter (where sepsis_label = 1) as first_positive_row_idx,
        min(iculos) filter (where sepsis_label = 1) as first_positive_iculos,
        min(iculos) as first_iculos,
        max(iculos) as last_iculos,
        max(row_idx) as last_row_idx,
        count(*) as n_hours_recorded
    from hours
    group by patient_id, source_set

)

select
    patient_id,
    source_set,
    is_sepsis_patient,
    first_positive_row_idx,
    first_positive_iculos,
    first_positive_row_idx + 6 as t_sepsis_row_idx,
    first_positive_iculos + 6 as sepsis_onset_iculos,
    coalesce(first_positive_row_idx = 0, false) as onset_left_censored,
    coalesce(first_positive_row_idx + 6 > last_row_idx, false) as onset_after_record_end,
    first_iculos,
    last_iculos,
    last_row_idx,
    n_hours_recorded,
    last_iculos as icu_los_hours
from patients
