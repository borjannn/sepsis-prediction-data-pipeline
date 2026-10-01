-- Static attributes (age, gender, unit, hosp_adm_time) are constant within a patient.
-- Violations do not break the pipeline (first row wins, static_attr_conflict is set),
-- so this only warns.
{{ config(severity='warn') }}

select patient_id
from {{ ref('int_patient_hour_clean') }}
where static_attr_conflict
group by patient_id
