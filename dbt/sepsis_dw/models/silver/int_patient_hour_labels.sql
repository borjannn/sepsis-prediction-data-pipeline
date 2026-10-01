{#-
    Targets per patient-hour (LABEL data: never a model input).
      label_sepsis              official SepsisLabel (already 6 h before t_sepsis)
      label_is_sepsis_patient   whole-stay outcome
      label_hours_to_onset      t_sepsis_row_idx - row_idx (NULL for non-septic patients);
                                lets the Data Mining team build other horizons
      label_utility_if_*        official challenge reward for predicting positive / negative
-#}
{{ config(
    tags=['label'],
    indexes=[{'columns': ['patient_id', 'iculos'], 'unique': true}],
) }}

select
    hours.patient_id,
    hours.iculos,
    hours.row_idx,
    hours.sepsis_label as label_sepsis,
    patients.is_sepsis_patient as label_is_sepsis_patient,
    case
        when patients.is_sepsis_patient then patients.t_sepsis_row_idx - hours.row_idx
    end as label_hours_to_onset,
    {{ sepsis_utility('hours.row_idx', 'patients.t_sepsis_row_idx',
                      'patients.is_sepsis_patient', true) }} as label_utility_if_positive,
    {{ sepsis_utility('hours.row_idx', 'patients.t_sepsis_row_idx',
                      'patients.is_sepsis_patient', false) }} as label_utility_if_negative
from {{ ref('stg_patient_hour') }} as hours
inner join {{ ref('int_patient_sepsis') }} as patients
    on hours.patient_id = patients.patient_id
