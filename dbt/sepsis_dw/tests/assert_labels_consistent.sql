-- Label columns agree with each other: a positive hour belongs to a septic patient,
-- label_hours_to_onset exists exactly for septic patients, and every hour from 6 h before
-- t_sepsis onwards is positive (the official labelling rule). Returns violating rows.
select patient_id, iculos, label_sepsis, label_is_sepsis_patient, label_hours_to_onset
from {{ ref('int_patient_hour_labels') }}
where (label_sepsis = 1 and not label_is_sepsis_patient)
    or (label_is_sepsis_patient and label_hours_to_onset is null)
    or (not label_is_sepsis_patient and label_hours_to_onset is not null)
    or (label_hours_to_onset <= 6 and label_sepsis = 0)
    or (label_hours_to_onset > 6 and label_sepsis = 1)
