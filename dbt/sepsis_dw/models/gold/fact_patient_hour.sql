{#-
    Periodic snapshot fact: one row per patient per ICU hour (grain patient x stay hour).
    Measures: the 34 cleaned values (NULL where not measured or out of range), measurement
    counts, SIRS / partial SOFA scores, and the label columns for analysis.
-#}
{{ config(
    indexes=[
        {'columns': ['patient_key', 'stay_hour_key'], 'unique': true},
        {'columns': ['stay_hour_key']},
        {'columns': ['hospital_key']},
        {'columns': ['unit_key']},
    ],
    post_hook=[
        "{{ foreign_key('patient_key', ref('dim_patient'), 'patient_key') }}",
        "{{ foreign_key('hospital_key', ref('dim_hospital'), 'hospital_key') }}",
        "{{ foreign_key('unit_key', ref('dim_icu_unit'), 'unit_key') }}",
        "{{ foreign_key('stay_hour_key', ref('dim_stay_hour'), 'stay_hour_key') }}",
    ],
) }}

select
    patients.patient_key,
    hospitals.hospital_key,
    units.unit_key,
    clean.iculos as stay_hour_key,
    {%- for v in clinical_variable_names() %}
    clean.{{ v }},
    {%- endfor %}
    features.n_vitals_measured,
    features.n_labs_measured,
    features.sirs_count,
    features.qsofa_partial,
    features.sofa_partial,
    labels.label_sepsis,
    labels.label_hours_to_onset
from {{ ref('int_patient_hour_clean') }} as clean
inner join {{ ref('int_patient_hour_features') }} as features
    on clean.patient_id = features.patient_id and clean.iculos = features.iculos
inner join {{ ref('int_patient_hour_labels') }} as labels
    on clean.patient_id = labels.patient_id and clean.iculos = labels.iculos
inner join {{ ref('dim_patient') }} as patients
    on clean.patient_id = patients.patient_id
inner join {{ ref('dim_hospital') }} as hospitals
    on clean.source_set = hospitals.hospital_code
inner join {{ ref('dim_icu_unit') }} as units
    on clean.unit_code = units.unit_code
