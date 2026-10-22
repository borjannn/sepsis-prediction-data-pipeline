{#-
    The Data Mining mart: one row per patient-hour, with the column-group contract of
    docs/PROJECT.md §9:
      keys      patient_id, iculos
      meta_*    hospital, ICU unit, split assignments (not features by default)
      feat_*    causal features: static demographics, every imputed variable (observed value,
                _measured, _hours_since, _last) and every derived feature
      label_*   targets; NEVER model inputs
    No outcome_* columns. A view (not a ~3 GB copy): it is materialised once, as Parquet, by
    `sepsis export`. Feature columns are expanded with dbt_utils.star at run time.
-#}
{{ config(materialized='view') }}

{%- set imputed_non_features = ['patient_id', 'iculos', 'source_set', 'row_idx', 'age', 'gender',
                                'unit_code', 'hosp_adm_time', 'static_attr_conflict'] %}

select
    imputed.patient_id,
    imputed.iculos,
    imputed.source_set as meta_hospital,
    imputed.unit_code as meta_icu_unit,
    split.split_random as meta_split_random,
    split.cv_fold as meta_cv_fold,
    split.split_hospital_holdout as meta_split_hospital_holdout,
    imputed.iculos as feat_iculos,
    imputed.age as feat_age,
    imputed.gender as feat_gender,
    imputed.hosp_adm_time as feat_hosp_adm_time,
    {{ dbt_utils.star(from=ref('int_patient_hour_imputed'), relation_alias='imputed',
                      except=imputed_non_features, prefix='feat_') }},
    {{ dbt_utils.star(from=ref('int_patient_hour_features'), relation_alias='features',
                      except=['patient_id', 'iculos'], prefix='feat_') }},
    labels.label_sepsis,
    labels.label_is_sepsis_patient,
    labels.label_hours_to_onset,
    labels.label_utility_if_positive,
    labels.label_utility_if_negative
from {{ ref('int_patient_hour_imputed') }} as imputed
inner join {{ ref('int_patient_hour_features') }} as features
    on imputed.patient_id = features.patient_id and imputed.iculos = features.iculos
inner join {{ ref('int_patient_hour_labels') }} as labels
    on imputed.patient_id = labels.patient_id and imputed.iculos = labels.iculos
inner join {{ ref('patient_split') }} as split
    on imputed.patient_id = split.patient_id
