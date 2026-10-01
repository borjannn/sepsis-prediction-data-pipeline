{#-
    The wide, clean patient-hour table: one row per (patient_id, iculos), every hour present
    (hour spine from staging), each clinical variable holding its in-range value or NULL.

    Built by pivoting int_measurement_long, so it gets exactly the same unit harmonisation and
    range rules as the long table. Static attributes come from the patient's first row.

    Leakage contract (§4.2): this table must NOT contain sepsis_label or any label/outcome
    column; it is the base of every feature model. tests/assert_feature_models_have_no_labels.sql
-#}
{{ config(
    indexes=[{'columns': ['patient_id', 'iculos'], 'unique': true}],
) }}

with spine as (

    select patient_id, source_set, iculos, row_idx
    from {{ ref('stg_patient_hour') }}

),

patient_static as (

    select
        patient_id,
        (array_agg(age order by row_idx))[1] as age,
        (array_agg(gender order by row_idx))[1] as gender,
        (array_agg(unit1 order by row_idx))[1] as unit1,
        (array_agg(unit2 order by row_idx))[1] as unit2,
        (array_agg(hosp_adm_time order by row_idx))[1] as hosp_adm_time,
        (
            count(distinct age) > 1
            or count(distinct gender) > 1
            or count(distinct unit1) > 1
            or count(distinct unit2) > 1
            or count(distinct hosp_adm_time) > 1
        ) as static_attr_conflict
    from {{ ref('stg_patient_hour') }}
    group by patient_id

),

observations as (

    select
        patient_id,
        iculos,
        {%- for v in clinical_variable_names() %}
        max(value) filter (
            where variable = '{{ v }}' and not is_out_of_range and not is_unit_mismatch
        ) as {{ v }}
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ ref('int_measurement_long') }}
    group by patient_id, iculos

)

select
    spine.patient_id,
    spine.source_set,
    spine.iculos,
    spine.row_idx,
    patient_static.age,
    patient_static.gender,
    patient_static.unit1,
    patient_static.unit2,
    case
        when patient_static.unit1 = 1 and patient_static.unit2 = 1 then 'BOTH'
        when patient_static.unit1 = 1 then 'MICU'
        when patient_static.unit2 = 1 then 'SICU'
        else 'UNKNOWN'
    end as unit_code,
    patient_static.hosp_adm_time,
    patient_static.static_attr_conflict,
    {%- for v in clinical_variable_names() %}
    observations.{{ v }}{% if not loop.last %},{% endif %}
    {%- endfor %}
from spine
inner join patient_static
    on spine.patient_id = patient_static.patient_id
left join observations
    on spine.patient_id = observations.patient_id
    and spine.iculos = observations.iculos
