-- Leakage contract (docs/PROJECT.md §9): the feature models must not carry the target or any
-- label/outcome column. Returns every offending column.
select table_name, column_name
from information_schema.columns
where table_schema = '{{ ref("int_patient_hour_clean").schema }}'
    and table_name in (
        '{{ ref("int_patient_hour_clean").identifier }}',
        '{{ ref("int_patient_hour_imputed").identifier }}',
        '{{ ref("int_patient_hour_features").identifier }}'
    )
    and (
        column_name = 'sepsis_label'
        or column_name like 'label\_%'
        or column_name like 'outcome\_%'
        or column_name in ('is_sepsis', 'is_sepsis_patient', 't_sepsis_row_idx')
    )
