-- Every column of the ML mart belongs to exactly one group of the leakage contract
-- (keys, meta_*, feat_*, label_*), and no outcome_* column is exposed.
select column_name
from information_schema.columns
where table_schema = '{{ ref("ml_features_patient_hour").schema }}'
    and table_name = '{{ ref("ml_features_patient_hour").identifier }}'
    and not (
        column_name in ('patient_id', 'iculos')
        or column_name like 'meta\_%'
        or column_name like 'feat\_%'
        or column_name like 'label\_%'
    )
