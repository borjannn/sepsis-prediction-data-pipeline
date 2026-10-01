{#-
    Which patients may contribute to the reference statistics of each split scheme: only the
    scheme's TRAINING patients.
      random            split_random = 'train'
      hospital_holdout  split_hospital_holdout = 'train' (hospital A training part)
      cv_fold_k         train patients outside fold k (k = 1..5), so cross-validation does not
                        leak through scaling or imputation
-#}
select patient_id, 'random' as split_scheme
from {{ ref('patient_split') }}
where split_random = 'train'

union all

select patient_id, 'hospital_holdout' as split_scheme
from {{ ref('patient_split') }}
where split_hospital_holdout = 'train'

{% for k in range(1, 6) %}
union all

select patient_id, 'cv_fold_{{ k }}' as split_scheme
from {{ ref('patient_split') }}
where split_random = 'train' and cv_fold <> {{ k }}
{% endfor %}
