{#-
    Totals needed to compute the normalised challenge score per evaluation split:
        normalised = (observed - inaction) / (best - inaction)
    as in the official evaluate_sepsis_score.py:
      best      predict 1 for septic rows with t_sepsis - 12 <= row_idx <= t_sepsis + 3
                (i.e. -3 <= label_hours_to_onset <= 12), 0 elsewhere
      inaction  predict 0 everywhere
-#}
with rows as (

    select
        labels.patient_id,
        labels.label_utility_if_positive,
        labels.label_utility_if_negative,
        (
            labels.label_is_sepsis_patient
            and labels.label_hours_to_onset between -3 and 12
        ) as best_predicts_positive,
        split.split_random,
        split.split_hospital_holdout,
        split.cv_fold
    from {{ ref('int_patient_hour_labels') }} as labels
    inner join {{ ref('patient_split') }} as split
        on labels.patient_id = split.patient_id

),

assigned as (

    select rows.*, 'random' as split_scheme, split_random as split from rows
    union all
    select rows.*, 'hospital_holdout', split_hospital_holdout from rows
    union all
    select rows.*, 'cv', 'fold_' || cv_fold from rows where cv_fold is not null

)

select
    split_scheme,
    split,
    count(distinct patient_id) as n_patients,
    count(*) as n_rows,
    sum(
        case when best_predicts_positive then label_utility_if_positive
             else label_utility_if_negative end
    ) as best_utility_total,
    sum(label_utility_if_negative) as inaction_utility_total,
    sum(label_utility_if_positive) as all_positive_utility_total
from assigned
group by split_scheme, split
