-- Split proportions are within +-0.5 percentage points of 70/15/15 in every stratum
-- (hospital x septic), and CV folds of train patients differ by at most 1 patient per stratum.
with strata as (
    select
        source_set,
        is_sepsis,
        count(*) as n,
        avg((split_random = 'train')::int) as train_share,
        avg((split_random = 'validation')::int) as validation_share,
        avg((split_random = 'test')::int) as test_share
    from {{ ref('patient_split') }}
    group by source_set, is_sepsis
),

folds as (
    select source_set, is_sepsis, max(n) - min(n) as fold_spread
    from (
        select source_set, is_sepsis, cv_fold, count(*) as n
        from {{ ref('patient_split') }}
        where cv_fold is not null
        group by source_set, is_sepsis, cv_fold
    ) as per_fold
    group by source_set, is_sepsis
)

select strata.*, folds.fold_spread
from strata
inner join folds using (source_set, is_sepsis)
where abs(train_share - 0.70) > 0.005
    or abs(validation_share - 0.15) > 0.005
    or abs(test_share - 0.15) > 0.005
    or folds.fold_spread > 1
