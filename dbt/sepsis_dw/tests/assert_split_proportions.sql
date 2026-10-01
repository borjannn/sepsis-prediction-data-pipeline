-- Split sizes are within one patient of 70/15/15 in every stratum (hospital x septic), and CV
-- folds of train patients differ by at most one patient per stratum. Count-based, so it holds
-- for samples too; on the full data (smallest stratum 1,142 patients) one patient is < 0.1 %,
-- well inside the plan's +-0.5 % target.
with strata as (
    select
        source_set,
        is_sepsis,
        count(*) as n,
        count(*) filter (where split_random = 'train') as n_train,
        count(*) filter (where split_random = 'validation') as n_validation,
        count(*) filter (where split_random = 'test') as n_test
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
left join folds using (source_set, is_sepsis)
where abs(n_train - 0.70 * n) > 1
    or abs(n_validation - 0.15 * n) > 1
    or abs(n_test - 0.15 * n) > 1
    or coalesce(folds.fold_spread, 0) > 1
