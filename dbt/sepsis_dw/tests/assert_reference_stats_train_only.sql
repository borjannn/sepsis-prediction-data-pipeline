-- Reference statistics may only use each scheme's training patients: the membership model
-- (read by `sepsis export` when it computes mart.train_reference_stats) never contains a
-- held-out patient. The statistics themselves are tested in tests/test_export_and_timescale.py.
select membership.split_scheme, membership.patient_id
from {{ ref('split_scheme_membership') }} as membership
inner join {{ ref('patient_split') }} as split on membership.patient_id = split.patient_id
where (membership.split_scheme = 'random' and split.split_random <> 'train')
    or (membership.split_scheme = 'hospital_holdout'
        and (split.source_set <> 'A' or split.split_hospital_holdout <> 'train'))
    or (membership.split_scheme like 'cv_fold_%'
        and (split.split_random <> 'train'
             or split.cv_fold = replace(membership.split_scheme, 'cv_fold_', '')::int))
