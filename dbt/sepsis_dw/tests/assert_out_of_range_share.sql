-- Variables whose out-of-range share exceeds 5 %: an error, because such a share means the
-- seed's plausibility range (or the unit) is wrong, not the data.
-- The 1 % warning level is assert_out_of_range_share_warn.sql.
select variable, n_obs, out_of_range_share
from ({{ out_of_range_shares() }}) as shares
where out_of_range_share > 0.05
