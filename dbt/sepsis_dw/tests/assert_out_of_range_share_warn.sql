-- Variables whose out-of-range share exceeds 1 % (warning; review the seed range).
{{ config(severity='warn') }}

select variable, n_obs, out_of_range_share
from ({{ out_of_range_shares() }}) as shares
where out_of_range_share > 0.01
