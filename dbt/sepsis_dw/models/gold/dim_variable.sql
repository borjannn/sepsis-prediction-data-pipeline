{#- Clinical variable dimension (from the seed): one member per measured variable. -#}
{{ config(post_hook="{{ primary_key('variable_key') }}") }}

select
    (row_number() over (order by case category when 'vital' then 0 else 1 end, variable))::smallint
        as variable_key,
    variable,
    source_column,
    display_name,
    category,
    subcategory,
    unit,
    valid_min,
    valid_max,
    normal_low,
    normal_high,
    locf_max_hours
from {{ ref('variable_catalog') }}
