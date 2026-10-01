-- The static clinical_variables() macro and seeds/variable_catalog.csv must list the same
-- variables with the same category. Returns every mismatch.
with macro_list as (
    select * from (values
    {%- for v in clinical_variables() %}
        ('{{ v.name }}', '{{ v.category }}'){% if not loop.last %},{% endif %}
    {%- endfor %}
    ) as m (variable, category)
),

seed_list as (
    select variable, category from {{ ref('variable_catalog') }}
)

select
    coalesce(macro_list.variable, seed_list.variable) as variable,
    macro_list.category as macro_category,
    seed_list.category as seed_category
from macro_list
full outer join seed_list on macro_list.variable = seed_list.variable
where macro_list.variable is null
    or seed_list.variable is null
    or macro_list.category <> seed_list.category
