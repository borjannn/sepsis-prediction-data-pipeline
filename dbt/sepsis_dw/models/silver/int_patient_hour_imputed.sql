{#-
    Capped last-observation-carried-forward (LOCF), computed on the CLEAN values.
    For each of the 34 variables:
      <v>_measured    observed in this hour
      <v>_hours_since hours since the last observation (uncapped; NULL if never observed)
      <v>_last        last observed value if hours_since <= locf_max_hours (seed), else NULL

    One shared window for all 34 variables (a single sort): the max of the array
    [iculos, value] over a running frame is the latest observation, because arrays compare
    element by element and iculos is unique per patient. Only past and current hours are used.

    No population-level imputation here (means/medians must be fitted on the train split by
    the Data Mining team, using mart.train_reference_stats).

    Lineage contract: parents are int_patient_hour_clean and the seed only.
-#}
{{ config(
    tags=['feature'],
    indexes=[{'columns': ['patient_id', 'iculos'], 'unique': true}],
) }}

{%- set variables = clinical_variable_names() %}

with clean as (

    select * from {{ ref('int_patient_hour_clean') }}

),

caps as (

    select
        {%- for v in variables %}
        max(locf_max_hours) filter (where variable = '{{ v }}') as {{ v }}_cap
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ ref('variable_catalog') }}

),

observed as (

    select
        clean.*,
        {%- for v in variables %}
        max(array[iculos::float8, {{ v }}]) filter (where {{ v }} is not null) over w
            as {{ v }}_obs
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
    from clean
    window w as (
        partition by patient_id order by iculos
        rows between unbounded preceding and current row
    )

)

select
    observed.patient_id,
    observed.source_set,
    observed.iculos,
    observed.row_idx,
    observed.age,
    observed.gender,
    observed.unit_code,
    observed.hosp_adm_time,
    observed.static_attr_conflict,
    {%- for v in variables %}
    observed.{{ v }},
    (observed.{{ v }} is not null) as {{ v }}_measured,
    (observed.iculos - observed.{{ v }}_obs[1])::integer as {{ v }}_hours_since,
    case
        when observed.iculos - observed.{{ v }}_obs[1] <= caps.{{ v }}_cap
            then observed.{{ v }}_obs[2]
    end as {{ v }}_last
    {%- if not loop.last %},{% endif %}
    {%- endfor %}
from observed
cross join caps
