-- Capped LOCF never carries a value beyond the seed's locf_max_hours, and _last is NULL
-- exactly when there is no observation within the cap. Returns violating (patient, hour, var).
with caps as (
    select variable, locf_max_hours from {{ ref('variable_catalog') }}
),

checks as (
    {%- for v in clinical_variable_names() %}
    select
        patient_id,
        iculos,
        '{{ v }}' as variable,
        {{ v }}_last as last_value,
        {{ v }}_hours_since as hours_since
    from {{ ref('int_patient_hour_imputed') }}
    {% if not loop.last %}union all{% endif %}
    {%- endfor %}
)

select checks.*, caps.locf_max_hours
from checks
inner join caps on checks.variable = caps.variable
where (checks.last_value is not null and checks.hours_since > caps.locf_max_hours)
    or (checks.last_value is null and checks.hours_since <= caps.locf_max_hours)
