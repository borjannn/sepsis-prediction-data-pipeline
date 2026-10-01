{#-
    One row per observed clinical value (long / EAV shape), validated against the seed.
    A view: the ~10M rows are stored only once, in gold.fact_measurement.

    Harmonisation rules are data-format facts from the profile (docs/data_profile.md), not
    fitted values:
    - FiO2 recorded as a percentage (1 < value <= 100) is rescaled to a fraction
      (was_rescaled).
    - Calcium below 2 is ionized calcium in mmol/L (normal ~1.1-1.3) recorded under total
      calcium (mg/dL, normal 8.5-10.5): a different analyte that cannot be converted. It is kept
      here but flagged is_unit_mismatch, and excluded from the clean wide table (~11 % of
      calcium values).

    Flags:
    - is_out_of_range: outside the physiological plausibility range (kept and flagged here,
      nulled in the wide clean table). Never set for unit-mismatch rows.
    - is_abnormal: outside the clinical normal range.
-#}
{{ config(materialized='view') }}

with staged as (

    select * from {{ ref('stg_patient_hour') }}

),

unpivoted as (

    select
        staged.patient_id,
        staged.source_set,
        staged.iculos,
        staged.row_idx,
        obs.variable,
        obs.raw_value
    from staged
    cross join lateral (
        values
        {%- for v in clinical_variables() %}
            ('{{ v.name }}', staged.{{ v.name }}){% if not loop.last %},{% endif %}
        {%- endfor %}
    ) as obs (variable, raw_value)
    where obs.raw_value is not null

),

harmonised as (

    select
        unpivoted.*,
        (variable = 'fio2' and raw_value > 1 and raw_value <= 100) as was_rescaled,
        (variable = 'calcium' and raw_value < 2) as is_unit_mismatch
    from unpivoted

),

valued as (

    select
        harmonised.*,
        case when was_rescaled then raw_value / 100 else raw_value end as value
    from harmonised

)

select
    valued.patient_id,
    valued.source_set,
    valued.iculos,
    valued.row_idx,
    valued.variable,
    valued.raw_value,
    valued.value,
    valued.was_rescaled,
    valued.is_unit_mismatch,
    catalog.category,
    (
        not valued.is_unit_mismatch
        and valued.value not between catalog.valid_min and catalog.valid_max
    ) as is_out_of_range,
    (
        not valued.is_unit_mismatch
        and valued.value not between catalog.normal_low and catalog.normal_high
    ) as is_abnormal
from valued
inner join {{ ref('variable_catalog') }} as catalog
    on valued.variable = catalog.variable
