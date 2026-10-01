{#- Every observation excluded from the clean wide table, with the rule. Nothing is deleted. -#}
{{ config(materialized='view') }}

select
    measurement.patient_id,
    measurement.source_set,
    measurement.iculos,
    measurement.variable,
    measurement.raw_value,
    measurement.value,
    catalog.valid_min,
    catalog.valid_max,
    case
        when measurement.is_unit_mismatch then 'ionized calcium (mmol/L) recorded as total calcium'
        else 'outside physiological plausibility range'
    end as rule
from {{ ref('int_measurement_long') }} as measurement
inner join {{ ref('variable_catalog') }} as catalog
    on measurement.variable = catalog.variable
where measurement.is_out_of_range or measurement.is_unit_mismatch
