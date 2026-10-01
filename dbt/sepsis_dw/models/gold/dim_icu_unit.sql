{#- ICU unit dimension, from the Unit1 (MICU) / Unit2 (SICU) flags. -#}
{{ config(post_hook="{{ primary_key('unit_key') }}") }}

select
    unit_key::smallint as unit_key,
    unit_code,
    description
from {{ ref('icu_unit') }}
