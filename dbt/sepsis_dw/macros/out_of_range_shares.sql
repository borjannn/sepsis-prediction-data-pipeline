{#- Per-variable share of out-of-range observations (used by the out-of-range tests). -#}
{% macro out_of_range_shares() %}
    select
        variable,
        count(*) as n_obs,
        avg(is_out_of_range::int)::float8 as out_of_range_share
    from {{ ref('int_measurement_long') }}
    group by variable
{% endmacro %}
