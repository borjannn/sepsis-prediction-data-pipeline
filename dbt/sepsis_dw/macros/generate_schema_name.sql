{#-
    Use custom schema names verbatim (e.g. `silver`) instead of dbt's default
    `<target_schema>_<custom_schema>`, so models land in the layer schemas
    created by sql/init/10_schemas.sql.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
