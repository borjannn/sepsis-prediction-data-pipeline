{#-
    Constraint post-hooks for the star schema (documentation + integrity in the database).

    Primary/unique keys are backed by indexes, and dbt rebuilds a table by renaming the old copy
    to __dbt_backup before dropping it; Postgres does not rename the old copy's indexes, so a
    fixed constraint name would collide on every rebuild. Their names therefore carry the first
    8 characters of the dbt invocation id. Foreign keys are not indexes and keep stable names.

    dbt drops replaced tables with CASCADE, so rebuilding a dimension alone removes the
    foreign keys that point at it: FKs are guaranteed only after a full `dbt build`
    (relationships tests guarantee integrity regardless).
-#}
{% macro primary_key(columns) -%}
    alter table {{ this }}
        add constraint {{ this.identifier }}_pk_{{ invocation_id[:8] }} primary key ({{ columns }})
{%- endmacro %}

{% macro unique_key(columns) -%}
    alter table {{ this }}
        add constraint {{ this.identifier }}_uq_{{ invocation_id[:8] }} unique ({{ columns }})
{%- endmacro %}

{% macro foreign_key(column, to_relation, to_column) -%}
    alter table {{ this }}
        add constraint {{ this.identifier }}_{{ column }}_fk
        foreign key ({{ column }}) references {{ to_relation }} ({{ to_column }})
{%- endmacro %}
