{#-
    1:1 with raw.patient_hour: snake_case names, integer/smallint casts, and row_idx
    (0-based row index per patient, the index the official challenge scoring uses).

    The leakage_cutoff / leakage_patients vars are set only by the truncation leakage test:
    they drop hours > cutoff for those patients, to prove features never look ahead.
-#}
{%- set cutoff = var('leakage_cutoff') -%}
{%- set leakage_patients = var('leakage_patients', []) -%}

with source as (

    select * from {{ source('raw', 'patient_hour') }}

),

renamed as (

    select
        patient_id,
        source_set,
        {%- for v in clinical_variables() %}
        {{ adapter.quote(v.source) }} as {{ v.name }},
        {%- endfor %}
        "Age" as age,
        "Gender"::smallint as gender,
        "Unit1"::smallint as unit1,
        "Unit2"::smallint as unit2,
        "HospAdmTime" as hosp_adm_time,
        "ICULOS"::integer as iculos,
        "SepsisLabel"::smallint as sepsis_label,
        source_file,
        source_row_number,
        load_run_id
    from source
    {%- if cutoff is not none and leakage_patients | length > 0 %}
    where not (
        patient_id in (
            {%- for p in leakage_patients %}'{{ p }}'{% if not loop.last %}, {% endif %}{% endfor -%}
        )
        and "ICULOS" > {{ cutoff }}
    )
    {%- endif %}

)

select
    *,
    (row_number() over (partition by patient_id order by iculos, source_row_number) - 1)::integer
        as row_idx
from renamed
