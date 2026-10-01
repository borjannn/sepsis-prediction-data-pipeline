{#-
    Derived, causal features per patient-hour (keys + derived columns only; the imputed values
    stay in int_patient_hour_imputed and are joined in the mart).

    Every feature uses the current and past hours of the same patient only, plus constants from
    the seed. Windows:
      w6 / w24     RANGE frames on iculos (last 6 / 24 hours), robust to gaps
      w_prior24    the 24 hours before the current one
      w_running    all hours up to and including the current one
      w_order      lag() over rows; hours are contiguous (docs/data_profile.md), so 1 row = 1 h
    Rolling statistics use OBSERVED (clean, non-imputed) values; scores use LOCF'd values.

    Clinical definitions:
      SIRS (Bone et al. 1992): temp > 38 or < 36; hr > 90; resp > 20 or PaCO2 < 32;
        WBC > 12 or < 4 (x10^3/uL).
      qSOFA (Seymour et al. 2016), partial: resp >= 22, SBP <= 100 (mentation not recorded).
      SOFA (Vincent et al. 1996), partial: coagulation (platelets), liver (bilirubin),
        cardiovascular (MAP < 70 only; no vasopressor data), renal (creatinine), respiratory via
        the SpO2/FiO2 ratio (Pandharipande et al. 2009 thresholds 512/357/214/89), since PaO2
        is not in the data.

    Lineage contract: parents are int_patient_hour_imputed and the seed only.
-#}
{{ config(
    tags=['feature'],
    indexes=[{'columns': ['patient_id', 'iculos'], 'unique': true}],
) }}

{%- set variables = clinical_variable_names() %}
{%- set vitals = clinical_variable_names('vital') %}
{%- set labs = clinical_variable_names('lab') %}
{%- set rolling_vitals = ['hr', 'o2sat', 'temp', 'sbp', 'map', 'resp', 'dbp'] %}
{%- set sirs = ['sirs_temp', 'sirs_hr', 'sirs_resp', 'sirs_wbc'] %}
{%- set qsofa = ['qsofa_resp', 'qsofa_sbp'] %}
{%- set sofa = ['sofa_coagulation', 'sofa_liver', 'sofa_cardiovascular', 'sofa_renal',
                'sofa_respiratory'] %}

with imputed as (

    select * from {{ ref('int_patient_hour_imputed') }}

),

normals as (

    select
        {%- for v in variables %}
        max(normal_low) filter (where variable = '{{ v }}') as {{ v }}_lo,
        max(normal_high) filter (where variable = '{{ v }}') as {{ v }}_hi
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ ref('variable_catalog') }}

),

base as (

    select
        imputed.*,
        imputed.hr_last / nullif(imputed.sbp_last, 0) as shock_index,
        imputed.sbp_last - imputed.dbp_last as pulse_pressure,
        coalesce(imputed.map_last, (imputed.sbp_last + 2 * imputed.dbp_last) / 3)
            as map_or_derived,
        (
            imputed.map_last is null
            and imputed.sbp_last is not null
            and imputed.dbp_last is not null
        ) as map_is_derived,
        imputed.bun_last / nullif(imputed.creatinine_last, 0) as bun_creatinine_ratio,
        imputed.o2sat_last / nullif(imputed.fio2_last, 0) as spo2_fio2_ratio,
        (
            {%- for v in vitals %}
            imputed.{{ v }}_measured::int{% if not loop.last %} +{% endif %}
            {%- endfor %}
        ) as n_vitals_measured,
        (
            {%- for v in labs %}
            imputed.{{ v }}_measured::int{% if not loop.last %} +{% endif %}
            {%- endfor %}
        ) as n_labs_measured,
        (
            {%- for v in variables %}
            coalesce((imputed.{{ v }} < normals.{{ v }}_lo
                or imputed.{{ v }} > normals.{{ v }}_hi)::int, 0)
            {%- if not loop.last %} +{% endif %}
            {%- endfor %}
        ) as n_abnormal
    from imputed
    cross join normals

),

scored as (

    select
        base.*,
        (temp_last > 38 or temp_last < 36) as sirs_temp,
        (hr_last > 90) as sirs_hr,
        (resp_last > 20 or paco2_last < 32) as sirs_resp,
        (wbc_last > 12 or wbc_last < 4) as sirs_wbc,
        (resp_last >= 22) as qsofa_resp,
        (sbp_last <= 100) as qsofa_sbp,
        case
            when platelets_last < 20 then 4
            when platelets_last < 50 then 3
            when platelets_last < 100 then 2
            when platelets_last < 150 then 1
            when platelets_last is not null then 0
        end as sofa_coagulation,
        case
            when bilirubin_total_last >= 12 then 4
            when bilirubin_total_last >= 6 then 3
            when bilirubin_total_last >= 2 then 2
            when bilirubin_total_last >= 1.2 then 1
            when bilirubin_total_last is not null then 0
        end as sofa_liver,
        case
            when map_or_derived < 70 then 1
            when map_or_derived is not null then 0
        end as sofa_cardiovascular,
        case
            when creatinine_last >= 5 then 4
            when creatinine_last >= 3.5 then 3
            when creatinine_last >= 2 then 2
            when creatinine_last >= 1.2 then 1
            when creatinine_last is not null then 0
        end as sofa_renal,
        case
            when spo2_fio2_ratio < 89 then 4
            when spo2_fio2_ratio < 214 then 3
            when spo2_fio2_ratio < 357 then 2
            when spo2_fio2_ratio < 512 then 1
            when spo2_fio2_ratio is not null then 0
        end as sofa_respiratory
    from base

),

summed as (

    select
        scored.*,
        ({% for c in sirs %}coalesce({{ c }}::int, 0){% if not loop.last %} + {% endif %}{% endfor %})
            as sirs_count,
        ({% for c in sirs %}({{ c }} is not null)::int{% if not loop.last %} + {% endif %}{% endfor %})
            as sirs_components_available,
        ({% for c in qsofa %}coalesce({{ c }}::int, 0){% if not loop.last %} + {% endif %}{% endfor %})
            as qsofa_partial,
        ({% for c in qsofa %}({{ c }} is not null)::int{% if not loop.last %} + {% endif %}{% endfor %})
            as qsofa_components_available,
        ({% for c in sofa %}coalesce({{ c }}, 0){% if not loop.last %} + {% endif %}{% endfor %})
            as sofa_partial,
        ({% for c in sofa %}({{ c }} is not null)::int{% if not loop.last %} + {% endif %}{% endfor %})
            as sofa_components_available
    from scored

)

select
    patient_id,
    iculos,
    shock_index,
    pulse_pressure,
    map_or_derived,
    map_is_derived,
    bun_creatinine_ratio,
    spo2_fio2_ratio,
    n_vitals_measured,
    n_labs_measured,
    n_abnormal,
    {%- for c in sirs + qsofa + sofa %}
    {{ c }},
    {%- endfor %}
    sirs_count,
    sirs_components_available,
    qsofa_partial,
    qsofa_components_available,
    sofa_partial,
    sofa_components_available,
    sofa_partial - min(sofa_partial) over w_prior24 as delta_sofa_partial_24h,
    {%- for v in rolling_vitals %}
    avg({{ v }}) over w6 as {{ v }}_mean_6h,
    min({{ v }}) over w6 as {{ v }}_min_6h,
    max({{ v }}) over w6 as {{ v }}_max_6h,
    stddev_samp({{ v }}) over w6 as {{ v }}_std_6h,
    count({{ v }}) over w6 as {{ v }}_count_6h,
    avg({{ v }}) over w24 as {{ v }}_mean_24h,
    min({{ v }}) over w24 as {{ v }}_min_24h,
    max({{ v }}) over w24 as {{ v }}_max_24h,
    stddev_samp({{ v }}) over w24 as {{ v }}_std_24h,
    count({{ v }}) over w24 as {{ v }}_count_24h,
    regr_slope({{ v }}, iculos) over w24 as {{ v }}_slope_24h,
    {{ v }}_last - lag({{ v }}_last, 1) over w_order as {{ v }}_delta_1h,
    {{ v }}_last - lag({{ v }}_last, 6) over w_order as {{ v }}_delta_6h,
    {%- endfor %}
    sum((n_labs_measured > 0)::int) over w24 as lab_draw_hours_24h,
    iculos - max(case when n_labs_measured > 0 then iculos end) over w_running
        as hours_since_any_lab,
    sum(n_abnormal) over w_running as n_abnormal_cumulative,
    max(lactate) over w_running as lactate_max_so_far
from summed
window
    w_order as (partition by patient_id order by iculos),
    w_running as (
        partition by patient_id order by iculos
        rows between unbounded preceding and current row
    ),
    w6 as (
        partition by patient_id order by iculos
        range between 5 preceding and current row
    ),
    w24 as (
        partition by patient_id order by iculos
        range between 23 preceding and current row
    ),
    w_prior24 as (
        partition by patient_id order by iculos
        range between 24 preceding and 1 preceding
    )
