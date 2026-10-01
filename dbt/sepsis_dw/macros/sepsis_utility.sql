{#-
    Per-row utility of the PhysioNet/CinC 2019 challenge, ported from compute_prediction_utility
    in the official evaluate_sepsis_score.py (physionetchallenges/evaluation-2019):

        dt_early = -12, dt_optimal = -6, dt_late = 3, max_u_tp = 1, min_u_fn = -2,
        u_fp = -0.05, u_tn = 0;  t_sepsis = argmax(labels) - dt_optimal  (a ROW index)

    With d = row_idx - t_sepsis_row_idx:
      predict positive, septic:     d <= -6: max(d/6 + 2, -0.05);  -6 < d <= 3: -d/9 + 1/3;  else 0
      predict negative, septic:     d <= -6: 0;                    -6 < d <= 3: -2d/9 - 4/3; else 0
      non-septic patient:           positive -0.05, negative 0
    tests/test_utility.py checks this against a verbatim port of the official function.
-#}
{% macro sepsis_utility(row_idx, t_sepsis_row_idx, is_septic, predict_positive) %}
    {%- set d = '(' ~ row_idx ~ ' - ' ~ t_sepsis_row_idx ~ ')::float8' -%}
    {%- if predict_positive -%}
    case
        when not {{ is_septic }} then -0.05
        when {{ d }} <= -6 then greatest({{ d }} / 6.0 + 2.0, -0.05)
        when {{ d }} <= 3 then -{{ d }} / 9.0 + 1.0 / 3.0
        else 0.0
    end
    {%- else -%}
    case
        when not {{ is_septic }} then 0.0
        when {{ d }} <= -6 then 0.0
        when {{ d }} <= 3 then -2.0 * {{ d }} / 9.0 - 4.0 / 3.0
        else 0.0
    end
    {%- endif -%}
{% endmacro %}
