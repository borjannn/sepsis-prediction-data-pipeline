{#- Hospital dimension: the two public hospital systems of the challenge. -#}
{{ config(post_hook="{{ primary_key('hospital_key') }}") }}

select *
from (
    values
        (1, 'A', 'training_setA', 'Hospital system A (public training set A)'),
        (2, 'B', 'training_setB', 'Hospital system B (public training set B)')
) as hospitals (hospital_key, hospital_code, source_set_folder, description)
