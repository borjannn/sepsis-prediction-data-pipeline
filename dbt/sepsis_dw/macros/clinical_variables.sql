{#-
    The 34 clinical variables: warehouse name, source column, category.
    Static on purpose, so wide models can be generated at parse time without a DB round-trip.
    All numbers (valid/normal ranges, LOCF caps) live in seeds/variable_catalog.csv and are
    applied through SQL joins; tests/assert_variable_macro_matches_seed.sql keeps both in sync.
-#}
{% macro clinical_variables(category=none) %}
    {%- set variables = [
        {'name': 'hr',               'source': 'HR',               'category': 'vital'},
        {'name': 'o2sat',            'source': 'O2Sat',            'category': 'vital'},
        {'name': 'temp',             'source': 'Temp',             'category': 'vital'},
        {'name': 'sbp',              'source': 'SBP',              'category': 'vital'},
        {'name': 'map',              'source': 'MAP',              'category': 'vital'},
        {'name': 'dbp',              'source': 'DBP',              'category': 'vital'},
        {'name': 'resp',             'source': 'Resp',             'category': 'vital'},
        {'name': 'etco2',            'source': 'EtCO2',            'category': 'vital'},
        {'name': 'base_excess',      'source': 'BaseExcess',       'category': 'lab'},
        {'name': 'hco3',             'source': 'HCO3',             'category': 'lab'},
        {'name': 'fio2',             'source': 'FiO2',             'category': 'lab'},
        {'name': 'ph',               'source': 'pH',               'category': 'lab'},
        {'name': 'paco2',            'source': 'PaCO2',            'category': 'lab'},
        {'name': 'sao2',             'source': 'SaO2',             'category': 'lab'},
        {'name': 'ast',              'source': 'AST',              'category': 'lab'},
        {'name': 'bun',              'source': 'BUN',              'category': 'lab'},
        {'name': 'alkalinephos',     'source': 'Alkalinephos',     'category': 'lab'},
        {'name': 'calcium',          'source': 'Calcium',          'category': 'lab'},
        {'name': 'chloride',         'source': 'Chloride',         'category': 'lab'},
        {'name': 'creatinine',       'source': 'Creatinine',       'category': 'lab'},
        {'name': 'bilirubin_direct', 'source': 'Bilirubin_direct', 'category': 'lab'},
        {'name': 'glucose',          'source': 'Glucose',          'category': 'lab'},
        {'name': 'lactate',          'source': 'Lactate',          'category': 'lab'},
        {'name': 'magnesium',        'source': 'Magnesium',        'category': 'lab'},
        {'name': 'phosphate',        'source': 'Phosphate',        'category': 'lab'},
        {'name': 'potassium',        'source': 'Potassium',        'category': 'lab'},
        {'name': 'bilirubin_total',  'source': 'Bilirubin_total',  'category': 'lab'},
        {'name': 'troponin_i',       'source': 'TroponinI',        'category': 'lab'},
        {'name': 'hct',              'source': 'Hct',              'category': 'lab'},
        {'name': 'hgb',              'source': 'Hgb',              'category': 'lab'},
        {'name': 'ptt',              'source': 'PTT',              'category': 'lab'},
        {'name': 'wbc',              'source': 'WBC',              'category': 'lab'},
        {'name': 'fibrinogen',       'source': 'Fibrinogen',       'category': 'lab'},
        {'name': 'platelets',        'source': 'Platelets',        'category': 'lab'},
    ] -%}
    {%- if category is none -%}
        {{ return(variables) }}
    {%- else -%}
        {{ return(variables | selectattr('category', 'equalto', category) | list) }}
    {%- endif -%}
{% endmacro %}

{% macro clinical_variable_names(category=none) %}
    {{ return(clinical_variables(category) | map(attribute='name') | list) }}
{% endmacro %}
