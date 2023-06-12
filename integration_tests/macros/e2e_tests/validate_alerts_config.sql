{% macro validate_alerts_config() %}
    {% set dbt_models_relation = ref('dbt_models') %}
    {% set dbt_models_query %}
        select distinct name, {{ elementary.get_json_path('meta','alerts_config') }} as alerts_config
        from {{ dbt_models_relation }}
    {% endset %}
    {%- set results_agate = elementary.run_query(dbt_models_query) %}
    {%- set dbt_models_dict = elementary.agate_to_dicts(results_agate) %}

    {% set dbt_tests_relation = ref('dbt_tests') %}
    {% set dbt_tests_query %}
        select distinct name, {{ elementary.get_json_path('meta','alerts_config') }} as alerts_config
        from {{ dbt_tests_relation }}
    {% endset %}
    {%- set results_agate = elementary.run_query(dbt_tests_query) %}
    {%- set dbt_tests_dict = elementary.agate_to_dicts(results_agate) %}

    {%- set expected_alerts_config = {
        'one': '{"alert_fields":null,"alert_suppression_interval":12,"channel":"model_channel","slack_group_alerts_by":null,"subscribers":["@idan"]}',
        'accepted_values_one_one__2__3': '{"alert_fields":null,"alert_suppression_interval":12,"channel":"test_channel","slack_group_alerts_by":null,"subscribers":["@idan","@or"]}',
        'elementary_volume_anomalies_any_type_column_anomalies_hour__4': '{"alert_fields":null,"alert_suppression_interval":24,"channel":"test_channel","slack_group_alerts_by":null,"subscribers":["@egk","@or","@idan"]}',
        'any_type_column_anomalies': '{"alert_fields":null,"alert_suppression_interval":24,"channel":null,"slack_group_alerts_by":null,"subscribers":["@egk"]}'
    }
    %}

    {%- for row_dict in dbt_models_dict %}
        {%- set name = elementary.insensitive_get_dict_value(row_dict, 'name', none) %}
        {%- if name in expected_alerts_config.keys() %}
            {%- set row_dict = row_dict.get('alerts_config') | replace(' ','') %}
            {%- do assert_value(row_dict, expected_alerts_config.get(name) ) -%}
        {%- endif %}
    {%- endfor %}

    {%- for row_dict in dbt_tests_dict %}
        {%- set name = elementary.insensitive_get_dict_value(row_dict, 'name', none) %}
        {%- if name in expected_alerts_config.keys() %}
            {%- set row_dict = row_dict.get('alerts_config') | replace(' ','') %}
            {%- do assert_value(row_dict, expected_alerts_config.get(name) ) -%}
        {%- endif %}
    {%- endfor %}
{% endmacro %}