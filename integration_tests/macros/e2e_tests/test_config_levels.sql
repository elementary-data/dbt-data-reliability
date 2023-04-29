{% test config_levels(model, expected_config, timestamp_column, time_bucket, where_expression) %}
    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {%- set unexpected_config = [] %}
        {%- set model_relation = dbt.load_relation(model) %}

        {% set timestamp_column, time_bucket, where_expression = elementary.get_anomalies_test_configuration(model_relation, timestamp_column, where_expression, time_bucket) %}

        {%- set time_bucket_check = compare_configs('time_bucket', time_bucket, expected_config) %}
        {%- if time_bucket_check %}
            {%- do unexpected_config.append(time_bucket_check) -%}
        {%- endif %}

        {%- set timestamp_column_check = compare_configs('timestamp_column', timestamp_column, expected_config) %}
        {%- if timestamp_column_check %}
            {%- do unexpected_config.append(timestamp_column_check) -%}
        {%- endif %}

        {%- set where_expression_check = compare_configs('where_expression', where_expression, expected_config) %}
        {%- if where_expression_check %}
            {%- do unexpected_config.append(where_expression_check) -%}
        {%- endif %}

        {%- if unexpected_config | length > 0 %}
            {%- do exceptions.raise_compiler_error('Failure config_levels: ' ~ unexpected_config) -%}
        {%- else %}
            {#- test must run an sql query -#}
            {{ elementary.no_results_query() }}
        {%- endif %}
    {%- endif %}
{%- endtest %}

{% macro compare_configs(config_name, config, expected_config) %}
    {%- if config != expected_config.get(config_name) %}
        {%- set unexpected_message = ('got config: {0}, expected config: {1}').format(config, expected_config.get(config_name) ) %}
        {{ return(unexpected_message) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}