{% test config_levels(model, expected_config, timestamp_column, time_bucket, where_expression, anomaly_sensitivity, anomaly_direction, days_back, backfill_days, seasonality) %}
    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {%- set unexpected_config = [] %}
        {%- set model_relation = dbt.load_relation(model) %}

        {% set timestamp_column, time_bucket, where_expression, anomaly_sensitivity, anomaly_direction, days_back, backfill_days, seasonality =
               elementary.get_anomalies_test_configuration(model_relation,
                                                           timestamp_column,
                                                           where_expression,
                                                           time_bucket,
                                                           anomaly_sensitivity,
                                                           anomaly_direction,
                                                           days_back,
                                                           backfill_days,
                                                           seasonality) %}

        --TODO: min_training_set

        {%- set configs_to_test = [('timestamp_column', timestamp_column),
                                   ('where_expression', where_expression),
                                   ('time_bucket', time_bucket),
                                   ('anomaly_sensitivity', anomaly_sensitivity),
                                   ('anomaly_direction', anomaly_direction),
                                   ('days_back', days_back),
                                   ('backfill_days', backfill_days),
                                   ('seasonality', seasonality)
                                   ] %}

        {%- for config in configs_to_test %}
            {%- set config_name, config_value = config %}
            {%- set config_check = compare_configs(config_name, config_value, expected_config) %}
            {%- if config_check %}
                {%- do unexpected_config.append(config_check) -%}
            {%- endif %}
        {%- endfor %}

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