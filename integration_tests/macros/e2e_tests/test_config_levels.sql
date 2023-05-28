{% test config_levels(model, expected_config, time_bucket) %}
    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {%- set unexpected_config = [] %}
        {%- set model_relation = dbt.load_relation(model) %}
        {% set model_graph_node = elementary.get_model_graph_node(model_relation) %}

        {%- set time_bucket = elementary.get_time_bucket(time_bucket, model_graph_node) %}
        {%- if time_bucket != expected_config.get('time_bucket') %}
            {%- set unexpected_message = ('got config: {0}, expected config: {1}').format(time_bucket, expected_config.get('time_bucket') ) %}
            {%- do unexpected_config.append(unexpected_message) -%}
        {%- endif %}

        {%- if unexpected_config | length > 0 %}
            {%- do exceptions.raise_compiler_error('Failure config_levels: ' ~ unexpected_config) -%}
        {%- else %}
            {#- test must run an sql query -#}
            {{ elementary.no_results_query() }}
        {%- endif %}
    {%- endif %}
{%- endtest %}