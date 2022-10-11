{% macro upload_dbt_invocation() %}
    {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
    {% if execute and not edr_cli_run %}
        {% if elementary.get_config_var('disable_uploading_dbt_invocation') %}
            {% do elementary.edr_log("dbt invocations logging is disabled, skipping upload of current dbt invocation.") %}
            {{ return('') }}
        {% endif %}
        {{ elementary.debug_log("Adding current dbt invocation to invocations table.") }}
        {% set database_name, schema_name = elementary.get_model_database_and_schema('elementary', 'dbt_invocations') %}
        {%- set dbt_invocations_relation = adapter.get_relation(database=database_name,
                                                                schema=schema_name,
                                                                identifier='dbt_invocations') -%}
        {# TODO: make it compatible with old dbt versions #}
        {% set current_dbt_invocation_dict = {
            'invocation_id': invocation_id,
            'generated_at': elementary.datetime_now_utc_as_string(),
            'dbt_command': flags.WHICH,
            'selected_resources': selected_resources,
            'project_hash': ''
        }%}

        {%- if dbt_invocations_relation -%}
            {% do elementary.insert_dicts(dbt_invocations_relation,
                                          [current_dbt_invocation_dict],
                                          elementary.get_config_var('dbt_artifacts_chunk_size'),
                                          should_commit=True) %}
        {%- endif -%}
    {% endif %}
    {{ elementary.edr_log("Uploaded current dbt invocation successfully.") }}
    {{ return ('') }}
{% endmacro %}

{% macro get_dbt_invocations_empty_table_query() %}
    {% set dbt_invocations_empty_table_query = elementary.empty_table([('invocation_id', 'long_string'),
                                                                       ('generated_at', 'string'),
                                                                       ('dbt_command', 'string'),
                                                                       ('selected_resources', 'long_string'),
                                                                       ('project_hash', 'string')
                                                                       ]) %}
    {{ return(dbt_invocations_empty_table_query) }}
{% endmacro %}

{%- macro get_project_hashes() -%}
    {% set hashable_test_nodes = {} %}
    {% set hashable_model_nodes = {} %}
    {%- for node in graph.nodes.values() -%}
        {%- if node.resource_type == 'test'-%}
            {% set flatten_test_dict = elementary.flatten_test(node) %}
            {% do hashable_test_nodes.update({node.unique_id: tojson(flatten_test_dict, sort_keys=True)})%}
        {%- elif node.resource_type == 'model' -%}
            {% set flatten_model_dict = elementary.flatten_model(node) %}
            {% do hashable_model_nodes.update({node.unique_id: tojson(flatten_model_dict, sort_keys=True)})%}
        {%- endif -%}
    {%- endfor -%}
    {# TODO: make it compatible with old dbt versions #}
    {{- return('') -}}
{%- endmacro -%}

{%- macro hacky_hash(some_relation, str_value) -%}
    {{- return(md5(str_value)) -}}
{%- endmacro -%}