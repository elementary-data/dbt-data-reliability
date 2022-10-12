{% test schema_changes_from_baseline(model) %}
    -- depends_on: {{ ref('elementary_test_results') }}
    -- depends_on: {{ ref('schema_columns_snapshot') }}
    -- depends_on: {{ ref('filtered_information_schema_columns') }}

    {%- if execute and flags.WHICH in ['test', 'build'] %}
        {# get monitored table columns #}
        {%- set full_table_name = elementary.relation_to_full_name(model) %}
        {%- set model_relation = dbt.load_relation(model) %}
        {% if not model_relation %}
            {{ elementary.test_log('monitored_table_not_found', full_table_name) }}
            {{ return(elementary.no_results_query()) }}
        {% endif %}
        {%- set current_column_objects = adapter.get_columns_in_relation(model_relation) -%}

        {%- for column_obj in column_objects %}
            {% if column_obj.name | lower == column_name | lower %}
                {%- set column_monitors = elementary.column_monitors_by_type(column_obj.dtype, column_tests) %}
                {%- set column_item = {'column': column_obj, 'monitors': column_monitors} %}
                {{ return(column_item) }}
            {% endif %}
        {% endfor %}

        {{ print('------- current --------') }}
        {{ print(current_columns) }}
        {{ print('------- current --------') }}

        {# get schema baseline #}
        {%- set model_graph_node = elementary.get_model_graph_node(model_relation) %}
        {%- set model_yml_columns = model_graph_node.get('columns') %}
        {%- if model_yml_columns | length == 0 %}
            {{ exceptions.raise_compiler_error('No base schema defined in yml for schema_changes_from_baseline test') }}
        {%- endif %}

        {{ print('------- model_yml_columns --------') }}
        {{ print(model_yml_columns) }}
        {{ print('------- model_yml_columns --------') }}

        {# return schema changes query as standard test query #}
        {{ elementary.no_results_query() }}


    {% else %}

        {# test must run an sql query #}
        {{ elementary.no_results_query() }}

    {% endif %}

{% endtest %}