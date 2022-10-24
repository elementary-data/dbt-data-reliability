{% macro unit_test_numeric_monitors(dicts, table_name, table_schema) %}

    {% do drop_unit_test_table(table_name) %}
    {% do drop_unit_test_table(table_name ~ '__metrics') %}

    {% set monitors_inputs_table_relation = create_unit_test_table(table_name=table_name,
                                                             table_schema=table_schema,
                                                             temp=False) %}
    {% do elementary.insert_rows(monitors_inputs_table_relation, dicts) %}

    {%- set column_object = adapter.get_columns_in_relation(monitors_inputs_table_relation)[0] -%}
    {%- set default_all_types = elementary.get_config_var('edr_monitors')['column_any_type'] | list %}
    {%- set default_numeric_monitors = elementary.get_config_var('edr_monitors')['column_numeric'] | list %}
    {% set numeric_monitors = [] %}
    {% do numeric_monitors.extend(default_all_types) %}
    {% do numeric_monitors.extend(default_numeric_monitors) %}

    {%- set column_monitoring_query = elementary.column_monitoring_query(monitors_inputs_table_relation, none, false, elementary.get_run_started_at(), column_object, numeric_monitors) %}
    {%- set metrics_table_name = table_name ~ '__metrics' %}
    {%- set metrics_table_relation = get_or_create_unit_test_table_relation(metrics_table_name)[1] -%}
    {%- do elementary.create_or_replace(False, metrics_table_relation, column_monitoring_query) %}

    {% set row_count = elementary.get_row_count(metrics_table_relation) %}
    {{ assert_value(row_count, numeric_monitors | length) }}

    {% do drop_unit_test_table(table_name) %}
    {% do drop_unit_test_table(metrics_table_name) %}

{% endmacro %}


{% macro test_numeric_monitors() %}

    {% set dicts = [{'bigint_column': 2981833722},{'bigint_column': 2981833722}] %}
    {% set unit_test_table_name = 'unit_test_table' %}
    {% set unit_test_table_schema = [('bigint_column', 'bigint')] %}
    {%- do unit_test_numeric_monitors(dicts, unit_test_table_name, unit_test_table_schema) -%}


    {% set dicts = [{'int_column': -10000},{'int_column': 100000000}] %}
    {% set unit_test_table_name = 'unit_test_table' %}
    {% set unit_test_table_schema = [('int_column', 'int')] %}
    {%- do unit_test_numeric_monitors(dicts, unit_test_table_name, unit_test_table_schema) -%}

    {% set dicts = [{'float_column': 111111111111111111111.99999999999999999999999},{'float_column': 2981833722}] %}
    {% set unit_test_table_name = 'unit_test_table' %}
    {% set unit_test_table_schema = [('float_column', 'float')] %}
    {%- do unit_test_numeric_monitors(dicts, unit_test_table_name, unit_test_table_schema) -%}

    {% set dicts = [{'float_column': 111111111111111111111.99999999999999999999999},{'float_column': 1},{'float_column': 1000000},{'float_column': -1000000.222222222222222}] %}
    {% set unit_test_table_name = 'unit_test_table' %}
    {% set unit_test_table_schema = [('float_column', 'float')] %}
    {%- do unit_test_numeric_monitors(dicts, unit_test_table_name, unit_test_table_schema) -%}

{% endmacro %}

