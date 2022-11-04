{% macro test_types() %}

    {% set dicts = [{'bigint_column': 2981833722}] %}
    {% set unit_test_table_name = 'unit_test_table' %}
    {% set unit_test_table_schema = [('bigint_column', 'bigint')] %}

    {% do drop_unit_test_table(unit_test_table_name) %}
    {% set unit_test_table_relation = create_unit_test_table(table_name=unit_test_table_name,
                                                             table_schema=unit_test_table_schema,
                                                             temp=False) %}
    {% do elementary.insert_rows(unit_test_table_relation, dicts) %}
    {% set row_count = elementary.get_row_count(unit_test_table_relation) %}
    {{ assert_value(row_count, dicts | length) }}
    {% do drop_unit_test_table(unit_test_table_name) %}

{% endmacro %}