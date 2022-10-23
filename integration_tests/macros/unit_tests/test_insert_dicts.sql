{% macro test_insert_rows() %}

    {% set dicts = [{'test_column1': 'string_value', 'test_column2': 1},
                    {'test_column1': 'string_value2', 'test_column2': 2}] %}
    {% set unit_test_table_name = 'unit_test_table' %}
    {% set unit_test_table_schema = [('test_column1', 'string'), ('test_column2', 'int')] %}

    {% do drop_unit_test_table(unit_test_table_name) %}
    {% set unit_test_table_relation = create_unit_test_table(table_name=unit_test_table_name,
                                                             table_schema=unit_test_table_schema,
                                                             temp=False) %}
    {% do elementary.insert_rows(unit_test_table_relation, dicts) %}
    {% set row_count = elementary.get_row_count(unit_test_table_relation) %}
    {{ assert_value(row_count, dicts | length) }}
    {% do drop_unit_test_table(unit_test_table_name) %}

{% endmacro %}