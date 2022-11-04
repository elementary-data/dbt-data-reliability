{% macro test_get_insert_rows_query() %}

    {% set columns = [{'name': 'test_column1'}, {'name': 'test_column2'}] %}
    {% set dicts = [{'test_column1': 'string_value', 'test_column2': 1}] %}
    {% set unit_test_table_name = 'unit_test_table' %}
    {% set unit_test_table_schema = [('test_column1', 'string'), ('test_column2', 'int')] %}
    {% do drop_unit_test_table(unit_test_table_name) %}
    {% set unit_test_table_relation = create_unit_test_table(table_name=unit_test_table_name,
                                                             table_schema=unit_test_table_schema,
                                                             temp=False) %}
    {% set result = elementary.get_insert_rows_query(unit_test_table_relation, columns, dicts) %}
    {{ assert_str_in_value(unit_test_table_name, result) }}
    {{ assert_str_in_value('(test_column1,test_column2)', result) }}
    {{ assert_str_in_value("('string_value',1)", result) }}
    {% do drop_unit_test_table(unit_test_table_name) %}

{% endmacro %}