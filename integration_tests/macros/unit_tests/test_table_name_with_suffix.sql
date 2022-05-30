{% macro test_table_name_with_suffix() %}
    {% set table_name_with_suffix = elementary.table_name_with_suffix('my_table_name', '__metrics') %}
    {{ assert_value(table_name_with_suffix, 'my_table_name__metrics') }}

    {% set max_name_len = elementary.get_relation_max_name_length() %}
    {% set table_name_with_suffix = elementary.table_name_with_suffix('a' * 500, '__metrics') %}
    {% if max_name_len %}
        {% set expected_length = max_name_len %}
    {% else %}
        {% set expected_length = (500 + '__metrics' | length) %}
    {% endif %}
    {{ assert_value(table_name_with_suffix | length == expected_length, True) }}
{% endmacro %}