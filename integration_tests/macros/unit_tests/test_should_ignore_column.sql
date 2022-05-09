{% macro test_should_ignore_column() %}

    {% set result = elementary.should_ignore_column(column_name='null_count_int',
                                                    exclude_prefix='null') %}
    {{ assert_value(result, True) }}

    {% set result = elementary.should_ignore_column(column_name='null_count_int',
                                                    exclude_prefix='count') %}
    {{ assert_value(result, False) }}

    {% set result = elementary.should_ignore_column(column_name='null_count_int',
                                                    exclude_regexp='.*count.*') %}
    {{ assert_value(result, True) }}

    {% set result = elementary.should_ignore_column(column_name='null_count_int',
                                                    exclude_regexp='.*count$') %}
    {{ assert_value(result, False) }}

    {% set result = elementary.should_ignore_column(column_name='null_count_int',
                                                    exclude_regexp='.*count.*',
                                                    exclude_prefix='bla') %}
    {{ assert_value(result, True) }}

    {% set result = elementary.should_ignore_column(column_name='null_count_int',
                                                    exclude_regexp='.*count.*',
                                                    exclude_prefix='null') %}
    {{ assert_value(result, True) }}

    {% set result = elementary.should_ignore_column(column_name='null_count_int',
                                                    exclude_regexp='.*countint',
                                                    exclude_prefix='null') %}
    {{ assert_value(result, True) }}

    {% set result = elementary.should_ignore_column(column_name='null_count_int',
                                                    exclude_regexp='.*countint',
                                                    exclude_prefix='nullcount') %}
    {{ assert_value(result, False) }}

    {% set result = elementary.should_ignore_column(column_name='null_count_int') %}
    {{ assert_value(result, False) }}

    {% set result = elementary.should_ignore_column(column_name='null_count_int',
                                                    exclude_regexp=none,
                                                    exclude_prefix=none) %}
    {{ assert_value(result, False) }}

{% endmacro %}

