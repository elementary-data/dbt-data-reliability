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

{% macro assert_value(value, expected_value) %}
    {% if value != expected_value %}
        {% do elementary.edr_log("FAILED: value " ~ value ~ " does not equal to " ~ expected_value) %}
        {{ return(1) }}
    {% else %}
        {% do elementary.edr_log("SUCCESS") %}
        {{ return(0) }}
    {% endif %}
{% endmacro %}