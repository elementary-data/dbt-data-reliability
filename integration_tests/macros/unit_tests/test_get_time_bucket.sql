{% macro test_get_time_bucket() %}

    {%- set default_config = elementary.get_default_time_bucket() %}

    {%- set config = {'period': 'day', 'count': 5, 'bla': 'bla'} %}
    {%- set result = elementary.get_time_bucket(config) %}
    {{ assert_value(result.period, "day") }}
    {{ assert_value(result.count, 5) }}


    {%- set config = {'period': 'day', 'count': 5} %}
    {%- set result = elementary.get_time_bucket(config) %}
    {{ assert_value(result.period, "day") }}
    {{ assert_value(result.count, 5) }}

    {%- set config = {'period': 'week'} %}
    {%- set result = elementary.get_time_bucket(config) %}
    {{ assert_value(result.period, "week") }}
    {{ assert_value(result.count, default_config.count) }}

    {%- set config = {'count': 3} %}
    {%- set result = elementary.get_time_bucket(config) %}
    {{ assert_value(result.count, 3) }}
    {{ assert_value(result.period, default_config.period) }}

    {%- set result = elementary.get_time_bucket() %}
    {{ assert_value(result.count, default_config.count) }}
    {{ assert_value(result.period, default_config.period) }}

{% endmacro %}