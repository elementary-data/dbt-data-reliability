{% macro test_insensitive_get_dict_value() %}

    {% set result = elementary.insensitive_get_dict_value({}, 'test', none) %}
    {{ assert_value(result, none) }}

    {% set result = elementary.insensitive_get_dict_value({'test': 1}, 'test', none) %}
    {{ assert_value(result, 1) }}

    {% set result = elementary.insensitive_get_dict_value({'other': 1}, 'test', 'default') %}
    {{ assert_value(result, 'default') }}

    {% set result = elementary.insensitive_get_dict_value({'other': 1}, 'test', none) %}
    {{ assert_value(result, none) }}

    {% set result = elementary.insensitive_get_dict_value({'test': undefined}, 'test', 'default') %}
    {{ assert_value(result, 'default') }}

    {% set result = elementary.insensitive_get_dict_value({'test': undefined}, 'test') %}
    {{ assert_value(result, none) }}

    {% set result = elementary.insensitive_get_dict_value({'test': none}, 'test', 'default') %}
    {{ assert_value(result, 'default') }}

    {% set result = elementary.insensitive_get_dict_value({'test': none}, 'test') %}
    {{ assert_value(result, none) }}

{% endmacro %}