{% macro test_safe_get_with_default() %}

    {% set result = elementary.safe_get_with_default({}, 'test', none) %}
    {{ assert_value(result, none) }}

    {% set result = elementary.safe_get_with_default({'test': 1}, 'test', none) %}
    {{ assert_value(result, 1) }}

    {% set result = elementary.safe_get_with_default({'other': 1}, 'test', 'default') %}
    {{ assert_value(result, 'default') }}

    {% set result = elementary.safe_get_with_default({'other': 1}, 'test', none) %}
    {{ assert_value(result, none) }}

    {% set result = elementary.safe_get_with_default({'test': undefined}, 'test', 'default') %}
    {{ assert_value(result, 'default') }}

    {% set result = elementary.safe_get_with_default({'test': undefined}, 'test') %}
    {{ assert_value(result, none) }}

    {% set result = elementary.safe_get_with_default({'test': none}, 'test', 'default') %}
    {{ assert_value(result, 'default') }}

    {% set result = elementary.safe_get_with_default({'test': none}, 'test') %}
    {{ assert_value(result, none) }}

    {% set result = elementary.safe_get_with_default({'test': none}, 'test', []) %}
    {{ assert_value(result, []) }}

    {% set result = elementary.safe_get_with_default({'test': none}, 'test1', {}) %}
    {{ assert_value(result, {}) }}

    {% set result = elementary.safe_get_with_default({'test': none}, 'test', undefined) %}
    {{ assert_value(result, none) }}

    {% set result = elementary.safe_get_with_default({'other': none}, 'test', undefined) %}
    {{ assert_value(result, none) }}

{% endmacro %}