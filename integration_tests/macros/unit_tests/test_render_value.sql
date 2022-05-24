{% macro test_render_value() %}

    {% set result = elementary.render_value('test') %}
    {{ assert_value(result, "'test'") }}

    {% set result = elementary.render_value(undefined) %}
    {{ assert_value(result, "NULL") }}

    {% set result = elementary.render_value(none) %}
    {{ assert_value(result, "NULL") }}

    {% set result = elementary.render_value({'test': 'dict'}) %}
    {{ assert_value(result, "'{\"test\": \"dict\"}'") }}

    {% set result = elementary.render_value([1,2,3]) %}
    {{ assert_value(result, "'[1, 2, 3]'") }}

    {% set result = elementary.render_value([]) %}
    {{ assert_value(result, "'[]'") }}

    {% set result = elementary.render_value({}) %}
    {{ assert_value(result, "'{}'") }}

{% endmacro %}