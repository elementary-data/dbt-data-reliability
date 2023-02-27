{% macro set(value_expression, expression_name, first_flag, last_flag) -%}
    {{ return(adapter.dispatch('set', 'elementary')(value_expression, expression_name, first_flag, last_flag)) }}
{%- endmacro %}

{% macro default__set(value_expression, expression_name, first_flag, last_flag) %}

    {% if first_flag %} with {% endif %} {{ expression_name }} as (
            {{ value_expression }}
            ) {% if not last_flag %}, {% endif %}

{% endmacro %}

{% macro sqlserver__set(value_expression, expression_name, first_flag, last_flag) %}

{% endmacro %}


{% macro unset(value_expression, expression_name, first_flag, last_flag) -%}
    {{ return(adapter.dispatch('unset', 'elementary')(value_expression, expression_name, first_flag, last_flag)) }}
{%- endmacro %}

{% macro default__unset(value_expression, expression_name, first_flag, last_flag) %}
    
    {{ expression_name }}

{% endmacro %}

{% macro sqlserver__unset(value_expression, expression_name, first_flag, last_flag) %}
    
        (
            {{ value_expression }}
        ) {{ expression_name }} {% if not last_flag %}, {% endif %}

{% endmacro %}
