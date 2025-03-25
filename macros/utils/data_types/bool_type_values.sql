{# Evaluate bool - For jinja bool true/false evaluation towards DBs compatible bool type#}

{# Due to "faking" of bool using bit in T-SQL we have distinguish representation of true/false #}
{# We have different bools for use in parameter wheres vs. in columns as values#}


{% macro edr_evaluate_bool(return_value) %}
{{ return(adapter.dispatch('edr_bool', 'elementary')(return_value)) }}
{%endmacro%}

{% macro edr_evaluate_bool_condition(return_value) %}
{{ return(adapter.dispatch('edr_bool_condition', 'elementary')(return_value)) }}
{%endmacro%}

{% macro default__edr_bool(return_value) %}
    {% if return_value == true %}
        {%do return(true)%}
    {% else %}
        {%do return(false)%}
    {% endif %}
{% endmacro %}


{% macro fabric__edr_bool(return_value) %}
    {% if return_value == true %}
        {%do return(1)%}
    {% else %}
        {%do return(0)%}
    {% endif %}
{% endmacro %}


{% macro default__edr_bool_condition(return_value) %}
    {% if return_value == true %}
        {%do return(true)%}
    {% else %}
        {%do return(false)%}
    {% endif %}
{% endmacro %}


{% macro fabric__edr_bool_condition(return_value) %}
    {% if return_value == true %}
        {%do return('1=1')%}
    {% else %}
        {%do return('0=1')%}
    {% endif %}
{% endmacro %}
