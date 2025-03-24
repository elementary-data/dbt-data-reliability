{# Evaluate bool - For jinja bool true/false evaluation towards DBs compatible bool type#}

{# Due to "faking" of bool using bit in T-SQL we have distinguish representation of true/false #}
{# We have different bools for use in parameter wheres vs. in columns as values#}


{#True bools#}

{% macro edr_bool_true() -%}
    {{ return(adapter.dispatch('edr_bool_true', 'elementary')()) }}
{%- endmacro %}

{# False bools#} 

{% macro edr_bool_false() -%}
    {{ return(adapter.dispatch('edr_bool_false', 'elementary')()) }}
{%- endmacro %}

{% macro edr_evaluate_bool_condition(return_value) %}
{% if return_value == true %}
{{ return(adapter.dispatch('edr_bool_true', 'elementary')()) }}
{% else %}
{{ return(adapter.dispatch('edr_bool_false', 'elementary')()) }}
{% endif %}
{%endmacro%}


{% macro default__edr_bool_true() %}{%do return(true)%}{% endmacro %}

{% macro fabric__edr_bool_true() %}1{% endmacro %}

{% macro default__edr_bool_false() %}{%do return(false)%}{% endmacro %}

{% macro fabric__edr_bool_false() %}0{% endmacro %}
