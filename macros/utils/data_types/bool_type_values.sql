{# Evaluate bool - For jinja bool true/false evaluation towards DBs compatible bool type#}
{% macro edr_evaluate_bool(return_value) -%}
    {%- if return_value == true -%} {{ edr_bool_true() }}   
    {%- else -%} {{ edr_bool_false() }}
    {%- endif -%}
{%- endmacro %}

{% macro edr_evaluate_bool_condition(return_value) -%}
    {%- if return_value == true -%} {{ edr_bool_true_condition() }}   
    {%- else -%} {{ edr_bool_false_condition() }}
    {%- endif -%}
{%- endmacro %}

{# Due to "faking" of bool using bit in T-SQL we have distinguish representation of true/false #}
{# We have different bools for use in parameter wheres vs. in columns as values#}


{#True bools#}

{% macro edr_bool_true() -%}
    {{ return(adapter.dispatch('edr_bool_true', 'elementary')()) }}
{%- endmacro %}

{% macro default__edr_bool_true() %}true{% endmacro %}

{% macro fabric__edr_bool_true() %}1{% endmacro %}


{% macro edr_bool_true_condition() -%}
    {{ return(adapter.dispatch('edr_bool_true_condition', 'elementary')()) }}
{%- endmacro %}

{% macro default__edr_bool_true_condition() %}true{% endmacro %}

{% macro fabric__edr_bool_true_condition() %}1 = 1{% endmacro %}

{# False bools#} 

{% macro edr_bool_false() -%}
    {{ return(adapter.dispatch('edr_bool_false', 'elementary')()) }}
{%- endmacro %}

{% macro default__edr_bool_false() %}false{% endmacro %}

{% macro fabric__edr_bool_false() %}0{% endmacro %}



{% macro edr_bool_false_condition() -%}
    {{ return(adapter.dispatch('edr_bool_false_condition', 'elementary')()) }}
{%- endmacro %}

{% macro default__edr_bool_false_condition() %}false{% endmacro %}

{% macro fabric__edr_bool_false_condition() %}0 = 1{% endmacro %}