{% macro false_bool(table_relation, timestamp_column) %}
    {{ return(adapter.dispatch('false_bool', 'elementary')()) }}
{%- endmacro %}

{% macro default__false_bool() %}
    
    'False'

{% endmacro %}

{% macro sqlserver__false_bool() %}
    
    0

{% endmacro %}