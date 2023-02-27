{% macro true_bool(table_relation, timestamp_column) %}
    {{ return(adapter.dispatch('true_bool', 'elementary')()) }}
{%- endmacro %}

{% macro default__true_bool() %}
    
    'True'

{% endmacro %}

{% macro sqlserver__true_bool() %}
    
    1

{% endmacro %}
