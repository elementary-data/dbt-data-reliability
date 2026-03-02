{% macro fully_drop_relation(relation) %}
    {{ return(adapter.dispatch('fully_drop_relation', 'elementary')(relation)) }}
{% endmacro %}

{% macro default__fully_drop_relation(relation) %}
    {% do adapter.drop_relation(relation) %}
{% endmacro %}

{% macro athena__fully_drop_relation(relation) %}
    {% do adapter.clean_up_table(relation) %}
    {% do adapter.drop_relation(relation) %}
{% endmacro %}