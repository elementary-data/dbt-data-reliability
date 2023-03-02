{% macro create_table_like(relation, like_relation, temporary=False) %}
    {{ return(adapter.dispatch('create_table_like', 'elementary')(relation, like_relation, temporary=False)) }}
{% endmacro %}

{% macro default__create_table_like(relation, like_relation, temporary=False) %}
    {% set empty_table_query = 'SELECT * FROM {} WHERE 1 = 0'.format(like_relation) %}
    {% do elementary.run_query(dbt.create_table_as(temporary, relation, empty_table_query)) %}
{% endmacro %}

{% macro sqlserver__create_table_like(relation, like_relation, temporary=False) %}
    {% set empty_table_query = 'SELECT * INTO {} FROM {} WHERE 1 = 0'.format(relation, like_relation) %}
    {% do elementary.run_query(empty_table_query) %}
{% endmacro %}
