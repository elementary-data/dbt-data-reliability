{% macro get_column_data_type(column_relation) %}
    {% set data_type = adapter.dispatch('get_column_data_type','elementary')(column_relation) %}
    {{ return(data_type) }}
{% endmacro %}

{% macro default__get_column_data_type(column_relation) %}
   {{return (column_relation["dtype"]) }}
{% endmacro %}

{% macro bigquery__get_column_data_type(column_relation) %}
   {{return (column_relation["data_type"]) }}
{% endmacro %}
