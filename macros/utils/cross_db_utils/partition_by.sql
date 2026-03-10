{% macro get_default_partition_by() %}
    {% do return(adapter.dispatch("get_default_partition_by", "elementary")()) %}
{% endmacro %}

{%- macro bigquery__get_default_partition_by() %}
    {% if not elementary.get_config_var("bigquery_disable_partitioning") %}
        {% do return(
            {
                "field": "created_at",
                "data_type": "timestamp",
                "granularity": "day",
            }
        ) %}
    {% endif %}
    {% do return(none) %}
{% endmacro %}

{% macro default__get_default_partition_by() %} {% do return(none) %} {% endmacro %}
