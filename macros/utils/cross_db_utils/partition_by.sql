{% macro get_partition_by(column="created_at") %}
    {% do return(adapter.dispatch("get_partition_by", "elementary")(column)) %}
{% endmacro %}

{# Backward-compatible alias so existing user overrides / references keep working. #}
{% macro get_default_partition_by() %}
    {% do return(elementary.get_partition_by()) %}
{% endmacro %}

{%- macro bigquery__get_partition_by(column) %}
    {% if not elementary.get_config_var("bigquery_disable_partitioning") %}
        {% do return(
            {
                "field": column,
                "data_type": "timestamp",
                "granularity": "day",
            }
        ) %}
    {% endif %}
    {% do return(none) %}
{% endmacro %}

{% macro default__get_partition_by(column) %} {% do return(none) %} {% endmacro %}
