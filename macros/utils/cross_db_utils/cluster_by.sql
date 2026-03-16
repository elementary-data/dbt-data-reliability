{% macro get_cluster_by(columns) %}
    {% do return(adapter.dispatch("get_cluster_by", "elementary")(columns)) %}
{% endmacro %}

{%- macro bigquery__get_cluster_by(columns) %}
    {% if not elementary.get_config_var("bigquery_disable_clustering") %}
        {% do return(columns) %}
    {% endif %}
    {% do return(none) %}
{% endmacro %}

{% macro default__get_cluster_by(columns) %} {% do return(none) %} {% endmacro %}
