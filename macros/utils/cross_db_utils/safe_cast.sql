{% macro safe_cast(field, type) %}
    {{ return(adapter.dispatch('safe_cast', 'elementary') (field, type)) }}
{% endmacro %}

{% macro default__safe_cast(field, type) %}
    {% if dbt_version >= '1.2.0' %}
        {{ return(dbt.safe_cast(field, type)) }}
    {% else %}
        {{ return(dbt_utils.safe_cast(field, type)) }}
    {% endif %}
{% endmacro %}

{% macro databricks__safe_cast(field, type) %}
    try_cast({{field}} as {{type}})
{% endmacro %}

{% macro spark__safe_cast(field, type) %}
    try_cast({{field}} as {{type}})
{% endmacro %}
