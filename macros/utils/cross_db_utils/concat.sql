{#- Cross-adapter concat that wraps dbt.concat().
    Accepts a list of SQL expressions to concatenate.
    On Fabric the result is cast to varchar(4000) because CONCAT() returns nvarchar
    which Fabric Warehouse does not support.

    NOTE: varchar(4000) is a known limitation and may truncate very long strings. -#}
{% macro edr_concat(fields) %}
    {{ return(adapter.dispatch("edr_concat", "elementary")(fields)) }}
{% endmacro %}

{% macro default__edr_concat(fields) %} {{ dbt.concat(fields) }} {% endmacro %}

{% macro fabric__edr_concat(fields) %}
    cast({{ dbt.concat(fields) }} as varchar(4000))
{% endmacro %}
