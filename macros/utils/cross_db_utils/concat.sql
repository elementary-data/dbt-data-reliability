{%- macro edr_concat(val1, val2) -%}
    concat(
        {{ elementary.edr_cast_as_string(val1) }},
        {{ elementary.edr_cast_as_string(val2) }}
    )
{%- endmacro -%}

{#- Cross-adapter concat that wraps dbt.concat().
    On Fabric the result is cast to varchar(4000) because CONCAT() returns nvarchar
    which Fabric Warehouse does not support.

    NOTE: varchar(4000) is a known limitation and may truncate very long strings. -#}
{% macro edr_dbt_concat(fields) %}
    {{ return(adapter.dispatch("edr_dbt_concat", "elementary")(fields)) }}
{% endmacro %}

{% macro default__edr_dbt_concat(fields) %} {{ dbt.concat(fields) }} {% endmacro %}

{% macro fabric__edr_dbt_concat(fields) %}
    cast({{ dbt.concat(fields) }} as varchar(4000))
{% endmacro %}
