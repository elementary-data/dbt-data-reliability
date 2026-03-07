{%- macro edr_concat(val1, val2) -%}
    concat(
        {{ elementary.edr_cast_as_string(val1) }},
        {{ elementary.edr_cast_as_string(val2) }}
    )
{%- endmacro -%}

{#- Cross-adapter concat that wraps dbt.concat().
    On Fabric the result is cast to varchar because CONCAT() returns nvarchar
    which Fabric does not support. -#}
{% macro edr_dbt_concat(fields) %}
    {{ return(adapter.dispatch("edr_dbt_concat", "elementary")(fields)) }}
{% endmacro %}

{% macro default__edr_dbt_concat(fields) %} {{ dbt.concat(fields) }} {% endmacro %}

{% macro fabric__edr_dbt_concat(fields) %}
    cast({{ dbt.concat(fields) }} as varchar(4000))
{% endmacro %}
