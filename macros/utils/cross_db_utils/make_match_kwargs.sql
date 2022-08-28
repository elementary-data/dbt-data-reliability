{#
This macro copies the logic of adapter._make_match_kwargs.
https://github.com/dbt-labs/dbt-core/blob/a7ff003d4f74763f126e77a0fc9c55e7a3311b3d/core/dbt/adapters/base/impl.py#L707
#}

{% macro make_match_kwargs(database, schema, identifier) %}
    {{ return(adapter.dispatch('make_match_kwargs', 'elementary')(database, schema, identifier)) }}
{% endmacro %}

{% macro snowflake__make_match_kwargs(database, schema, identifier) %}
    {{ return([database.upper(), schema.upper(), identifier.upper()]) }}
{% endmacro %}

{% macro default__make_match_kwargs(database, schema, identifier) %}
    {{ return([database.lower(), schema.lower(), identifier.lower()]) }}
{% endmacro %}
