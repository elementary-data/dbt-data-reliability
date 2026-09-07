{# ---------------------------------------------------------------------- #}
{# Adapter-agnostic hooks for nested-column (STRUCT) monitoring.            #}
{# #}
{# The monitoring query macros are cross-warehouse and must not know about #}
{# any specific adapter, so the BigQuery STRUCT handling lives behind these #}
{# dispatched macros. The `default__` implementations are exact no-ops —    #}
{# byte-identical to the pre-nested-support behaviour — and the BigQuery    #}
{# specifics live in the `bigquery__` overrides, which delegate to the      #}
{# helpers in `macros/utils/sql_utils/bigquery_nested_columns.sql`.         #}
{# A new warehouse only needs its own `<adapter>__` overrides; the generic  #}
{# macros never change.                                                     #}
{# ---------------------------------------------------------------------- #}

{# Expand the columns of a monitored relation before a column is looked up by
   name. Lets an adapter surface nested STRUCT leaves (e.g. user.address.city)
   as monitorable columns. `column_name` is the requested column so an adapter
   can skip the work when the request cannot be a nested path. #}
{% macro flatten_columns_for_monitoring(column_objects, column_name) %}
    {{
        return(
            adapter.dispatch("flatten_columns_for_monitoring", "elementary")(
                column_objects, column_name
            )
        )
    }}
{% endmacro %}

{% macro default__flatten_columns_for_monitoring(column_objects, column_name) %}
    {{ return(column_objects) }}
{% endmacro %}

{% macro bigquery__flatten_columns_for_monitoring(column_objects, column_name) %}
    {#- Only a dotted name can refer to a nested STRUCT leaf, so skip the
        (potentially wide) flattening pass entirely for ordinary columns. -#}
    {%- if "." not in column_name -%} {{ return(column_objects) }} {%- endif -%}
    {{ return(elementary.bq_flatten_nested_columns(column_objects)) }}
{% endmacro %}


{# How to project a monitored column and how to reference it downstream.
   Returns {"projection": <select-list SQL>, "expression": <aggregate SQL>}.
   For an ordinary column both are just the quoted column; an adapter can
   override to project a computed expression under an alias and reference the
   alias in the metric aggregates. #}
{% macro monitored_column_projection(column_obj) %}
    {{
        return(
            adapter.dispatch("monitored_column_projection", "elementary")(
                column_obj
            )
        )
    }}
{% endmacro %}

{% macro default__monitored_column_projection(column_obj) %}
    {{ return({"projection": column_obj.quoted, "expression": column_obj.quoted}) }}
{% endmacro %}

{% macro bigquery__monitored_column_projection(column_obj) %}
    {#- A nested struct leaf (user.address.city) cannot be referenced via
        `column_obj.quoted` — that wraps the whole dotted name in one pair of
        backticks — and projecting it into a CTE unaliased would collapse the
        path to its last segment. Project it segment-quoted under a dot-free
        alias and have the metric aggregates reference that alias instead.
        Non-nested columns keep using `column_obj.quoted`, so identifier
        quoting (reserved words, case-sensitive names) is never lost. -#}
    {%- if elementary.bq_is_nested_identifier(column_obj.name) -%}
        {%- set alias = adapter.quote(elementary.bq_safe_alias(column_obj.name)) -%}
        {{
            return({
                "projection": (
                    elementary.bq_segment_quote(column_obj.name) ~ " as " ~ alias
                ),
                "expression": alias,
            })
        }}
    {%- else -%}
        {{
            return(
                {"projection": column_obj.quoted, "expression": column_obj.quoted}
            )
        }}
    {%- endif -%}
{% endmacro %}


{# SQL form of a dimension for the select list / concat expression. Plain
   identifiers and arbitrary SQL expressions pass through unchanged. #}
{% macro dimension_monitoring_sql(dimension) %}
    {{
        return(
            adapter.dispatch("dimension_monitoring_sql", "elementary")(dimension)
        )
    }}
{% endmacro %}

{% macro default__dimension_monitoring_sql(dimension) %}
    {{ return(dimension) }}
{% endmacro %}

{% macro bigquery__dimension_monitoring_sql(dimension) %}
    {{ return(elementary.bq_segment_quote(dimension)) }}
{% endmacro %}


{# Alias-safe form of a dimension, used to build the `dimension_<...>` column
   alias. Must be a dot-free identifier; plain identifiers and expressions pass
   through unchanged. #}
{% macro dimension_monitoring_alias(dimension) %}
    {{
        return(
            adapter.dispatch("dimension_monitoring_alias", "elementary")(dimension)
        )
    }}
{% endmacro %}

{% macro default__dimension_monitoring_alias(dimension) %}
    {{ return(dimension) }}
{% endmacro %}

{% macro bigquery__dimension_monitoring_alias(dimension) %}
    {{ return(elementary.bq_alias_safe_dimension(dimension)) }}
{% endmacro %}
