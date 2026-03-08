{% macro count_true(column_name) -%}
    {%- set bool_expr = (
        "cast(" ~ column_name ~ " as " ~ elementary.edr_type_bool() ~ ")"
    ) -%}
    coalesce(
        sum(case when {{ elementary.edr_is_true(bool_expr) }} then 1 else 0 end), 0
    )
{%- endmacro %}

{% macro count_false(column_name) -%}
    {%- set bool_expr = (
        "cast(" ~ column_name ~ " as " ~ elementary.edr_type_bool() ~ ")"
    ) -%}
    coalesce(
        sum(case when {{ elementary.edr_is_false(bool_expr) }} then 1 else 0 end), 0
    )
{%- endmacro %}
