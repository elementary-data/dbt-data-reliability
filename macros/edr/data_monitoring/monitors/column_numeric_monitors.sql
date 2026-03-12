{% macro max(column_name) -%}
    max(cast({{ column_name }} as {{ elementary.edr_type_float() }}))
{%- endmacro %}

{% macro min(column_name) -%}
    min(cast({{ column_name }} as {{ elementary.edr_type_float() }}))
{%- endmacro %}

{% macro average(column_name) -%}
    avg(cast({{ column_name }} as {{ elementary.edr_type_float() }}))
{%- endmacro %}

{% macro zero_count(column_name) %}
    coalesce(
        sum(
            case
                when {{ column_name }} is null
                then 1
                when cast({{ column_name }} as {{ elementary.edr_type_float() }}) = 0
                then 1
                else 0
            end
        ),
        0
    )
{% endmacro %}

{% macro zero_percent(column_name) %}
    {{
        elementary.edr_percent(
            elementary.zero_count(column_name), elementary.row_count()
        )
    }}
{% endmacro %}

{% macro not_zero_percent(column_name) %}
    {{
        elementary.edr_not_percent(
            elementary.zero_count(column_name), elementary.row_count()
        )
    }}
{% endmacro %}

{% macro standard_deviation(column_name) -%}
    {{ adapter.dispatch("standard_deviation", "elementary")(column_name) }}
{%- endmacro %}

{% macro default__standard_deviation(column_name) -%}
    stddev(cast({{ column_name }} as {{ elementary.edr_type_float() }}))
{%- endmacro %}

{% macro clickhouse__standard_deviation(column_name) -%}
    stddevPop(cast({{ column_name }} as Nullable({{ elementary.edr_type_float() }})))
{%- endmacro %}

{% macro dremio__standard_deviation(column_name) -%}
    -- Dremio's stddev in window functions can raise division by zero with single values
    -- stddev_pop returns 0 for single values instead of raising an error
    -- We'll handle the single-value case in the anomaly detection logic using
    -- training_set_size
    stddev_pop(cast({{ column_name }} as {{ elementary.edr_type_float() }}))
{%- endmacro %}

{# T-SQL uses STDEV instead of stddev #}
{% macro fabric__standard_deviation(column_name) -%}
    stdev(cast({{ column_name }} as {{ elementary.edr_type_float() }}))
{%- endmacro %}

{% macro variance(column_name) -%}
    {{ return(adapter.dispatch("variance", "elementary")(column_name)) }}
{%- endmacro %}

{% macro default__variance(column_name) -%}
    variance(cast({{ column_name }} as {{ elementary.edr_type_float() }}))
{%- endmacro %}

{% macro clickhouse__variance(column_name) -%}
    varSamp(cast({{ column_name }} as Nullable({{ elementary.edr_type_float() }})))
{%- endmacro %}

{# T-SQL uses VAR instead of variance #}
{% macro fabric__variance(column_name) -%}
    var(cast({{ column_name }} as {{ elementary.edr_type_float() }}))
{%- endmacro %}

{% macro sum(column_name) -%}
    sum(cast({{ column_name }} as {{ elementary.edr_type_float() }}))
{%- endmacro %}

{#- edr_normalize_stddev – post-process a stddev column reference so that
    floating-point artefacts (tiny non-zero values for constant inputs) are
    cleaned up.  The default implementation is the identity function; Vertica
    overrides it with round() because its STDDEV can return ~4e-08 for
    perfectly identical values. -#}
{% macro edr_normalize_stddev(column_expr) -%}
    {{ adapter.dispatch("edr_normalize_stddev", "elementary")(column_expr) }}
{%- endmacro %}

{% macro default__edr_normalize_stddev(column_expr) -%}
    {{ column_expr }}
{%- endmacro %}

{% macro vertica__edr_normalize_stddev(column_expr) -%}
    round({{ column_expr }}, 6)
{%- endmacro %}
