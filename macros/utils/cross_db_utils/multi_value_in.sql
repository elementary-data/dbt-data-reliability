{% macro edr_multi_value_in(source_cols, target_cols, target_table) %}
    {% do return(adapter.dispatch('edr_multi_value_in', 'elementary') (source_cols, target_cols, target_table)) %}
{% endmacro %}

{%- macro default__edr_multi_value_in(source_cols, target_cols, target_table) -%}
    (
        {%- for val in source_cols -%}
            {{ val }}
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
    ) in (
        select {% for val in target_cols -%}
            {{ val }}
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
        from {{ target_table }}
    )
{%- endmacro -%}

{%- macro bigquery__edr_multi_value_in(source_cols, target_cols, target_table) -%}
    -- BigQuery doesn't support multi-value IN, so we emulate it with CONCAT
    concat(
        {%- for val in source_cols -%}
            {{ elementary.edr_cast_as_string(val) -}}
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
    ) in (
        select concat({%- for val in target_cols -%}
            {{ elementary.edr_cast_as_string(val) -}}
            {%- if not loop.last %}, {% endif %}
        {%- endfor %})
        from {{ target_table }}
    )
{%- endmacro -%}

{%- macro redshift__edr_multi_value_in(source_cols, target_cols, target_table) -%}
    exists (
        select 1
        from {{ target_table }} as _edr_mv_target
        where
            {%- for i in range(source_cols | length) %}
                {{ source_cols[i] }} = _edr_mv_target.{{ target_cols[i] }}
                {%- if not loop.last %} and {% endif -%}
            {%- endfor %}
    )
{%- endmacro -%}
