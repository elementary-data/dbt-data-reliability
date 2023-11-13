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
