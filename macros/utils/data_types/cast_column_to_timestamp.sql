{%- macro cast_column_to_timestamp(timestamp_field) -%}
    cast({{ timestamp_field }} as {{ dbt_utils.type_timestamp() }})
{%- endmacro -%}
