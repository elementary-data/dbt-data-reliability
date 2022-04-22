{%- macro cast_as_timestamp(timestamp_field) -%}
    cast({{ timestamp_field }} as {{- dbt_utils.type_timestamp() -}})
{%- endmacro -%}

{%- macro cast_as_float(column) -%}
    cast({{ column }} as {{ dbt_utils.type_float() }})
{%- endmacro -%}

{%- macro cast_as_string(column) -%}
    cast({{ column }} as {{ elementary.type_string() }})
{%- endmacro -%}

{%- macro const_as_string(string) -%}
    cast('{{ string }}' as {{ elementary.type_string() }})
{%- endmacro -%}
