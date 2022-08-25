{%- macro cast_as_timestamp(timestamp_field) -%}
    cast({{ timestamp_field }} as {{ elementary.type_timestamp() }})
{%- endmacro -%}

{%- macro cast_as_float(column) -%}
    cast({{ column }} as {{ elementary.type_float() }})
{%- endmacro -%}

{%- macro cast_as_string(column) -%}
    cast({{ column }} as {{ elementary.type_string() }})
{%- endmacro -%}

{%- macro cast_as_long_string(column) -%}
    cast({{ column }} as {{ elementary.type_long_string() }})
{%- endmacro -%}

{%- macro cast_as_bool(column) -%}
    cast({{ column }} as {{ elementary.type_bool() }})
{%- endmacro -%}

{%- macro const_as_string(string) -%}
    cast('{{ string }}' as {{ elementary.type_string() }})
{%- endmacro -%}
