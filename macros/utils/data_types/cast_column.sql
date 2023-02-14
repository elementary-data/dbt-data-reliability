{%- macro edr_cast_as_timestamp(timestamp_field) -%}
    cast({{ timestamp_field }} as {{ elementary.edr_type_timestamp() }})
{%- endmacro -%}

{%- macro edr_cast_as_float(column) -%}
    cast({{ column }} as {{ elementary.edr_type_float() }})
{%- endmacro -%}

{%- macro edr_cast_as_numeric(column) -%}
    cast({{ column }} as {{ elementary.edr_type_numeric() }})
{%- endmacro -%}

{%- macro edr_cast_as_int(column) -%}
    cast({{ column }} as {{ elementary.edr_type_int() }})
{%- endmacro -%}

{%- macro edr_cast_as_string(column) -%}
    cast({{ column }} as {{ elementary.edr_type_string() }})
{%- endmacro -%}

{%- macro edr_cast_as_long_string(column) -%}
    cast({{ column }} as {{ elementary.edr_type_long_string() }})
{%- endmacro -%}

{%- macro edr_cast_as_bool(column) -%}
    cast({{ column }} as {{ elementary.edr_type_bool() }})
{%- endmacro -%}

{%- macro const_as_string(string) -%}
    cast('{{ string }}' as {{ elementary.edr_type_string() }})
{%- endmacro -%}
