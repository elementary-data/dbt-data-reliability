{%- macro edr_concat(val1, val2) -%}
    concat({{ elementary.edr_cast_as_string(val1) }}, {{ elementary.edr_cast_as_string(val2) }})
{%- endmacro -%}