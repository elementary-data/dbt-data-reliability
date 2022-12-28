{%- macro null_int() -%}
    cast(null as {{ elementary.type_int() }})
{%- endmacro -%}

{%- macro null_timestamp() -%}
    cast(null as {{ elementary.type_timestamp() }})
{%- endmacro -%}

{%- macro null_float() -%}
    cast(null as {{ elementary.type_float() }})
{%- endmacro -%}

{% macro null_string() %}
    cast(null as {{ elementary.type_string() }})
{% endmacro %}
