{%- macro null_int() -%}
    cast(null as {{ elementary.edr_type_int() }})
{%- endmacro -%}

{%- macro null_timestamp() -%}
    cast(null as {{ elementary.edr_type_timestamp() }})
{%- endmacro -%}

{%- macro null_float() -%}
    cast(null as {{ elementary.edr_type_float() }})
{%- endmacro -%}

{% macro null_string() %}
    cast(null as {{ elementary.edr_type_string() }})
{% endmacro %}

{% macro null_boolean() %} 
    cast(null as {{ elementary.edr_type_bool() }}) 
{% endmacro %}
