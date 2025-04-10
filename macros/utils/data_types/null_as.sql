{%- macro null_int() -%}
    {{ return(adapter.dispatch('null_int', 'elementary')()) }}
{%- endmacro -%}

{%- macro default__null_int() -%}
    cast(null as {{ elementary.edr_type_int() }})
{%- endmacro -%}

{%- macro clickhouse__null_int() -%}
    cast(null as Nullable({{ elementary.edr_type_int() }}))
{%- endmacro -%}

{%- macro null_timestamp() -%}
    {{ return(adapter.dispatch('null_timestamp', 'elementary')()) }}
{%- endmacro -%}

{%- macro default__null_timestamp() -%}
    cast(null as {{ elementary.edr_type_timestamp() }})
{%- endmacro -%}

{%- macro clickhouse__null_timestamp() -%}
    cast(null as Nullable({{ elementary.edr_type_timestamp() }}))
{%- endmacro -%}

{%- macro null_float() -%}
    cast(null as {{ elementary.edr_type_float() }})
{%- endmacro -%}

{% macro null_string() %}
    {{ return(adapter.dispatch('null_string', 'elementary')()) }}
{% endmacro %}

{% macro default__null_string() %}
    cast(null as {{ elementary.edr_type_string() }})
{% endmacro %}

{% macro clickhouse__null_string() %}
    cast(null as Nullable({{ elementary.edr_type_string() }}))
{% endmacro %}

{% macro null_boolean() %} 
    cast(null as {{ elementary.edr_type_bool() }}) 
{% endmacro %}
