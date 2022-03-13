{%- macro cast_to_timestamp(timestamp_field) -%}
    cast({{ timestamp_field }} as {{ dbt_utils.type_timestamp() }})
{%- endmacro -%}

{%- macro null_string() -%}
    cast(null as {{ dbt_utils.type_string() }})
{%- endmacro -%}

{%- macro null_int() -%}
    cast(null as {{ dbt_utils.type_int() }})
{%- endmacro -%}

{%- macro null_timestamp() -%}
    cast(null as {{ dbt_utils.type_timestamp() }})
{%- endmacro -%}

