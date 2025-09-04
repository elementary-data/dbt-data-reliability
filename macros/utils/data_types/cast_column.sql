{%- macro edr_cast_as_timestamp(timestamp_field) -%}
    {{ return(adapter.dispatch('edr_cast_as_timestamp', 'elementary')(timestamp_field)) }}
{%- endmacro -%}

{%- macro default__edr_cast_as_timestamp(timestamp_field) -%}
    cast({{ timestamp_field }} as {{ elementary.edr_type_timestamp() }})
{%- endmacro -%}

{# Athena and Trino needs explicit conversion for ISO8601 timestamps used in buckets_cte #}
{%- macro athena__edr_cast_as_timestamp(timestamp_field) -%}
    coalesce(
        try_cast({{ timestamp_field }} as {{ elementary.edr_type_timestamp() }}),
        cast(from_iso8601_timestamp(cast({{ timestamp_field }} AS {{ elementary.edr_type_string() }})) AS {{ elementary.edr_type_timestamp() }})
    )
{%- endmacro -%}

{%- macro trino__edr_cast_as_timestamp(timestamp_field) -%}
    coalesce(
        try_cast({{ timestamp_field }} as {{ elementary.edr_type_timestamp() }}),
        cast(from_iso8601_timestamp(cast({{ timestamp_field }} AS {{ elementary.edr_type_string() }})) AS {{ elementary.edr_type_timestamp() }})
    )
{%- endmacro -%}

{%- macro clickhouse__edr_cast_as_timestamp(timestamp_field) -%}
    coalesce(
        parseDateTimeBestEffortOrNull(toString({{ timestamp_field }}), 'UTC'),
        toDateTime('1970-01-01 00:00:00', 'UTC')
    )
{%- endmacro -%}

{%- macro dremio__edr_cast_as_timestamp(timestamp_field) -%}
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

{%- macro edr_cast_as_date(timestamp_field) -%}
    {{ return(adapter.dispatch('edr_cast_as_date', 'elementary')(timestamp_field)) }}
{%- endmacro -%}

{%- macro default__edr_cast_as_date(timestamp_field) -%}
    cast({{ timestamp_field }} as {{ elementary.edr_type_date() }})
{%- endmacro -%}

{# Bigquery (for some reason that is beyond me) can't cast a string as date if it's in timestamp format #}
{%- macro bigquery__edr_cast_as_date(timestamp_field) -%}
    cast({{ elementary.edr_cast_as_timestamp(timestamp_field) }} as {{ elementary.edr_type_date() }})
{%- endmacro -%}

{# Athena and Trino needs explicit conversion for ISO8601 timestamps used in buckets_cte #}
{%- macro athena__edr_cast_as_date(timestamp_field) -%}
    coalesce(
        try_cast({{ timestamp_field }} as {{ elementary.edr_type_date() }}),
        cast(from_iso8601_timestamp(cast({{ timestamp_field }} AS {{ elementary.edr_type_string() }})) AS {{ elementary.edr_type_date() }})
    )
{%- endmacro -%}

{%- macro trino__edr_cast_as_date(timestamp_field) -%}
    coalesce(
        try_cast({{ timestamp_field }} as {{ elementary.edr_type_date() }}),
        cast(from_iso8601_timestamp(cast({{ timestamp_field }} AS {{ elementary.edr_type_string() }})) AS {{ elementary.edr_type_date() }})
    )
{%- endmacro -%}

{%- macro dremio__edr_cast_as_date(timestamp_field) -%}
    cast({{ timestamp_field }} as {{ elementary.edr_type_date() }})
{%- endmacro -%}


{%- macro const_as_text(string) -%}
    {{ return(adapter.dispatch('const_as_text', 'elementary')(string)) }}
{%- endmacro -%}

{%- macro default__const_as_text(string) -%}
    {{ elementary.const_as_string(string) }}
{%- endmacro -%}

{%- macro redshift__const_as_text(string) -%}
    '{{ string }}'::text
{%- endmacro -%}
