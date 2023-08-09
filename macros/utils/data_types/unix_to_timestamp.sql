{%- macro unix_to_timestamp(unix_ts) -%}
{{ adapter.dispatch("unix_to_timestamp", "elementary")(unix_ts) }}
{%- endmacro %}

{%- macro default__unix_to_timestamp(unix_ts) -%}
to_timestamp({{ unix_ts }})
{%- endmacro %}

{%- macro bigquery__unix_to_timestamp(unix_ts) -%}
timestamp_seconds({{ elementary.edr_cast_as_int(unix_ts) }})
{%- endmacro %}
