{% macro current_timestamp() -%}
  {{ return(adapter.dispatch('current_timestamp', 'elementary')()) }}
{%- endmacro %}

{% macro default__current_timestamp() %}
    current_timestamp::{{elementary.type_timestamp()}}
{% endmacro %}

{% macro redshift__current_timestamp() %}
    getdate()
{% endmacro %}

{% macro bigquery__current_timestamp() %}
    current_timestamp
{% endmacro %}



{% macro current_timestamp_in_utc() -%}
  {{ return(adapter.dispatch('current_timestamp_in_utc', 'elementary')()) }}
{%- endmacro %}

{% macro default__current_timestamp_in_utc() %}
    {{elementary.current_timestamp()}}
{% endmacro %}

{% macro snowflake__current_timestamp_in_utc() %}
    convert_timezone('UTC', {{elementary.current_timestamp()}})::{{elementary.type_timestamp()}}
{% endmacro %}

{% macro postgres__current_timestamp_in_utc() %}
    (current_timestamp at time zone 'utc')::{{elementary.type_timestamp()}}
{% endmacro %}

{# redshift should use default instead of postgres #}
{% macro redshift__current_timestamp_in_utc() %}
    {{ return(elementary.default__current_timestamp_in_utc()) }}
{% endmacro %}
