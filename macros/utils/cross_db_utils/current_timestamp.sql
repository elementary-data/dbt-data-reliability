{% macro current_timestamp() %}
  {% set macro = dbt.current_timestamp_backcompat or dbt_utils.current_timestamp %}
  {% if not macro %}
    {{ exceptions.raise_compiler_error("Did not find a current_timestamp macro.") }}
  {% endif %}
  {{ return(macro) }}
{% endmacro %}


{% macro current_timestamp_in_utc() %}
  {% if dbt_version >= '1.2.0' %}
        {# This macro is depricated from dbt_utils version 0.9.0, but still hasn't got an equivalent macro at dbt-core #}
        {# Should be replaced to the equivalent macro once it released #}
        {{ return(dbt_utils.current_timestamp_in_utc()) }}
    {% else %}
        {{ return(dbt_utils.current_timestamp_in_utc()) }}
    {% endif %}
{% endmacro %}
