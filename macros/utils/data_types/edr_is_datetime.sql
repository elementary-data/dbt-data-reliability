{% macro edr_is_datetime(val) %}
    {# Duck-typed check for date/datetime objects (works on both dbt-core and Fusion) #}
    {% do return(
        val is not none
        and val is not string
        and val is not mapping
        and val.year is defined
    ) %}
{% endmacro %}
