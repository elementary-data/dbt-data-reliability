{% macro has_temp_table_support() %}
    {% do return(adapter.dispatch("has_temp_table_support", "elementary")()) %}
{% endmacro %}

{% macro default__has_temp_table_support() %}
    {% do return(true) %}
{% endmacro %}

{% macro spark__has_temp_table_support() %}
    {% do return(false) %}
{% endmacro %}

{% macro trino__has_temp_table_support() %}
    {% do return(false) %}
{% endmacro %}

{% macro athena__has_temp_table_support() %}
    {% do return(false) %}
{% endmacro %}

{% macro dremio__has_temp_table_support() %}
    {% do return(false) %}
{% endmacro %}

{% macro clickhouse__has_temp_table_support() %}
    {% do return(false) %}
{% endmacro %}

{% macro redshift__has_temp_table_support() %}
    {# Workaround for dbt-fusion 2.0.0-preview.104 ADBC 0.22 bug
       where get_columns_in_relation returns empty catalog/schema for temp tables
       causing panic: "Either resolved_catalog or resolved_schema must be present"
       at fs/sa/crates/dbt-adapter/src/metadata/mod.rs:91:9

       This occurs intermittently during INSERT operations on temporary tables.
       Disabling temp tables for Redshift+fusion uses regular tables instead,
       which is slightly less efficient but avoids the panic.

       Bug introduced in preview.104, not present in preview.102.
       TODO: Re-enable once dbt-fusion fixes this in a future release. #}
    {% if elementary.is_dbt_fusion() %}
        {% do return(false) %}
    {% else %}
        {% do return(true) %}
    {% endif %}
{% endmacro %}

