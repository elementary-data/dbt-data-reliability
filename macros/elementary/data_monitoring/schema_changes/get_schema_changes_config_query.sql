{% macro get_schema_changes_config_query() %}
    {# We query from config without validating against information_schema, so we could alert on deleted tables #}
    select *
    from {{ elementary.get_table_config_path() }}
    where table_monitored = true
{% endmacro %}