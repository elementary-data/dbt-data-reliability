{% macro monitored_tables(thread_number) %}
    select *
    from {{ ref('edr_tables_config') }}
    where table_monitored = true and thread_number = {{ thread_number }}
{% endmacro %}