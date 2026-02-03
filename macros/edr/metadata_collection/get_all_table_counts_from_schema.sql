{%- macro get_all_table_counts_from_schema(schema_name, catalog_name = target.catalog | default(target.database, true)) -%}

{%- set schema_to_use = target.schema ~ '_' ~ schema_name -%}

{%- set catalog_sql %}
    SELECT table_catalog, table_schema, table_name
    FROM {{ catalog_name }}.information_schema.tables t
    where t.table_schema = '{{schema_to_use}}'
{%- endset -%}

{%- set results = dbt_utils.get_query_results_as_dict(catalog_sql) -%}

{% for relation in results['table_name'] %}

    {%- if loop.first -%}
        {% do log(msg='Number of counts to perform: ' ~ loop.length, info=true) %}
    {%- endif -%}

    select '{{run_started_at}}' as date_time, '{{results['table_catalog'][loop.index0]}}' as catalog_name, '{{results['table_schema'][loop.index0]}}' as schema_name, '{{relation}}' as table_name, count(*) as count
    from {{ adapter.quote(results['table_catalog'][loop.index0]) }}.{{ adapter.quote(results['table_schema'][loop.index0]) }}.{{ adapter.quote(results['table_name'][loop.index0]) }} tab

    {% if not ( loop.last ) -%}
        union all
    {% endif %}

{%- endfor -%}

{%- endmacro %}
