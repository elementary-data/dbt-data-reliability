{%- macro tables_to_alert_on_schema_changes() -%}
    {%- set table_to_alert_on_schema_changes_list = [] -%}
    {%- set table_config_query -%}
    {# We query from config without validating against information_schema, so we could alert on deleted tables #}
        select *
            from {{ elementary.get_table_config_path() }}
            where table_monitored = true
    {%- endset -%}
    {%- set monitored_tables = run_query(table_config_query) -%}
    {%- for monitored_table in monitored_tables -%}
        {%- set full_table_name = elementary.insensitive_get_dict_value(monitored_table, 'full_table_name') -%}
        {%- set table_monitors = elementary.insensitive_get_dict_value(monitored_table, 'table_monitors') -%}
        {%- set default_table_monitors = var('edr_monitors')['table'] | list %}
        {%- if table_monitors is none and 'schema_changes' in default_table_monitors %}
            {%- do table_to_alert_on_schema_changes_list.append(full_table_name | upper) %}
        {%- elif table_monitors is not none -%}
            {%- if 'schema_changes' in fromjson(table_monitors) %}
                {%- do table_to_alert_on_schema_changes_list.append(full_table_name | upper) %}
            {%- endif %}
        {%- endif %}
    {%- endfor %}
    {%- set tables_to_alert_on_schema_changes = elementary.strings_list_to_tuple(table_to_alert_on_schema_changes_list) %}
    {{ return(tables_to_alert_on_schema_changes) }}

{%- endmacro -%}