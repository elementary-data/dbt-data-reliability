{% materialization data_monitoring, default %}

    {%- set thread_number = config.get('thread_number') -%}
    {%- set monitored_tables = get_monitored_tables_list(thread_number) %}
    {%- set empty_sql = elementary.empty_data_monitors() %}
    {%- set model_name = model['name'] -%}

    {%- set old_relation = adapter.get_relation(identifier=model_name, schema=schema, database=database) -%}

    {# setup: drop old table and run pre hooks outside of transaction #}
    {%- if old_relation %}
        {{ adapter.drop_relation(old_relation) }}
    {%- endif %}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    {# 'BEGIN', run pre hooks inside of transaction #}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {# create empty target table #}
    {%- set target_relation = api.Relation.create(identifier=model_name,
                                                  schema=target.schema,
                                                  database=elementary.target_database(),
                                                  type='table') -%}

    {% call statement('main') -%}
        {{ get_create_table_as_sql(False, target_relation, empty_sql) }}
    {%- endcall %}

    {%- if monitored_tables is iterable and monitored_tables | length > 0 %}

        {# insert metrics to target table #}
        {%- for monitored_table in monitored_tables %}

            {%- set start_msg = 'Started running data monitors on table: ' ~ monitored_table %}
            {%- set end_msg = 'Finished running data monitors on table: ' ~ monitored_table %}

            {% do edr_log(start_msg) %}

            {%- set table_monitoring_query = elementary.table_monitoring_query(monitored_table) %}
            {%- set column_monitoring_query = elementary.column_monitoring_query(monitored_table) %}

            {%- set insert_table_monitoring = elementary.insert_as_select(this, table_monitoring_query) %}
            {%- do run_query(insert_table_monitoring) %}

            {%- set insert_column_monitoring = elementary.insert_as_select(this, column_monitoring_query) %}
            {%- do run_query(insert_column_monitoring) %}

            {% do edr_log(end_msg) %}

        {%- endfor %}

    {%- endif %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {# `COMMIT` happens here #}
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

