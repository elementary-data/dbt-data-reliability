{% macro monitors_query(thread_number) %}
    -- depends_on: {{ ref('elementary_runs') }}
    -- depends_on: {{ ref('edr_tables_config') }}
    -- depends_on: {{ ref('edr_columns_config') }}

    {%- set tables_queried = [] %}

    {%- set monitored_tables = run_query(monitored_tables(thread_number)) %}
    {%- if execute %}
        {%- set table_config_column_names = monitored_tables.column_names %}
    {%- endif %}

    {%- for monitored_table in monitored_tables %}
        {%- set full_table_name = monitored_table[table_config_column_names[1]] %}
        {%- set database_name = monitored_table[table_config_column_names[2]] %}
        {%- set schema_name = monitored_table[table_config_column_names[3]] %}
        {%- set table_name = monitored_table[table_config_column_names[4]] %}
        {%- set timestamp_column = monitored_table[table_config_column_names[5]] %}
        {%- set bucket_duration_hours = monitored_table[table_config_column_names[6]] | int %}
        {%- set table_monitored = monitored_table[table_config_column_names[7]] %}
        {%- set columns_monitored = monitored_table[table_config_column_names[9]] %}
        {%- set table_should_backfill = monitored_table[table_config_column_names[10]] %}

        {%- if table_monitored is sameas true %}
            {%- if monitored_table[table_config_column_names[8]] %}
                {%- set config_table_monitors = fromjson(monitored_table[table_config_column_names[8]]) %}
            {%- endif %}
            {%- set table_monitors = get_table_monitors(config_table_monitors) %}
        {%- endif %}

        {%- if columns_monitored is sameas true %}
            {%- set column_monitors_config = get_columns_monitors_config(full_table_name) %}
        {%- endif %}

        {%- if table_should_backfill is sameas true %}
            {%- set should_backfill = true %}
        {%- elif column_monitors_config is defined and column_monitors_config|length %}
            {%- set should_backfill_columns = [] %}
            {%- for i in column_monitors_config %}
                {% do should_backfill_columns.append(i['should_backfill']) %}
            {%- endfor %}
            {%- if should_backfill_columns[0] is sameas true %}
                {%- set should_backfill = true %}
            {%- endif %}
        {%- else %}
            {%- set should_backfill = false %}
        {%- endif %}

        {% set table_query = table_monitors_query(full_table_name, timestamp_column, var('days_back'), bucket_duration_hours, table_monitors, column_monitors_config, should_backfill) %}
        {%- if 'select' in table_query %}
            {{ table_query }}
            {%- if not loop.last %} union all {%- endif %}
            {% do tables_queried.append(full_table_name) %}
        {%- endif %}

    {%- endfor %}

    {%- if not tables_queried|length %}
        {{ empty_table([('table_name','str'),('column_name','str'),('metric_name','str'),('metric_value','int'),('timeframe_start','timestamp'),('timeframe_end','timestamp'),('timeframe_duration','int'),('run_started_at','timestamp')]) }}
        where table_name is not null
    {%- endif %}

{% endmacro %}

