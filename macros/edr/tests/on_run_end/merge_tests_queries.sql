{% macro get_temp_tables(temp_tables_type) %}
    {%- set database_name = elementary.target_database() %}
    {%- set tables_suffix = '__' ~  temp_tables_type %}
    {%- set schema_name = schema %}

    {%- set info_schema_query %}
        with temp_tables as (
            select upper(table_catalog) as database_name,
                   upper(table_schema) as schema_name,
                   upper(table_name) as table_name
            from {{ elementary.from_information_schema('TABLES', database_name) }}
            where lower(table_schema) like lower('{{ schema_name }}')
                and lower(table_name) like '%{{ tables_suffix }}'
            )
        select {{ elementary.full_table_name() }} as full_table_name
        from temp_tables
    {%- endset %}

    {%- set temp_tables = elementary.result_column_to_list(info_schema_query) %}
    {{ return(temp_tables) }}
{% endmacro %}


{% macro union_anomalies_query() %}
    {%- set temp_tables_list = elementary.get_temp_tables('anomalies') %}
    {%- if temp_tables_list | length > 0 %}
        {%- set union_temp_query -%}
            {%- for temp_table in temp_tables_list -%}
                select * from {{- elementary.from(temp_table) -}}
                {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
        {%- endset %}
        {{ return(union_temp_query) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}


{% macro union_metrics_query() %}
    {%- set temp_tables_list = elementary.get_temp_tables('metrics') %}
    {%- if temp_tables_list | length > 0 %}
        {%- set union_temp_query -%}
            with union_temps as (
            {%- for temp_table in temp_tables_list -%}
                select * from {{- elementary.from(temp_table) -}}
                {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
                )
            select *
            from union_temps
            qualify row_number() over (partition by id order by updated_at desc) = 1
        {%- endset %}
        {{ return(union_temp_query) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}


{% macro anomalies_alerts_query() %}
    {%- set temp_tables_list = elementary.get_temp_tables('anomalies') %}
    {%- if temp_tables_list | length > 0 %}
        {%- set anomalies_alerts_query %}
            with union_temp as (
                {{ elementary.union_anomalies_query() }}
            )
            select
                id as alert_id,
                {{ elementary.current_timestamp_column() }} as detected_at,
                {{ elementary.full_name_split('database_name') }},
                {{ elementary.full_name_split('schema_name') }},
                {{ elementary.full_name_split('table_name') }},
                column_name,
                'anomaly_detection' as alert_type,
                metric_name as sub_type,
                {{ elementary.anomaly_detection_description() }},
                {{ elementary.null_string() }} as owner,
                {{ elementary.null_string() }} as tags,
                {{ elementary.null_string() }} as alert_results_query,
                {{ elementary.null_string() }} as other
            from union_temp
            qualify row_number() over (partition by id order by detected_at desc) = 1
        {%- endset %}
        {{ return(anomalies_alerts_query) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}


{% macro union_schema_changes_query() %}
    {%- set temp_tables_list = elementary.get_temp_tables('schema_alerts') %}
    {%- if temp_tables_list | length > 0 %}
        {%- set union_temp_query -%}
            with union_temps as (
            {%- for temp_table in temp_tables_list -%}
                select * from {{- elementary.from(temp_table) -}}
                {%- if not loop.last %} union all {% endif %}
            {%- endfor %}
                )
            select *
            from union_temps
            qualify row_number() over (partition by alert_id order by detected_at desc) = 1
        {%- endset %}
        {{ return(union_temp_query) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}