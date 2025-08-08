{% macro table_monitoring_query(monitored_table, monitored_table_relation, min_bucket_start, max_bucket_end, table_metrics, metric_properties) %}

    {{ elementary.get_table_monitoring_query(monitored_table, monitored_table_relation, min_bucket_start, max_bucket_end, table_metrics, metric_properties) }}

    select
       {{ elementary.generate_surrogate_key([
                  'full_table_name',
                  'column_name',
                  'metric_name',
                  'metric_type',
                  'bucket_end',
                  'metric_properties'
                  ]) }}  as id,
        full_table_name,
        column_name,
        metric_name,
        metric_type,
        metric_value,
        source_value,
        bucket_start,
        bucket_end,
        bucket_duration_hours,
        {{ elementary.edr_current_timestamp_in_utc() }} as updated_at,
        dimension,
        dimension_value,
        metric_properties
    from metrics_final

{% endmacro %}

{% macro get_table_monitoring_query(monitored_table, monitored_table_relation, min_bucket_start, max_bucket_end, table_metrics, metric_properties) %}
    {%- set full_table_name_str = elementary.edr_quote(elementary.relation_to_full_name(monitored_table_relation)) %}
    {%- set timestamp_column = metric_properties.timestamp_column %}

    {%- if timestamp_column %}
        {{ elementary.get_timestamp_table_query(monitored_table, metric_properties, timestamp_column, table_metrics, min_bucket_start, max_bucket_end, full_table_name_str) }}
    {% else %}
        {{ elementary.get_no_timestamp_table_query(monitored_table, metric_properties, table_metrics, full_table_name_str) }}
    {% endif %}
{% endmacro %}

{% macro get_no_timestamp_table_query(monitored_table, metric_properties, table_metrics, full_table_name_str) %}
    with monitored_table as (
        select * from {{ monitored_table }}
        {% if metric_properties.where_expression %} where {{ metric_properties.where_expression }} {% endif %}
    ),

    metrics as (
        {{ elementary.get_unified_metrics_query(table_metrics, metric_properties) }}
    ),

    metrics_final as (
    select
        {{ elementary.edr_cast_as_string(full_table_name_str) }} as full_table_name,
        {{ elementary.null_string() }} as column_name,
        metric_name,
        metric_type,
        {{ elementary.edr_cast_as_float('metric_value') }} as metric_value,
        {{ elementary.null_string() }} as source_value,
        {{ elementary.null_timestamp() }} as bucket_start,
        {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.run_started_at_as_string())) }} as bucket_end,
        {{ elementary.null_int() }} as bucket_duration_hours,
        {{ elementary.null_string() }} as dimension,
        {{ elementary.null_string() }} as dimension_value,
        {{elementary.dict_to_quoted_json(metric_properties) }} as metric_properties
    from metrics
    )
{% endmacro %}

{% macro get_no_timestamp_volume_query(metric, metric_properties) %}
    select
        {{ elementary.const_as_string(metric.name) }} as metric_name,
        {{ elementary.const_as_string("row_count") }} as metric_type,
        {{ elementary.row_count() }} as metric_value
    from monitored_table
{% endmacro %}

{% macro get_timestamp_table_query(monitored_table, metric_properties, timestamp_column, table_metrics, min_bucket_start, max_bucket_end, full_table_name_str) %}
    {%- set timestamp_col_expr = elementary.edr_cast_as_timestamp(elementary.escape_reserved_keywords(timestamp_column)) -%}

    with partially_time_filtered_monitored_table as (
        select
            {{ timestamp_col_expr }} as monitored_table_timestamp_column
            {%- if metric_properties.timestamp_column and metric_properties.event_timestamp_column %}
            , {{ elementary.edr_cast_as_timestamp(elementary.escape_reserved_keywords(metric_properties.event_timestamp_column)) }} as monitored_table_event_timestamp_column
            {%- endif %}
        from {{ monitored_table }}
        -- Freshness metric calculated differences between consecutive buckets, thus the first diff
        -- is always null. Therefore we let few old buckets inside the query and filter them later, just for
        -- the first relevant diff not to be null
        where {{ timestamp_col_expr }} >= {{ elementary.edr_timeadd("day", -7, elementary.edr_cast_as_timestamp(min_bucket_start)) }}
        {% if metric_properties.where_expression %} and {{ metric_properties.where_expression }} {% endif %}
    ),
    monitored_table as (
        select
            *
        from partially_time_filtered_monitored_table
        where monitored_table_timestamp_column >= {{ elementary.edr_cast_as_timestamp(min_bucket_start) }}
    ),
    buckets as (
        select edr_bucket_start, edr_bucket_end
        from ({{ elementary.complete_buckets_cte(metric_properties, min_bucket_start, max_bucket_end) }}) results
        where edr_bucket_start >= {{ elementary.edr_cast_as_timestamp(min_bucket_start) }}
          and edr_bucket_end <= {{ elementary.edr_cast_as_timestamp(max_bucket_end) }}
    ),

    time_filtered_monitored_table as (
        select *,
               {{ elementary.get_start_bucket_in_data('monitored_table_timestamp_column', min_bucket_start, metric_properties.time_bucket) }} as start_bucket_in_data
        from monitored_table
        where
            monitored_table_timestamp_column >= (select min(edr_bucket_start) from buckets)
            and monitored_table_timestamp_column < (select max(edr_bucket_end) from buckets)
            {# To avoid adding buckets before the table first timestamp #}
            and monitored_table_timestamp_column >= (select min(monitored_table_timestamp_column) from monitored_table)
    ),

    metrics as (
        {{ elementary.get_unified_metrics_query(table_metrics, metric_properties) }}
    ),

    metrics_final as (
        select
            {{ elementary.edr_cast_as_string(full_table_name_str) }} as full_table_name,
            {{ elementary.null_string() }} as column_name,
            metric_name,
            metric_type,
            {{ elementary.edr_cast_as_float('metric_value') }} as metric_value,
            source_value,
            edr_bucket_start as bucket_start,
            edr_bucket_end as bucket_end,
            {{ elementary.timediff("hour", "edr_bucket_start", "edr_bucket_end") }} as bucket_duration_hours,
            {{ elementary.null_string() }} as dimension,
            {{ elementary.null_string() }} as dimension_value,
            {{elementary.dict_to_quoted_json(metric_properties) }} as metric_properties
        from
            metrics
        where (metric_value is not null and cast(metric_value as {{ elementary.edr_type_int() }}) < {{ elementary.get_config_var('max_int') }}) or
            metric_value is null
    )

{% endmacro %}

{% macro get_unified_metrics_query(table_metrics, metric_properties) %}
    {%- set metric_name_to_query = {} %}
    {%- for metric in table_metrics %}
        {% set metric_query = elementary.get_metric_query(metric, metric_properties) %}
        {% do metric_name_to_query.update({metric.name: metric_query}) %}
    {%- endfor %}

    {% if not metric_name_to_query %}
        {% if metric_properties.timestamp_column %}
            {% do return(elementary.empty_table([('edr_bucket_start','timestamp'),('edr_bucket_end','timestamp'),('metric_name','string'),('metric_type','string'),('source_value','string'),('metric_value','int')])) %}
        {% else %}
            {% do return(elementary.empty_table([('metric_name','string'),('metric_type','string'),('metric_value','int')])) %}
        {% endif %}
    {% endif %}

    with
    {%- for metric_name, metric_query in metric_name_to_query.items() %}
        {{ metric_name }} as (
            {{ metric_query }}
        ){% if not loop.last %},{% endif %}
    {%- endfor %}

    {%- for metric_name in metric_name_to_query %}
    select * from {{ metric_name }}
    {% if not loop.last %} union all {% endif %}
    {%- endfor %}
{% endmacro %}

{% macro get_metric_query(metric, metric_properties) %}
    {% if metric_properties.timestamp_column %}
        {% set metrics_macro_mapping = {
            "row_count": elementary.row_count_metric_query,
            "freshness": elementary.freshness_metric_query,
            "event_freshness": elementary.event_freshness_metric_query
        } %}
    {% else %}
        {% set metrics_macro_mapping = {
            "row_count": elementary.get_no_timestamp_volume_query,
            "event_freshness": elementary.get_no_timestamp_event_freshness_query
        } %}
    {% endif %}

    {%- set metric_macro = metrics_macro_mapping.get(metric.type) %}
    {%- if not metric_macro %}
        {%- do exceptions.raise_compiler_error("Unable to get macro query for metric '{}'".format(metric.type)) %}
    {%- endif %}

    {%- set metric_query = metric_macro(metric, metric_properties) %}
    {%- if not metric_query %}
        {%- do exceptions.raise_compiler_error("Unable to get query for metric '{}'".format(metric.type)) %}
    {%- endif %}

    {{ metric_query }}
{% endmacro %}

{% macro row_count_metric_query(metric, metric_properties) %}
    with row_count_values as (
        select edr_bucket_start,
               edr_bucket_end,
               start_bucket_in_data,
               case when start_bucket_in_data is null then
                   0
               else {{ elementary.edr_cast_as_float(elementary.row_count()) }} end as row_count_value
        from buckets left join time_filtered_monitored_table on (edr_bucket_start = start_bucket_in_data)
        group by 1,2,3
    )

    select edr_bucket_start,
           edr_bucket_end,
           {{ elementary.const_as_string(metric.name) }} as metric_name,
            {{ elementary.const_as_string("row_count") }} as metric_type,
           {{ elementary.null_string() }} as source_value,
           row_count_value as metric_value
    from row_count_values
{% endmacro %}

{% macro freshness_metric_query(metric, metric_properties) %}
    -- get ordered consecutive update timestamps in the source data
    with unique_timestamps as (
        select distinct monitored_table_timestamp_column as timestamp_val
        from partially_time_filtered_monitored_table
        order by 1
    ),

    -- compute freshness for every update as the time difference from the previous update
    consecutive_updates_freshness as (
        select
            timestamp_val as update_timestamp,
            {{ elementary.timediff('second', elementary.lag('timestamp_val') ~ ' over (order by timestamp_val)', 'timestamp_val') }} as freshness
        from unique_timestamps
    ),
    time_filtered_consecutive_updates_freshness as (
        select
            *
        from consecutive_updates_freshness
        where update_timestamp >= (select min(edr_bucket_start) from buckets)
    ),
    -- divide the freshness metrics above to buckets
    bucketed_consecutive_updates_freshness as (
        select
            edr_bucket_start, edr_bucket_end, update_timestamp, freshness
        from buckets cross join time_filtered_consecutive_updates_freshness
        where update_timestamp >= edr_bucket_start AND update_timestamp < edr_bucket_end
    ),

    -- we also want to record the freshness at the end of each bucket as an additional point. By this we mean
    -- the time that passed since the last update in the bucket and the end of the bucket.
    bucket_end_freshness as (
        select
            edr_bucket_start,
            edr_bucket_end,
            max(timestamp_val) as update_timestamp,
            {{ elementary.timediff('second', elementary.edr_cast_as_timestamp('max(timestamp_val)'), "least(edr_bucket_end, {})".format(elementary.current_timestamp_column())) }} as freshness
        from buckets cross join unique_timestamps
        where timestamp_val < edr_bucket_end
        group by 1,2
    ),

    -- create a single table with all the freshness values
    bucket_all_freshness_metrics as (
        select * from bucketed_consecutive_updates_freshness
        union all
        select * from bucket_end_freshness
    ),

    -- get all the freshness values, ranked by size (we use partition by and not group by, because we also want to have
    -- the associated timestamp as source value)
    bucket_freshness_ranked as (
        select
            *,
            row_number () over (partition by edr_bucket_end order by freshness is null, freshness desc) as row_num
        from bucket_all_freshness_metrics
    )

    select
        edr_bucket_start,
        edr_bucket_end,
        {{ elementary.const_as_string(metric.name) }} as metric_name,
        {{ elementary.const_as_string("freshness") }} as metric_type,
        {{ elementary.edr_cast_as_string('update_timestamp') }} as source_value,
        freshness as metric_value
    from bucket_freshness_ranked
    where row_num = 1
{% endmacro %}

{% macro event_freshness_metric_query(metric, metric_properties) %}
    select
        edr_bucket_start,
        edr_bucket_end,
        {{ elementary.const_as_string(metric.name) }} as metric_name,
        {{ elementary.const_as_string("event_freshness") }} as metric_type,
        {{ elementary.edr_cast_as_string('max({})'.format('monitored_table_event_timestamp_column')) }} as source_value,
        {{ 'coalesce(max({}), {})'.format(
                elementary.timediff('second', elementary.edr_cast_as_timestamp('monitored_table_event_timestamp_column'), elementary.edr_cast_as_timestamp('monitored_table_timestamp_column')),
                elementary.timediff('second', 'edr_bucket_start', 'edr_bucket_end')
            ) }} as metric_value
    from buckets left join time_filtered_monitored_table on (edr_bucket_start = start_bucket_in_data)
    group by 1,2
{% endmacro %}

{% macro get_no_timestamp_event_freshness_query(metric, metric_properties) %}
    {{ return(adapter.dispatch('get_no_timestamp_event_freshness_query', 'elementary')(metric, metric_properties)) }}
{% endmacro %}

{% macro default__get_no_timestamp_event_freshness_query(metric, metric_properties) %}
    select
        {{ elementary.const_as_string(metric.name) }} as metric_name,
        {{ elementary.const_as_string("event_freshness") }} as metric_type,
        {{ elementary.timediff('second', elementary.edr_cast_as_timestamp("max({})".format(metric_properties.event_timestamp_column)), elementary.edr_quote(elementary.get_run_started_at())) }} as metric_value
    from monitored_table
{% endmacro %}

{% macro clickhouse__get_no_timestamp_event_freshness_query(metric, metric_properties) %}
    select
        {{ elementary.const_as_string(metric.name) }} as metric_name,
        {{ elementary.const_as_string("event_freshness") }} as metric_type,
        {{ elementary.timediff('second', elementary.edr_cast_as_timestamp("max(toDateTime64({}, 3))".format(metric_properties.event_timestamp_column)), elementary.edr_quote(elementary.get_run_started_at())) }} as metric_value
    from monitored_table
{% endmacro %}
