{% macro table_monitoring_query(monitored_table_relation, timestamp_column, min_bucket_start, table_monitors, where_expression, time_bucket, metric_args) %}

    {% set full_table_name_str = elementary.quote(elementary.relation_to_full_name(monitored_table_relation)) %}

    with monitored_table as (
        select * from {{ monitored_table_relation }}
        {% if where_expression %}
        where {{ where_expression }}
        {% endif %}
    ),

    {% if timestamp_column %}
        buckets as (
            select edr_bucket_start, edr_bucket_end from ({{ elementary.complete_buckets_cte(time_bucket) }}) results
            where edr_bucket_start >= {{ elementary.cast_as_timestamp(min_bucket_start) }}
        ),

        time_filtered_monitored_table as (
            select *,
                   {{ elementary.get_start_bucket_in_data(timestamp_column, min_bucket_start, time_bucket) }} as start_bucket_in_data
            from monitored_table
            where
                {{ elementary.cast_as_timestamp(timestamp_column) }} >= (select min(edr_bucket_start) from buckets)
                and {{ elementary.cast_as_timestamp(timestamp_column) }} < (select max(edr_bucket_end) from buckets)
        ),

        metrics as (
            {{ elementary.get_unified_metrics_query(metrics=table_monitors,
                                                    metric_args=metric_args,
                                                    timestamp_column=timestamp_column) }}
        ),

        metrics_final as (

        select
            {{ elementary.cast_as_string(full_table_name_str) }} as full_table_name,
            {{ elementary.null_string() }} as column_name,
            metric_name,
            {{ elementary.cast_as_float('metric_value') }} as metric_value,
            source_value,
            edr_bucket_start as bucket_start,
            edr_bucket_end as bucket_end,
            {{ elementary.timediff("hour", "edr_bucket_start", "edr_bucket_end") }} as bucket_duration_hours,
            {{ elementary.null_string() }} as dimension,
            {{ elementary.null_string() }} as dimension_value
        from
            metrics
        where (metric_value is not null and cast(metric_value as {{ elementary.type_int() }}) < {{ elementary.get_config_var('max_int') }}) or
            metric_value is null
        )
    {% else %}
        metrics as (
            {{ elementary.get_unified_metrics_query(metrics=table_monitors, metric_args=metric_args) }}
        ),

        metrics_final as (

        select
            {{ elementary.cast_as_string(full_table_name_str) }} as full_table_name,
            {{ elementary.null_string() }} as column_name,
            metric_name,
            {{ elementary.cast_as_float('metric_value') }} as metric_value,
            {{ elementary.null_string() }} as source_value,
            {{ elementary.null_timestamp() }} as bucket_start,
            {{ elementary.cast_as_timestamp(elementary.quote(elementary.get_max_bucket_end())) }} as bucket_end,
            {{ elementary.null_int() }} as bucket_duration_hours,
            {{ elementary.null_string() }} as dimension,
            {{ elementary.null_string() }} as dimension_value
        from metrics

        )
    {% endif %}

    select
        {{ elementary.generate_surrogate_key([
            'full_table_name',
            'column_name',
            'metric_name',
            'bucket_end'
        ]) }} as id,
        full_table_name,
        column_name,
        metric_name,
        metric_value,
        source_value,
        bucket_start,
        bucket_end,
        bucket_duration_hours,
        {{ elementary.current_timestamp_in_utc() }} as updated_at,
        dimension,
        dimension_value
    from metrics_final

{% endmacro %}

{% macro get_unified_metrics_query(metrics, metric_args, timestamp_column=none) %}
    {%- set included_monitors = {} %}
    {%- for metric_name in metrics %}
        {%- set metric_query = elementary.get_metric_query(metric_name, metric_args, timestamp_column=timestamp_column) %}
        {%- if metric_query %}
            {% do included_monitors.update({metric_name: metric_query}) %}
        {%- endif %}
    {%- endfor %}

    {% if not included_monitors %}
        {% if timestamp_column %}
            {% do return(elementary.empty_table([('edr_bucket_start','timestamp'),('edr_bucket_end','timestamp'),('metric_name','string'),('source_value','string'),('metric_value','int')])) %}
        {% else %}
            {% do return(elementary.empty_table([('metric_name','string'),('metric_value','int')])) %}
        {% endif %}
    {% endif %}

    with
    {%- for metric_name, metric_query in included_monitors.items() %}
        {{ metric_name }} as (
            {{ metric_query }}
        ){% if not loop.last %},{% endif %}
    {%- endfor %}

    {%- for metric_name in included_monitors %}
    select * from {{ metric_name }}
    {% if not loop.last %} union all {% endif %}
    {%- endfor %}
{% endmacro %}

{% macro get_metric_query(metric_name, metric_args, timestamp_column) %}
    {%- set metrics_macro_mapping = {
        "row_count": elementary.row_count_metric_query,
        "freshness": elementary.freshness_metric_query,
        "event_freshness": elementary.event_freshness_metric_query
    } %}

    {%- set metric_macro = metrics_macro_mapping.get(metric_name) %}
    {%- if not metric_macro %}
        {%- do return(none) %}
    {%- endif %}

    {%- set metric_query = metric_macro(metric_args, timestamp_column=timestamp_column) %}
    {%- if not metric_query %}
        {%- do return(none) %}
    {%- endif %}

    {{ metric_query }}
{% endmacro %}

{% macro row_count_metric_query(metric_args, timestamp_column=none) %}
{% if timestamp_column %}
    with row_count_values as (
        select edr_bucket_start,
               edr_bucket_end,
               start_bucket_in_data,
               case when start_bucket_in_data is null then
                   0
               else {{ elementary.cast_as_float(elementary.row_count()) }} end as row_count_value
        from buckets left join time_filtered_monitored_table on (edr_bucket_start = start_bucket_in_data)
        group by 1,2,3
    )

    select edr_bucket_start,
           edr_bucket_end,
           {{ elementary.const_as_string('row_count') }} as metric_name,
           {{ elementary.null_string() }} as source_value,
           row_count_value as metric_value
    from row_count_values
{% else %}
    select
        {{ elementary.const_as_string('row_count') }} as metric_name,
        {{ elementary.row_count() }} as metric_value
    from monitored_table
    group by 1
{% endif %}
{% endmacro %}

{% macro freshness_metric_query(metric_args, timestamp_column=none) %}
{% if timestamp_column %}
    {%- set freshness_column = metric_args.freshness_column %}
    {%- if freshness_column is undefined or freshness_column is none %}
        {%- set freshness_column = timestamp_column %}
    {%- endif %}

    -- get ordered consecutive update timestamps in the source data
    with unique_timestamps as (
        select distinct {{ elementary.cast_as_timestamp(freshness_column) }} as timestamp_val
        from monitored_table
        order by 1
    ),

    consecutive_updates as (
        select
            timestamp_val as update_timestamp,
            lag(timestamp_val) over (order by timestamp_val) as prev_timestamp,
        from unique_timestamps
        where timestamp_val >= (select min(edr_bucket_start) from buckets)
    ),

    -- compute freshness for every update as the time difference from the previous update
    consecutive_updates_freshness as (
        select
            update_timestamp,
            {{ elementary.timediff('second', 'prev_timestamp', 'update_timestamp') }} as freshness
        from consecutive_updates
    ),

    -- divide the freshness metrics above to buckets
    bucketed_consecutive_updates_freshness as (
        select
            edr_bucket_start, edr_bucket_end, update_timestamp, freshness
        from buckets, consecutive_updates_freshness
        where update_timestamp >= edr_bucket_start AND update_timestamp < edr_bucket_end
    ),

    -- we also want to record the freshness at the end of each bucket as an additional point (as it might be larger,
    -- or even the only point if no data was updated at all in a particular bucket)
    bucket_end_freshness as (
        select
            edr_bucket_start,
            edr_bucket_end,
            max(timestamp_val) as update_timestamp,
            {{ elementary.timediff('second', elementary.cast_as_timestamp('max(timestamp_val)'), "least(edr_bucket_end, {})".format(elementary.current_timestamp_column())) }} as freshness
        from buckets, unique_timestamps
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
            rank () over (partition by edr_bucket_end order by freshness desc) as row_number
        from bucket_all_freshness_metrics
    )

    select
        edr_bucket_start,
        edr_bucket_end,
        {{ elementary.const_as_string('freshness') }} as metric_name,
        {{ elementary.cast_as_string('update_timestamp') }} as source_value,
        freshness as metric_value
    from bucket_freshness_ranked
    where row_number = 1
{% else %}
    {# Update freshness test not supported when timestamp column is not provided #}
    {# TODO: We can enhance this test for models to use model_run_results in case a timestamp column is not defined #}
    {% do return(none) %}
{% endif %}
{% endmacro %}

{% macro event_freshness_metric_query(metric_args, timestamp_column) %}
{% set event_timestamp_column = metric_args.event_timestamp_column %}
{% set update_timestamp_column = timestamp_column %}

{% if update_timestamp_column %}
    select
        edr_bucket_start,
        edr_bucket_end,
        {{ elementary.const_as_string('event_freshness') }} as metric_name,
        {{ elementary.cast_as_string('max({})'.format(event_timestamp_column)) }} as source_value,
        {{ 'coalesce(max({}), {})'.format(
                elementary.timediff('second', elementary.cast_as_timestamp(event_timestamp_column), elementary.cast_as_timestamp(update_timestamp_column)),
                elementary.timediff('second', 'edr_bucket_start', 'edr_bucket_end')
            ) }} as metric_value
    from buckets left join time_filtered_monitored_table on (edr_bucket_start = start_bucket_in_data)
    group by 1,2
{% else %}
    select
        {{ elementary.const_as_string('event_freshness') }} as metric_name,
        {{ elementary.timediff('second', elementary.cast_as_timestamp("max({})".format(event_timestamp_column)), elementary.current_timestamp_column()) }} as metric_value
    from monitored_table
    group by 1
{% endif %}
{% endmacro %}
