{% macro get_detection_end(detection_delay) %}
    {% if not detection_delay %}
        {% do return(elementary.get_run_started_at().replace(microsecond=0)) %}
    {% endif %}

    {%- set kwargs = {detection_delay.period+'s': detection_delay.count} %}
    {%- set detection_end = elementary.get_run_started_at().replace(microsecond=0) - modules.datetime.timedelta(**kwargs) %}
    {% do return(detection_end) %}
{% endmacro %}

{% macro get_trunc_min_bucket_start_expr(detection_end, metric_properties, days_back) %}
    {%- set untruncated_min = (detection_end - modules.datetime.timedelta(days_back | int)).strftime("%Y-%m-%d 00:00:00") %}
    {%- set trunc_min_bucket_start_expr = elementary.edr_date_trunc(metric_properties.time_bucket.period, elementary.edr_cast_as_timestamp(elementary.edr_quote(untruncated_min)))%}
    {{ return(trunc_min_bucket_start_expr) }}
{% endmacro %}

{# This macro cant be used without truncating to full buckets #}
{% macro get_backfill_bucket_start(detection_end, backfill_days) %}
    {% do return((detection_end - modules.datetime.timedelta(backfill_days)).strftime("%Y-%m-%d 00:00:00")) %}
{% endmacro %}


{% macro get_metric_buckets_min_and_max(model_relation, backfill_days, days_back, detection_delay=none, metric_names=none, column_name=none, metric_properties=none, unit_test=false, unit_test_relation=none) %}

    {%- set detection_end = elementary.get_detection_end(detection_delay) %}
    {%- set detection_end_expr = elementary.edr_cast_as_timestamp(elementary.edr_datetime_to_sql(detection_end)) %}
    {%- set trunc_min_bucket_start_expr = elementary.get_trunc_min_bucket_start_expr(detection_end, metric_properties, days_back) %}
    {%- set backfill_bucket_start = elementary.edr_cast_as_timestamp(elementary.edr_datetime_to_sql(elementary.get_backfill_bucket_start(detection_end, backfill_days))) %}
    {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}
    {%- set force_metrics_backfill = elementary.get_config_var('force_metrics_backfill') %}

    {%- if metric_names %}
        {%- set metric_names_tuple = elementary.strings_list_to_tuple(metric_names) %}
    {%- endif %}

    {%- if unit_test %}
        {%- set data_monitoring_metrics_relation = dbt.load_relation(unit_test_relation) %}
    {%- else %}
        {%- set data_monitoring_metrics_relation = elementary.get_elementary_relation('data_monitoring_metrics') %}
    {%- endif %}
    {%- set regular_bucket_times_query %}
        with bucket_times as (
            select
            {{ trunc_min_bucket_start_expr }} as days_back_start
           , {{ detection_end_expr }} as detection_end
        ),
        full_buckets_calc as (
            select *,
                floor({{ elementary.edr_datediff('days_back_start', 'detection_end', metric_properties.time_bucket.period) }} / {{ metric_properties.time_bucket.count }}) * {{ metric_properties.time_bucket.count }} as periods_until_max
            from bucket_times
        )
        select
             days_back_start as min_bucket_start,
             {{ elementary.edr_timeadd(metric_properties.time_bucket.period, 'periods_until_max', 'days_back_start') }} {# Add full buckets to last_max_bucket_end #}
        as max_bucket_end
        from full_buckets_calc
    {%- endset %}

    {%- set incremental_bucket_times_query %}
        with all_buckets as (
            select edr_bucket_start as bucket_start, edr_bucket_end as bucket_end
            from ({{ elementary.complete_buckets_cte(metric_properties, trunc_min_bucket_start_expr, detection_end_expr) }}) results
            where edr_bucket_start >= {{ trunc_min_bucket_start_expr }}
            and edr_bucket_end <= {{ detection_end_expr }}
        ),
        buckets_with_existing_metrics as (
            select distinct bucket_start, bucket_end
            from {{ data_monitoring_metrics_relation }}
            where bucket_start >= {{ trunc_min_bucket_start_expr }}
            and bucket_end <= {{ detection_end_expr }}
            and upper(full_table_name) = upper('{{ full_table_name }}')
            and metric_properties = {{ elementary.dict_to_quoted_json(metric_properties) }}
            {%- if metric_names %}
            and metric_name in {{ metric_names_tuple }}
            {%- endif %}
            {%- if column_name %}
            and upper(column_name) = upper('{{ column_name }}')
            {%- endif %}
        ),
        missing_bucket_starts as (
            select all_buckets.bucket_start
            from all_buckets
            left outer join buckets_with_existing_metrics existing on (
                existing.bucket_start = all_buckets.bucket_start and
                existing.bucket_end = all_buckets.bucket_end
            )
            where existing.bucket_start is NULL
        ),
        min_bucket_start_candidates as (
            select bucket_start from missing_bucket_starts
            union all
            select {{ backfill_bucket_start }} as bucket_start
        )
        select
            min(bucket_start) as min_bucket_start,
            {{ detection_end_expr }} as max_bucket_end
        from min_bucket_start_candidates
    {% endset %}

    {# We assume we should also cosider sources as incremental #}
    {% if force_metrics_backfill or not (elementary.is_incremental_model(elementary.get_model_graph_node(model_relation), source_included=true) or unit_test) %}
 
        {%- set buckets = elementary.agate_to_dicts(elementary.run_query(regular_bucket_times_query))[0] %}
    {%- else %}
        {%- set buckets = elementary.agate_to_dicts(elementary.run_query(incremental_bucket_times_query))[0] %}
    {% endif %}
    {%- if buckets %}
        {%- set min_bucket_start = elementary.edr_datetime_to_sql(buckets.get('min_bucket_start')) %}
        {%- set max_bucket_end = elementary.edr_datetime_to_sql(buckets.get('max_bucket_end')) %}
        {{ return([min_bucket_start, max_bucket_end]) }}
    {%- else %}
        {{ exceptions.raise_compiler_error("Failed to calc test buckets min and max") }}
    {%- endif %}

{% endmacro %}
