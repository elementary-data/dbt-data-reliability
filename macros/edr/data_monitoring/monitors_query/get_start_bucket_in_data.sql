{% macro get_start_bucket_in_data(timestamp_column, min_bucket_start, time_bucket) %}
    {% set bucket_start_datediff_expr %}
      floor({{ elementary.datediff(min_bucket_start, elementary.cast_as_timestamp(timestamp_column), time_bucket.period) }} / {{ time_bucket.count }}) * {{ time_bucket.count }}
    {% endset %}
    {% do return(elementary.cast_as_timestamp(elementary.dateadd(time_bucket.period, elementary.cast_as_int(bucket_start_datediff_expr), min_bucket_start))) %}
{% endmacro %}
