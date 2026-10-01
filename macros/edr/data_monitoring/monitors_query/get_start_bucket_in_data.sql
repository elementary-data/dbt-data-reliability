{% macro get_start_bucket_in_data(timestamp_column, min_bucket_start, time_bucket) %}
    {#- Count weeks as 7 days: where a week starts in datediff depends on the
        adapter (Sunday on Postgres), while buckets start on min_bucket_start. -#}
    {% if time_bucket.period | lower == "week" %}
        {% set time_bucket = {"period": "day", "count": time_bucket.count * 7} %}
    {% endif %}
    {% set bucket_start_datediff_expr %}
      floor({{ elementary.edr_datediff(min_bucket_start, elementary.edr_cast_as_timestamp(timestamp_column), time_bucket.period) }} / {{ time_bucket.count }}) * {{ time_bucket.count }}
    {% endset %}
    {% do return(
        elementary.edr_cast_as_timestamp(
            elementary.edr_timeadd(
                time_bucket.period,
                elementary.edr_cast_as_int(bucket_start_datediff_expr),
                min_bucket_start,
            )
        )
    ) %}
{% endmacro %}
