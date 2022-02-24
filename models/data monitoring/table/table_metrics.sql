-- convert to loop per table

{% set table_to_monitor = var('table_to_monitor') %}
{% set timestamp_field = var('timestamp_field') %}
{% set days_back = var('days_back') %}

{{ table_metrics_query(table_to_monitor, timestamp_field, days_back, 24, ['row_count'], [{'column_name': 'total_elapsed_time', 'monitors': ['min','max']}], true) }}
