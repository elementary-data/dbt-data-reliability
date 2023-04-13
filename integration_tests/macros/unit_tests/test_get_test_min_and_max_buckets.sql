{% macro test_get_test_buckets_min_and_max() %}

    {%- do drop_unit_test_table('monitors_runs_unit_test') -%}
    {%- do drop_unit_test_table('unit_test_table') -%}

    {%- set monitors_runs_schema = [('full_table_name', 'string'), ('metric_properties', 'string'), ('last_bucket_end', 'timestamp'), ('first_bucket_end', 'timestamp')] %}
    {%- set monitors_runs_relation = create_unit_test_table('monitors_runs_unit_test', monitors_runs_schema, temp=false) %}

    {%- set unit_test_table_schema = [('test_column1', 'string'), ('test_column2', 'int')] %}
    {%- set unit_test_relation = create_unit_test_table('unit_test_table', unit_test_table_schema, temp=false) %}
    {%- set full_table_name = elementary.relation_to_full_name(unit_test_relation) %}

    {# Test incremental buckets logic #}
    {# No previous run, daily buckets #}
    {%- set days_back = 100 %}
    {%- set backfill_days = 2 %}

    {%- set metric_properties = {'time_bucket' : {'count': 1, 'period': 'day'}} %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'days') / metric_properties.time_bucket.count %}
    {{ assert_value(total_buckets, 100.0) }}
    {{ assert_round_time(min_bucket_start,'days') }}
    {{ assert_round_time(max_bucket_end,'days') }}

    {# Previous run smaller than backfill, daily buckets #}
    {% set dicts = [{'full_table_name': full_table_name, 'metric_properties': metric_properties, 'last_bucket_end': get_x_time_ago(3,'days'), 'first_bucket_end': get_x_time_ago(200,'days')}] %}
    {% do elementary.insert_rows(monitors_runs_relation, dicts) %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'days') / metric_properties.time_bucket.count %}
    {{ assert_value(total_buckets, 3.0) }}
    {{ assert_round_time(min_bucket_start,'days') }}
    {{ assert_round_time(max_bucket_end,'days') }}

    {# Previous larger than backfill, daily buckets #}
    {%- do drop_unit_test_table('monitors_runs_unit_test') -%}
    {%- set monitors_runs_relation = create_unit_test_table('monitors_runs_unit_test', monitors_runs_schema, temp=false) %}
    {% set dicts = [{'full_table_name': full_table_name, 'metric_properties': metric_properties, 'last_bucket_end': get_x_time_ago(1,'days'), 'first_bucket_end': get_x_time_ago(200,'days')}] %}
    {% do elementary.insert_rows(monitors_runs_relation, dicts) %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'days') / metric_properties.time_bucket.count %}
    {{ assert_value(total_buckets, 2.0) }}
    {{ assert_round_time(min_bucket_start,'days') }}
    {{ assert_round_time(max_bucket_end,'days') }}

    {# No previous run, 5 days buckets #}
    {%- set metric_properties = {'time_bucket' : {'count': 5, 'period': 'day'}} %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'days') / metric_properties.time_bucket.count %}
    {{ assert_value(total_buckets, 20.0) }}
    {{ assert_round_time(min_bucket_start,'days') }}
    {{ assert_round_time(max_bucket_end,'days') }}

    {# Previous run smaller than backfill, 5 days buckets #}
    {% set dicts = [{'full_table_name': full_table_name, 'metric_properties': metric_properties, 'last_bucket_end': get_x_time_ago(5,'days'), 'first_bucket_end': get_x_time_ago(200,'days')}] %}
    {% do elementary.insert_rows(monitors_runs_relation, dicts) %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'days') / metric_properties.time_bucket.count %}
    {{ assert_value(total_buckets, 1.0) }}
    {{ assert_round_time(min_bucket_start,'days') }}
    {{ assert_round_time(max_bucket_end,'days') }}

    {# No previous run, hourly buckets #}
    {%- set days_back = 14 %}
    {%- set backfill_days = 2 %}

    {%- set metric_properties = {'time_bucket' : {'count': 1, 'period': 'hour'}} %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'hours') / metric_properties.time_bucket.count %}
    {%- set hours_since_run_start = elementary.get_run_started_at().strftime("%H") | int %}
    {{ assert_value(total_buckets, 336.0 + hours_since_run_start) }}
    {{ assert_round_time(min_bucket_start,'hours') }}
    {{ assert_round_time(max_bucket_end,'hours') }}

    {# Last run larger than backfill, hourly buckets #}
    {% set dicts = [{'full_table_name': full_table_name, 'metric_properties': metric_properties, 'last_bucket_end': get_x_time_ago(1,'days'), 'first_bucket_end': get_x_time_ago(200,'days')}] %}
    {% do elementary.insert_rows(monitors_runs_relation, dicts) %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'hours') / metric_properties.time_bucket.count %}
    {%- set hours_since_run_start = elementary.get_run_started_at().strftime("%H") | int %}
    {{ assert_value(total_buckets, 48.0 + hours_since_run_start) }}
    {{ assert_round_time(min_bucket_start,'hours') }}
    {{ assert_round_time(max_bucket_end,'hours') }}

    {# No previous run, buckets 12 hours #}
    {%- set days_back = 14 %}
    {%- set backfill_days = 2 %}

    {%- set metric_properties = {'time_bucket' : {'count': 12, 'period': 'hour'}} %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'hours') / metric_properties.time_bucket.count %}
    {%- set hours_since_run_start = elementary.get_run_started_at().strftime("%H") | int %}
    {%- if hours_since_run_start > 12 %}
        {{ assert_value(total_buckets, 29.0 ) }}
    {%- else %}
        {{ assert_value(total_buckets, 28.0 ) }}
    {%- endif %}
    {{ assert_round_time(min_bucket_start,'hours') }}
    {{ assert_round_time(max_bucket_end,'hours') }}

    {# Last run smaller than backfill, 12 hours buckets #}
    {% set dicts = [{'full_table_name': full_table_name, 'metric_properties': metric_properties, 'last_bucket_end': get_x_time_ago(3,'days'), 'first_bucket_end': get_x_time_ago(200,'days')}] %}
    {% do elementary.insert_rows(monitors_runs_relation, dicts) %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'hours') / metric_properties.time_bucket.count %}
    {%- set hours_since_run_start = elementary.get_run_started_at().strftime("%H") | int %}
    {%- if hours_since_run_start > 12 %}
        {{ assert_value(total_buckets, 7.0 ) }}
    {%- else %}
        {{ assert_value(total_buckets, 6.0 ) }}
    {%- endif %}
    {{ assert_round_time(min_bucket_start,'hours') }}
    {{ assert_round_time(max_bucket_end,'hours') }}

    {# No previous run, 1 week buckets #}
    {%- set days_back = 210 %}
    {%- set backfill_days = 14 %}

    {%- set metric_properties = {'time_bucket' : {'count': 1, 'period': 'week'}} %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'weeks') / metric_properties.time_bucket.count %}
    {{ assert_value(total_buckets, 30.0 ) }}
    {{ assert_round_time(min_bucket_start,'weeks') }}
    {{ assert_round_time(max_bucket_end,'weeks') }}

    {# First run smaller than days back, 1 week buckets #}
    {% set dicts = [{'full_table_name': full_table_name, 'metric_properties': metric_properties, 'last_bucket_end': get_x_time_ago(1,'weeks'), 'first_bucket_end': get_x_time_ago(10,'weeks')}] %}
    {% do elementary.insert_rows(monitors_runs_relation, dicts) %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'weeks') / metric_properties.time_bucket.count %}
    {{ assert_value(total_buckets, 30.0 ) }}
    {{ assert_round_time(min_bucket_start,'weeks') }}
    {{ assert_round_time(max_bucket_end,'weeks') }}

    {# No previous run, 1 month buckets #}
    {%- set days_back = 300 %}
    {%- set backfill_days = 14 %}

    {%- set metric_properties = {'time_bucket' : {'count': 1, 'period': 'month'}} %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'months') / metric_properties.time_bucket.count %}
    {{ assert_value(total_buckets, 10.0 ) }}
    {{ assert_round_time(min_bucket_start,'months') }}
    {{ assert_round_time(max_bucket_end,'months') }}

    {# Last run is earlier than days_back, 1 month buckets #}
    {% set dicts = [{'full_table_name': full_table_name, 'metric_properties': metric_properties, 'last_bucket_end': get_x_time_ago(500,'weeks'), 'first_bucket_end': get_x_time_ago(1000,'weeks')}] %}
    {% do elementary.insert_rows(monitors_runs_relation, dicts) %}
    {%- set min_bucket_start, max_bucket_end = elementary.get_test_buckets_min_and_max(model_relation=unit_test_relation,
                                                           backfill_days=backfill_days,
                                                           days_back=days_back,
                                                           metric_properties=metric_properties,
                                                           unit_test=true,
                                                           unit_test_relation=monitors_runs_relation) %}

    {%- set total_buckets = time_diff(min_bucket_start, max_bucket_end, 'months') / metric_properties.time_bucket.count %}
    {{ assert_value(total_buckets, 10.0 ) }}
    {{ assert_round_time(min_bucket_start,'months') }}
    {{ assert_round_time(max_bucket_end,'months') }}

    {%- do drop_unit_test_table('monitors_runs_unit_test') -%}
    {%- do drop_unit_test_table('unit_test_table') -%}

{% endmacro %}


{% macro time_diff(min_time, max_time, unit) %}

    {%- set min_time = min_time | replace("'", "") | replace("+00:00", "") %}
    {%- set max_time = max_time | replace("'", "") | replace("+00:00", "") %}
    {%- set start_time = modules.datetime.datetime.strptime(min_time, "%Y-%m-%dT%H:%M:%S") %}
    {%- set end_time = modules.datetime.datetime.strptime(max_time, "%Y-%m-%dT%H:%M:%S") %}
    {%- set delta = end_time - start_time %}

    {%- if unit == 'hours' %}
        {{ return(delta.total_seconds() / (60*60)) }}
    {%- elif unit == 'days' %}
        {{ return(delta.total_seconds() / (60*60*24)) }}
    {%- elif unit == 'weeks' %}
        {{ return(delta.total_seconds() / (60*60*24*7)) }}
    {%- elif unit == 'months' %}
        {%- set months = (end_time.year - start_time.year) * 12 + (end_time.month - start_time.month) %}
        {{ return(months) }}
    {%- endif %}
{% endmacro %}

{% macro assert_round_time(datetime, unit) %}
    {%- set datetime = datetime | replace("'", "") | replace("+00:00", "") %}
    {%- if unit == 'hours' %}
       {%- set time_string = modules.datetime.datetime.strptime(datetime, "%Y-%m-%dT%H:%M:%S").strftime("%M:%S") %}
       {{ assert_value(time_string, '00:00') }}
    {%- elif unit == 'days' %}
       {%- set time_string = modules.datetime.datetime.strptime(datetime, "%Y-%m-%dT%H:%M:%S").strftime("%H:%M:%S") %}
       {{ assert_value(time_string, '00:00:00') }}
    {%- elif unit == 'weeks' %}
        {%- set time_string = modules.datetime.datetime.strptime(datetime, "%Y-%m-%dT%H:%M:%S").strftime("%w %H:%M:%S") %}
        {%- if target.type == 'bigquery'%}
            {{ assert_value(time_string, '0 00:00:00') }}
        {%- else %}
            {{ assert_value(time_string, '1 00:00:00') }}
        {%- endif %}
    {%- elif unit == 'months' %}
        {%- set time_string = modules.datetime.datetime.strptime(datetime, "%Y-%m-%dT%H:%M:%S").strftime("%d %H:%M:%S") %}
        {{ assert_value(time_string, '01 00:00:00') }}
    {%- endif %}
{% endmacro %}

{% macro get_x_time_ago(x, unit) %}
    {%- if unit == 'hours' %}
        {%- set x_time_ago = (elementary.get_run_started_at() - modules.datetime.timedelta(hours=x)).strftime("%Y-%m-%d 00:00:00") %}
    {%- elif unit == 'days' %}
        {%- set x_time_ago = (elementary.get_run_started_at() - modules.datetime.timedelta(days=x)).strftime("%Y-%m-%d 00:00:00") %}
    {%- elif unit == 'weeks' %}
        {%- set x_time_ago = (elementary.get_run_started_at() - modules.datetime.timedelta(weeks=x)).strftime("%Y-%m-%d 00:00:00") %}
    {%- endif %}
    {{ return(x_time_ago) }}
{% endmacro %}