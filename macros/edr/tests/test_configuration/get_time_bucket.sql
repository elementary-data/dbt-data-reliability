{% macro get_daily_time_bucket() %}
  {% do return({"period": "day", "count": 1}) %}
{% endmacro %}

{% macro get_default_time_bucket() %}
  {% do return(elementary.get_daily_time_bucket()) %}
{% endmacro %}

{% macro get_time_bucket(time_bucket, model_graph_node) %}
    {%- set configured_time_bucket = elementary.get_test_argument('time_bucket', time_bucket, model_graph_node) %}
    {%- do elementary.validate_time_bucket(configured_time_bucket) %}
    {%- set default_time_bucket = elementary.get_default_time_bucket() %}

    {%- if not configured_time_bucket %}
        {{ return(default_time_bucket) }}
    {%- else %}
        {%- set time_bucket = default_time_bucket.copy() %}
        {%- do time_bucket.update(configured_time_bucket) %}
        {{ return(time_bucket) }}
    {%- endif %}
{% endmacro %}

{% macro validate_time_bucket(time_bucket) %}
    {% if time_bucket %}
        {%- if time_bucket is not mapping %}
            {% do exceptions.raise_compiler_error(
            "
            Invalid time_bucket format. Expected format:

               time_bucket:
                 count: int
                 period: string
            ") %}
        {%- endif %}
        {%- if time_bucket is mapping %}
            {%- set invalid_keys = [] %}
            {%- set valid_keys = ['period', 'count'] %}
            {%- for key, value in time_bucket.items() %}
                {%- if key not in valid_keys %}
                    {%- do invalid_keys.append(key) -%}
                {%- endif %}
            {%- endfor %}
            {%- if invalid_keys | length > 0 %}
                {% do exceptions.raise_compiler_error(
                ("
                Found invalid keys in time_bucket: {0}.
                Supported keys: {1}.
                Expected format:

                   time_bucket:
                     count: int
                     period: string
                ").format(invalid_keys, valid_keys)) %}
            {%- endif %}
        {%- endif %}

        {% if time_bucket.count and time_bucket.count is not integer %}
            {% do exceptions.raise_compiler_error("time_bucket.count expects valid integer, got: {} (If it's an integer, try to remove quotes)".format(time_bucket.count)) %}
        {% endif %}
        {% if time_bucket.count <= 0 %}
            {% do exceptions.raise_compiler_error("time_bucket.count has to be larger than 0, got: {})".format(time_bucket.count)) %}
        {% endif %}
        {% set supported_periods = adapter.dispatch("supported_periods", "elementary")() %}
        {% if time_bucket.period and time_bucket.period not in supported_periods %}
            {% do exceptions.raise_compiler_error("time_bucket.period value should be one of {0}, got: {1}".format(supported_periods, time_bucket.period)) %}
        {% endif %}
    {% endif %}
{% endmacro %}


{% macro default__supported_periods() %}
  {% do return(["hour", "day", "week", "month"]) %}
{% endmacro %}


{% macro redshift__supported_periods() %}
  {% do return(["hour", "day", "week"]) %}
{% endmacro %}
