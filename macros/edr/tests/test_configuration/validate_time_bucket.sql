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
                {%- if key not in ['period', 'count'] %}
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
            {% do exceptions.raise_compiler_error("time_bucket.count expectes valid integer, got: {} (If it's an integer, try to remove quotes)".format(time_bucket.count)) %}
        {% endif %}
        {% set supported_periods = ['hour','day','week','month'] %}
        {% if time_bucket.period and time_bucket.period not in supported_periods %}
            {% do exceptions.raise_compiler_error("time_bucket.period value should be one of {0}, got: {1}".format(supported_periods, time_bucket.period)) %}
        {% endif %}
    {% endif %}
{% endmacro %}