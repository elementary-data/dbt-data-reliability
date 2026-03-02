{% macro get_no_detection_delay() %}
  {% do return({"period": "hour", "count": 0}) %}
{% endmacro %}

{% macro get_default_detection_delay() %}
  {% do return(elementary.get_no_detection_delay()) %}
{% endmacro %}

{% macro get_detection_delay_supported_periods() %}
  {% do return(["hour", "day", "week"]) %}
{% endmacro %}

{% macro get_detection_delay(detection_delay, model_graph_node) %}
    {%- set configured_detection_delay = elementary.get_test_argument('detection_delay', detection_delay, model_graph_node) %}
    {%- do elementary.validate_detection_delay(configured_detection_delay) %}
    {%- set default_detection_delay = elementary.get_default_detection_delay() %}

    {%- if not configured_detection_delay %}
        {{ return(default_detection_delay) }}
    {%- else %}
        {%- set detection_delay = default_detection_delay.copy() %}
        {%- do detection_delay.update(configured_detection_delay) %}
        {{ return(detection_delay) }}
    {%- endif %}
{% endmacro %}

{% macro validate_detection_delay(detection_delay) %}
    {% if detection_delay %}
        {%- if detection_delay is not mapping %}
            {% do exceptions.raise_compiler_error(
            "
            Invalid detection_delay format. Expected format:

               detection_delay:
                 count: int
                 period: string
            ") %}
        {%- endif %}
        {%- if detection_delay is mapping %}
            {%- set invalid_keys = [] %}
            {%- set valid_keys = ['period', 'count'] %}
            {%- for key, value in detection_delay.items() %}
                {%- if key not in valid_keys %}
                    {%- do invalid_keys.append(key) -%}
                {%- endif %}
            {%- endfor %}
            {%- if invalid_keys | length > 0 %}
                {% do exceptions.raise_compiler_error(
                ("
                Found invalid keys in detection_delay: {0}.
                Supported keys: {1}.
                Expected format:

                   detection_delay:
                     count: int
                     period: string
                ").format(invalid_keys, valid_keys)) %}
            {%- endif %}
        {%- endif %}

        {% if detection_delay.count and detection_delay.count is not integer %}
            {% do exceptions.raise_compiler_error("detection_delay.count expects valid integer, got: {} (If it's an integer, try to remove quotes)".format(detection_delay.count)) %}
        {% endif %}
        {% if detection_delay.count < 0 %}
            {% do exceptions.raise_compiler_error("detection_delay.count can't be negative, got: {})".format(detection_delay.count)) %}
        {% endif %}
        {% set supported_periods = elementary.get_detection_delay_supported_periods() %}
        {% if detection_delay.period and detection_delay.period not in supported_periods %}
            {% do exceptions.raise_compiler_error("detection_delay.period value should be one of {0}, got: {1}".format(supported_periods, detection_delay.period)) %}
        {% endif %}
    {% endif %}
{% endmacro %}
