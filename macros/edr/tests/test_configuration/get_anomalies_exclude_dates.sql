{% macro get_anomalies_exclude_dates(anomalies_exclude_dates, model_graph_node) %}
    {%- set configured_anomalies_exclude_dates = elementary.get_test_argument('anomalies_exclude_dates', anomalies_exclude_dates, model_graph_node) %}
    {%- do elementary.validate_anomalies_exclude_dates(configured_anomalies_exclude_dates) %}
    {%- do return(configured_anomalies_exclude_dates) %}
{% endmacro %}

{% macro validate_anomalies_exclude_dates(anomalies_exclude_dates) %}
    {% if anomalies_exclude_dates is not sequence %}
        {% do elementary.raise_anomalies_exclusion_dates_invalid_format_error() %}
    {% endif %}

    {% set dates_in_config = [] %}
    {% for value in anomalies_exclude_dates %}
        {% if value is string %}
            {% do dates_in_config.append(value) %}
        {% elif value is mapping %}
            {# Don't allow ranges without "before" #}
            {% if "before" not in value %}
                {% do elementary.raise_anomalies_exclusion_dates_invalid_format_error() %}
            {% endif %}

            {# Make sure "after" and "before" are the only allowed keys #}
            {% for key, val in value.items() %}
                {% if key not in ["after", "before"] %}
                    {% do elementary.raise_anomalies_exclusion_dates_invalid_format_error() %}
                {% endif %}
                {% do dates_in_config.append(val) %}
            {% endfor %}
        {% else %}
            {% do elementary.raise_anomalies_exclusion_dates_invalid_format_error() %}
        {% endif %}
    {% endfor %}

    {% for cur_date in dates_in_config %}
        {% if not modules.re.match("^\d{4}-\d{2}-\d{2}$", cur_date) %}
            {% do elementary.raise_anomalies_exclusion_dates_invalid_format_error() %}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro raise_anomalies_exclusion_dates_invalid_format_error() %}
    {% do exceptions.raise_compiler_error(
    "
Invalid detection_delay format. Expected format is a list of either dates or date ranges. Below are
examples of the supported options:

   anomalies_exclude_dates:
     - 2023-10-01                   # Specific date
     - after: 2023-10-03            # Date range between two dates (will exclude October 3rd and 4th)
       before: 2023-10-05
     - before: 2023-10-03           # All data before a specific date (not including October 3rd)
    ".strip()) %}
{% endmacro %}