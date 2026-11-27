{% macro truncate_to_microseconds(value) %}
    {% if value is not string %} {% do return(value) %} {% endif %}
    {% set match = modules.re.search("^(.*\.\d{6})\d+(.*)$", value) %}
    {% if match %} {% do return(match.group(1) ~ match.group(2)) %} {% endif %}
    {% do return(value) %}
{% endmacro %}
