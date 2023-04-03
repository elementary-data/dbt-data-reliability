{% macro filter_none_and_sort(val) %}
    {% do return(val | reject("none") | sort) %}
{% endmacro %}
