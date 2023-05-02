{% macro tuple_to_list_of_size(tup, len) %}
    {% set padded_list = tup | list %}
    {% set list_len = padded_list | length %}
    {% set amount_of_padded = len - list_len if len > list_len else 0 %}
    {% for _ in range(0, amount_of_padded) %}
        {% do padded_list.append(None) %}
    {% endfor %}
    {% set result = padded_list[:len] %}
    {{ return(result) }}
{% endmacro %}
