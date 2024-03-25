{% macro split_list_to_chunks(item_list, chunk_size=50) %}
    {% do elementary.begin_duration_measure_context('split_list_to_chunks') %}

    {% set chunks = [] %}
    {% set current_chunk = [] %}
    {% set current_length = 0 %}
    {% for item in item_list %}
        {% set reminder = loop.index0 % chunk_size %}
        {% if (reminder == 0 and current_chunk) or (current_length + item | length) > elementary.get_config_var('query_max_size') %}
            {% do chunks.append(current_chunk.copy()) %}
            {% do current_chunk.clear() %}
            {% set current_length = 0 %}
        {% endif %}
        {% do current_chunk.append(item) %}
        {% do current_length + item | length %}
    {% endfor %}
    {% if current_chunk %}
        {% do chunks.append(current_chunk) %}
    {% endif %}

    {% do elementary.end_duration_measure_context('split_list_to_chunks') %}
    {{ return(chunks) }}
{% endmacro %}
