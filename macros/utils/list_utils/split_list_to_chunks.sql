{% macro split_list_to_chunks(item_list, chunk_size=50) %}
    {% do elementary.begin_duration_measure_context('split_list_to_chunks') %}

    {% set chunks = [] %}
    {% set current_chunk = [] %}
    {% for item in item_list %}
        {% set reminder = loop.index0 % chunk_size %}
        {% if reminder == 0 and current_chunk %}
            {% do chunks.append(current_chunk.copy()) %}
            {% do current_chunk.clear() %}
        {% endif %}
        {% do current_chunk.append(item) %}
    {% endfor %}
    {% if current_chunk %}
        {% do chunks.append(current_chunk) %}
    {% endif %}

    {% do elementary.end_duration_measure_context('split_list_to_chunks') %}
    {{ return(chunks) }}
{% endmacro %}
