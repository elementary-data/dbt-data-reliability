{% macro split_list_to_chunks(item_list, chunk_size=10000) %}
    {% set chunks = [] %}
    {% set current_chunk = [] %}
    {% for item in item_list %}
        {% set remainder = loop.index0 % chunk_size %}
        {% if remainder == 0 and current_chunk %}
            {% do chunks.append(current_chunk.copy()) %}
            {% do current_chunk.clear() %}
        {% endif %}
        {% do current_chunk.append(item) %}
    {% endfor %}
    {% if current_chunk %}
        {% do chunks.append(current_chunk) %}
    {% endif %}
    {{ return(chunks) }}
{% endmacro %}

