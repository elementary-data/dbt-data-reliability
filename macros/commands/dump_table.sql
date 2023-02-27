{% macro dump_table(model_unique_id, output_path, since=none, days_back=7) %}
    {% set node = graph.nodes[model_unique_id] %}
    {% set timestamp_column = node.meta.timestamp_column %}
    {% set query %}
        select * from {{ node.relation_name }}
        {% if timestamp_column %}
            {% if since %}
                where {{ timestamp_column }} > {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(since)) }}
            {% else %}
                where {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp(timestamp_column), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
            {% endif %}
        {% endif %}
    {% endset %}
    {% set results = elementary.run_query(query) %}
    {% do results.to_csv(output_path) %}
{% endmacro %}
