{% macro dump_table(model_unique_id, output_path, exclude_deprecated_columns=true, since=none, days_back=7) %}
    {% set node = graph.nodes[model_unique_id] %}
    {% set relation = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.alias) %}
    {% if relation is none %}
        {% do print("Relation '{}' does not exist.".format(node.relation_name)) %}
        {% do return([]) %}
    {% endif %}

    {% set column_names = adapter.get_columns_in_relation(relation) | map(attribute="name") | map("lower") | list %}
    {% if exclude_deprecated_columns %}
        {% set deprecated_column_names = node.columns.values() | selectattr("deprecated") | map(attribute="name") | map("lower") | list %}
        {% set column_names = column_names | reject("in", deprecated_column_names) | list %}
    {% endif %}

    {% set timestamp_column = node.meta.timestamp_column %}
    {% set query %}
        select {{ elementary.escape_select(column_names) }} from {{ relation }}
        {% if timestamp_column %}
            {% if since %}
                where {{ elementary.edr_cast_as_timestamp(timestamp_column) }} > {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(since)) }}
            {% else %}
                where {{ elementary.edr_datediff(elementary.edr_cast_as_timestamp(timestamp_column), elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
            {% endif %}
        {% endif %}
    {% endset %}
    {% set results = elementary.run_query(query) %}
    {% do results.to_csv(output_path) %}
    {% do return(results.column_names) %}
{% endmacro %}
