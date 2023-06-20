{% macro dump_table(model_unique_id, output_path, exclude_deprecated_columns=true, timestamp_column=none, since=none, days_back=7) %}
    {% set node = graph.nodes.get(model_unique_id) %}
    {% if not node %}
        {% do print("Node '{}' does not exist.".format(model_unique_id)) %}
        {% do return([]) %}
    {% endif %}

    {% set relation = api.Relation.create(database=node.database, schema=node.schema, identifier=node.alias) %}
    {% set column_names = adapter.get_columns_in_relation(relation) | map(attribute="name") | map("lower") | list %}
    {% if not column_names %}
        {% do print("Relation '{}' does not exist.".format(node.relation_name)) %}
        {% do return([]) %}
    {% endif %}

    {% if exclude_deprecated_columns %}
        {% set deprecated_column_names = node.meta.get("deprecated_columns", []) | map(attribute="name") | map("lower") | list %}
        {% set column_names = column_names | reject("in", deprecated_column_names) | list %}
    {% endif %}

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
