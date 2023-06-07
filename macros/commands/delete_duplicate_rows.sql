{% macro delete_duplicate_rows(model_unique_id) %}
    {% do return(adapter.dispatch("delete_duplicate_rows", "elementary")(model_unique_id)) %}
{% endmacro %}

{% macro postgres__delete_duplicate_rows(model_unique_id) %}
    {% set node = graph.nodes[model_unique_id] %}
    {% set relation = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.alias) %}
    {% if relation is none %}
        {% do print("Relation '{}' does not exist.".format(node.relation_name)) %}
        {% do return([]) %}
    {% endif %}

    {% set column_names = adapter.get_columns_in_relation(relation) | map(attribute="name") | map("lower") | list %}

    {% set query %}
        DELETE FROM {{ relation }} AS t1
        USING {{ relation }} AS t2
        WHERE t1.ctid < t2.ctid
        {% for col in column_names %}
        AND t1.{{ col }} = t2.{{ col }}
        {% endfor %}
    {% endset %}
    {% do elementary.run_query(query) %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro default__delete_duplicate_rows(model_unique_id) %}
  {{ exceptions.raise_compiler_error("This macro is not supported on '{}'.".format(target.type)) }}
{% endmacro %}
