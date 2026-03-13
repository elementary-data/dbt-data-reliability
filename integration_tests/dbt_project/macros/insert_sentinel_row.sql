{% macro insert_sentinel_row(table_name, sentinel_alias) %}
    {#- Insert a sentinel row into the given Elementary table.
        Used by integration tests to verify that replace_table_data
        actually truncates the whole table (sentinel disappears) rather
        than doing a diff-based update (sentinel would survive).

        Dynamically reads the table's columns so that every column is
        included in the INSERT (Spark / Delta Lake rejects partial
        column lists). Columns not explicitly set get NULL. -#}
    {% set relation = ref(table_name) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {% set col_names = [] %}
    {% set col_values = [] %}
    {% for col in columns %}
        {% do col_names.append(col.name) %}
        {% if col.name | lower == "unique_id" %}
            {% do col_values.append("'test.sentinel'") %}
        {% elif col.name | lower == "alias" %}
            {% do col_values.append("'" ~ sentinel_alias ~ "'") %}
        {% elif col.name | lower == "name" %} {% do col_values.append("'sentinel'") %}
        {% else %} {% do col_values.append("NULL") %}
        {% endif %}
    {% endfor %}

    {% do run_query(
        "INSERT INTO " ~ relation ~ " (" ~ col_names
        | join(", ") ~ ")" ~ " VALUES (" ~ col_values
        | join(", ") ~ ")"
    ) %}

    {#- Most SQL adapters need an explicit COMMIT because run_query DML is
        not auto-committed.  Spark-based and serverless engines do not
        support bare COMMIT statements, so we skip them. -#}
    {% set no_commit_adapters = ["spark", "databricks", "bigquery", "athena"] %}
    {% if target.type not in no_commit_adapters %}
        {% do run_query("COMMIT") %}
    {% endif %}
{% endmacro %}
