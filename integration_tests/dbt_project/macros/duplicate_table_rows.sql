{% macro duplicate_table_rows(table_name) %}
    {#- Insert a copy of every row of the given Elementary table, e.g. to
        simulate two overlapping artifact uploads to the same schema. -#}
    {% set relation = ref(table_name) %}
    {% do run_query("INSERT INTO " ~ relation ~ " SELECT * FROM " ~ relation) %}

    {#- Same commit handling as insert_sentinel_row. -#}
    {% set no_commit_adapters = ["spark", "databricks", "bigquery", "athena"] %}
    {% if target.type not in no_commit_adapters %}
        {% do run_query("COMMIT") %}
    {% endif %}
{% endmacro %}
