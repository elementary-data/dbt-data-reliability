{% macro delete_and_insert(relation, insert_rows=none, delete_values=none, delete_column_key=none) %}
    {% do elementary.file_log("Deleting from and inserting to: {}".format(relation)) %}
    {% set delete_rows = [] %}
    {% for delete_val in delete_values %}
        {% do delete_rows.append({delete_column_key: delete_val}) %}
    {% endfor %}

    {% if delete_values %}
        {% set delete_relation = elementary.create_intermediate_relation(relation, delete_rows, temporary=True, like_columns=[delete_column_key]) %}
    {% endif %}

    {% if insert_rows %}
        {% set insert_relation = elementary.create_intermediate_relation(relation, insert_rows, temporary=True) %}
    {% endif %}

    {% if not insert_relation and not delete_relation %}
        {% do return(none) %}
    {% endif %}

    {% set queries = elementary.get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% for query in queries %}
        {% do elementary.run_query(query) %}
    {% endfor %}

    {# Make sure we delete the temp tables we created #}
    {% if delete_relation %}
        {% do adapter.drop_relation(delete_relation) %}
    {% endif %}
    {% if insert_relation %}
        {% do adapter.drop_relation(insert_relation) %}
    {% endif %}

    {% do elementary.file_log("Finished deleting from and inserting to: {}".format(relation)) %}
{% endmacro %}

{% macro get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% do return(adapter.dispatch("get_delete_and_insert_queries", "elementary")(relation, insert_relation, delete_relation, delete_column_key)) %}
{% endmacro %}

{% macro default__get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% set query %}
        begin transaction;
        {% if delete_relation %}
            delete from {{ relation }}
            where
            {{ delete_column_key }} is null
            or {{ delete_column_key }} in (select {{ delete_column_key }} from {{ delete_relation }});
        {% endif %}
        {% if insert_relation %}
            insert into {{ relation }} select * from {{ insert_relation }};
        {% endif %}
        commit;
    {% endset %}
    {% do return([query]) %}
{% endmacro %}

{% macro clickhouse__get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% set queries = [] %}

    {% if delete_relation %}
        {% set delete_query %}
            alter table {{ relation }} delete where
            {{ delete_column_key }} is null
            or {{ delete_column_key }} in (select {{ delete_column_key }} from {{ delete_relation }});
        {% endset %}
        {% do queries.append(delete_query) %}
    {% endif %}

    {% if insert_relation %}
        {% set insert_query %}
            insert into {{ relation }} select * from {{ insert_relation }};
        {% endset %}
        {% do queries.append(insert_query) %}
    {% endif %}

    {% do return(queries) %}
{% endmacro %}

{% macro spark__get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% set queries = [] %}

    {# Calling `is_delta` raises an error if `metadata` is None - https://github.com/databricks/dbt-databricks/blob/33dca4b66b05f268741030b33659d34ff69591c1/dbt/adapters/databricks/relation.py#L71 #}
    {% if delete_relation and relation.metadata and relation.is_delta %}
        {% set delete_query %}
            merge into {{ relation }} as source
            using {{ delete_relation }} as target
            on (source.{{ delete_column_key }} = target.{{ delete_column_key }}) or source.{{ delete_column_key }} is null
            when matched then delete;
        {% endset %}
        {% do queries.append(delete_query) %}

    {% elif delete_relation %}
        {% set delete_query %}
            delete from {{ relation }}
            where
            {{ delete_column_key }} is null
            or {{ delete_column_key }} in (select {{ delete_column_key }} from {{ delete_relation }});
        {% endset %}
        {% do queries.append(delete_query) %}
    {% endif %}

    {% if insert_relation %}
        {% set insert_query %}
            insert into {{ relation }} select * from {{ insert_relation }};
        {% endset %}
        {% do queries.append(insert_query) %}
    {% endif %}

    {% do return(queries) %}
{% endmacro %}

{% macro athena__get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% set queries = [] %}

    {% if delete_relation %}
        {% set delete_query %}
            delete from {{ relation }}
            where
            {{ delete_column_key }} is null
            or {{ delete_column_key }} in (select {{ delete_column_key }} from {{ delete_relation }});
        {% endset %}
        {% do queries.append(delete_query) %}
    {% endif %}

    {% if insert_relation %}
        {% set insert_query %}
            insert into {{ relation }} select * from {{ insert_relation }};
        {% endset %}
        {% do queries.append(insert_query) %}
    {% endif %}

    {% do return(queries) %}
{% endmacro %}

{% macro dremio__get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% set queries = [] %}

    {% if delete_relation %}
        {% set delete_query %}
            delete from {{ relation }}
            where
            {{ delete_column_key }} is null
            or {{ delete_column_key }} in (select {{ delete_column_key }} from {{ delete_relation }});
        {% endset %}
        {% do queries.append(delete_query) %}
    {% endif %}

    {% if insert_relation %}
        {% set insert_query %}
            insert into {{ relation }} select * from {{ insert_relation }};
        {% endset %}
        {% do queries.append(insert_query) %}
    {% endif %}

    {% do return(queries) %}
{% endmacro %}

{% macro trino__get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% set queries = [] %}

    {% if delete_relation %}
        {% set delete_query %}
            delete from {{ relation }}
            where
            {{ delete_column_key }} is null
            or {{ delete_column_key }} in (select {{ delete_column_key }} from {{ delete_relation }});
        {% endset %}
        {% do queries.append(delete_query) %}
    {% endif %}

    {% if insert_relation %}
        {% set insert_query %}
            insert into {{ relation }} select * from {{ insert_relation }};
        {% endset %}
        {% do queries.append(insert_query) %}
    {% endif %}

    {% do return(queries) %}
{% endmacro %}
