{% macro delete_and_insert(
    relation, insert_rows=none, delete_values=none, delete_column_key=none
) %}
    {% do elementary.file_log("Deleting from and inserting to: {}".format(relation)) %}
    {% set delete_rows = [] %}
    {% for delete_val in delete_values %}
        {% do delete_rows.append({delete_column_key: delete_val}) %}
    {% endfor %}

    {% if delete_values %}
        {% set delete_relation = elementary.create_intermediate_relation(
            relation,
            delete_rows,
            temporary=True,
            like_columns=[delete_column_key],
        ) %}
    {% endif %}

    {% if insert_rows %}
        {% set insert_relation = elementary.create_intermediate_relation(
            relation, insert_rows, temporary=True
        ) %}
    {% endif %}

    {% if not insert_relation and not delete_relation %}
        {% do return(none) %}
    {% endif %}

    {% set queries = elementary.get_delete_and_insert_queries(
        relation, insert_relation, delete_relation, delete_column_key
    ) %}
    {% for query in queries %} {% do elementary.run_query(query) %} {% endfor %}

    {# DuckDB: explicit commit so changes survive dbt's post-on-run-end ROLLBACK #}
    {% if target.type == "duckdb" %} {% do adapter.commit() %} {% endif %}

    {# Make sure we delete the temp tables we created #}
    {% if delete_relation %} {% do adapter.drop_relation(delete_relation) %} {% endif %}
    {% if insert_relation %} {% do adapter.drop_relation(insert_relation) %} {% endif %}

    {# DuckDB: commit cleanup drops so they survive dbt's post-on-run-end ROLLBACK too #}
    {% if target.type == "duckdb" and (delete_relation or insert_relation) %}
        {% do adapter.commit() %}
    {% endif %}

    {% do elementary.file_log(
        "Finished deleting from and inserting to: {}".format(relation)
    ) %}
{% endmacro %}

{% macro get_delete_and_insert_queries(
    relation, insert_relation, delete_relation, delete_column_key
) %}
    {% do return(
        adapter.dispatch("get_delete_and_insert_queries", "elementary")(
            relation, insert_relation, delete_relation, delete_column_key
        )
    ) %}
{% endmacro %}

{% macro default__get_delete_and_insert_queries(
    relation, insert_relation, delete_relation, delete_column_key
) %}
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

{% macro clickhouse__get_delete_and_insert_queries(
    relation, insert_relation, delete_relation, delete_column_key
) %}
    {% set queries = [] %}

    {% if delete_relation %}
        {% set delete_query %}
            alter table {{ relation }} {{ on_cluster_clause(relation) }} delete where
            {{ delete_column_key }} is null
            or {{ delete_column_key }} in (select {{ delete_column_key }} from {{ delete_relation }})
            {{ adapter.get_model_query_settings(model) }};
        {% endset %}
        {% do queries.append(delete_query) %}
    {% endif %}

    {% if insert_relation %}
        {% set insert_query %}
            insert into {{ relation }} {{ adapter.get_model_query_settings(model) }} select * from {{ insert_relation }};
        {% endset %}
        {% do queries.append(insert_query) %}
    {% endif %}

    {% do return(queries) %}
{% endmacro %}

{% macro spark__get_delete_and_insert_queries(
    relation, insert_relation, delete_relation, delete_column_key
) %}
    {% set queries = [] %}

    {# Delta tables do not support DELETE … WHERE … IN (subquery), so use MERGE.
       On dbt-databricks relation.metadata is set and is_delta can be checked.
       On plain dbt-spark (thrift) relation.metadata is None – we default to
       MERGE because file_format=delta is configured.  Non-Delta tables on
       dbt-databricks fall through to a plain DELETE. #}
    {% if delete_relation and (
        (relation.metadata and relation.is_delta) or not relation.metadata
    ) %}
        {% set delete_query %}
            merge into {{ relation }} as target
            using {{ delete_relation }} as source
            on (target.{{ delete_column_key }} = source.{{ delete_column_key }}) or target.{{ delete_column_key }} is null
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

{% macro fabricspark__get_delete_and_insert_queries(
    relation, insert_relation, delete_relation, delete_column_key
) %}
    {{
        return(
            elementary.spark__get_delete_and_insert_queries(
                relation, insert_relation, delete_relation, delete_column_key
            )
        )
    }}
{% endmacro %}

{% macro redshift__get_delete_and_insert_queries(
    relation, insert_relation, delete_relation, delete_column_key
) %}
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

{% macro athena__get_delete_and_insert_queries(
    relation, insert_relation, delete_relation, delete_column_key
) %}
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

{% macro dremio__get_delete_and_insert_queries(
    relation, insert_relation, delete_relation, delete_column_key
) %}
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

{# DuckDB - separate queries without transaction wrapping (commit handled in delete_and_insert) #}
{% macro duckdb__get_delete_and_insert_queries(
    relation, insert_relation, delete_relation, delete_column_key
) %}
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

{% macro trino__get_delete_and_insert_queries(
    relation, insert_relation, delete_relation, delete_column_key
) %}
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
