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
    {% do elementary.file_log("Finished deleting from and inserting to: {}".format(relation)) %}
{% endmacro %}

{% macro get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% do return(adapter.dispatch("get_delete_and_insert_queries", "elementary")(relation, insert_relation, delete_relation, delete_column_key)) %}
{% endmacro %}

{% macro default__get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% set query %}
        begin transaction;
        {% if delete_relation %}
            delete from {{ relation }} where {{ delete_column_key }} in (select {{ delete_column_key }} from {{ delete_relation }});
        {% endif %}
        {% if insert_relation %}
            insert into {{ relation }} select * from {{ insert_relation }};
        {% endif %}
        commit;
    {% endset %}
    {% do return([query]) %}
{% endmacro %}

{% macro spark__get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    {% set queries = [] %}

    {% if delete_relation %}
        {% set delete_query %}
            delete from {{ relation }} where {{ delete_column_key }} in (select {{ delete_column_key }} from {{ delete_relation }});
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
