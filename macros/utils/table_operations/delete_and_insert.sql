{% macro delete_and_insert(relation, insert_rows=none, delete_values=none, delete_column_key=none) %}
    {% if insert_rows %}
        {% set intermediate_relation = elementary.create_intermediate_relation(relation, insert_rows, temporary=True) %}
    {% endif %}

    {% set query %}
        begin transaction;
        {% if delete_values %}
            delete from {{ relation }} where {{ delete_column_key }} in ('{{ delete_values | join("', '") }}');
        {% endif %}
        {% if intermediate_relation %}
            insert into {{ relation }} select * from {{ intermediate_relation }};
        {% endif %}
        commit;
    {% endset %}
    {% do elementary.run_query(query) %}
{% endmacro %}
