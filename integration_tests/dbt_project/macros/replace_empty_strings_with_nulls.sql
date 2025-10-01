{% macro replace_empty_strings_with_nulls(table_name) %}
    {% set relation = ref(table_name) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {% for col in columns %}
        {% set data_type = elementary.get_column_data_type(col) %}
        {% set normalized_data_type = elementary.normalize_data_type(data_type) %}
        
        {% if normalized_data_type == "string" %}
            {% set update_query %}
                update {{ relation }}
                set {{ col["name"] }} = NULL
                where {{ col["name"] }} = ''
            {% endset %}
            {% do elementary.run_query(update_query) %}
        {% endif %}
    {% endfor %}
{% endmacro %}
