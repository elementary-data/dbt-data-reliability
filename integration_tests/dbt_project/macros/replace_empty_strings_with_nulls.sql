{# This macro is only used for BigQuery fusion seeds (see dbt_project.py _fix_seed_if_needed).
   ClickHouse uses ClickHouseDirectSeeder (data_seeder.py) which creates Nullable(String)
   columns directly, so no post-hoc repair is needed. #}
{% macro replace_empty_strings_with_nulls(table_name) %}
    {% set relation = ref(table_name) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {% for col in columns %}
        {% set data_type = elementary.get_column_data_type(col) %}
        {% set normalized_data_type = elementary.normalize_data_type(data_type) %}

        {% if normalized_data_type == "string" %}
            {% set quoted_col = adapter.quote(col["name"]) %}
            {% set update_query %}
                update {{ relation }}
                set {{ quoted_col }} = NULL
                where {{ quoted_col }} = ''
            {% endset %}
            {% do elementary.run_query(update_query) %}
        {% endif %}
    {% endfor %}
{% endmacro %}
