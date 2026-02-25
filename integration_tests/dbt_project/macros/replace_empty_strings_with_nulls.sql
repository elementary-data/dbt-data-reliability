{% macro replace_empty_strings_with_nulls(table_name) %}
    {% set relation = ref(table_name) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {% if target.type == "clickhouse" %}
        {# On ClickHouse, columns are non-Nullable by default so NULLs in CSV seeds become
           empty strings. We first ALTER each string column to Nullable(String), then use
           ALTER TABLE UPDATE to convert empty strings to NULLs.
           We use statement blocks for DDL since dbt.run_query may not handle DDL on ClickHouse. #}
        {% for col in columns %}
            {% set data_type = elementary.get_column_data_type(col) %}
            {% set normalized_data_type = elementary.normalize_data_type(data_type) %}
            {% if normalized_data_type == "string" %}
                {% call statement('alter_nullable_' ~ col['name'], fetch_result=False) %}
                    alter table {{ relation }} modify column `{{ col['name'] }}` Nullable(String)
                {% endcall %}
                {% call statement('update_nulls_' ~ col['name'], fetch_result=False) %}
                    alter table {{ relation }} update `{{ col['name'] }}` = NULL where `{{ col['name'] }}` = '' settings mutations_sync = 1
                {% endcall %}
            {% endif %}
        {% endfor %}
    {% else %}
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
    {% endif %}
{% endmacro %}
