{% macro get_baseline_diff_query(model, model_baseline_relation) %}
    {% set full_table_name = elementary.relation_to_full_name(model) %}

    with baseline as (
        select column_name, data_type
        from {{ model_baseline_relation}}
    )

    select
        column_name,
        data_type

    with current_columns as (
        select
            full_table_name,
            column_name,
            cast(data_type as {{ elementary.type_string() }}) as data_type,

        from {{ ref('filtered_information_schema_columns') }}
        where lower(full_table_name) = lower('{{ full_table_name }}')
    ),
{% endmacro %}