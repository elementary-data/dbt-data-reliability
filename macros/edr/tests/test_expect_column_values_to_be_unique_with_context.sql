{% test expect_column_values_to_be_unique_with_context(
    model, column_name, row_condition=none, context_columns=none
) %}
    {#- `elementary_n_records` below is a helper column, so the default select
        list has to name every real column instead of using `*`. -#}
    {%- set select_clause = elementary.get_context_select_clause(
        model,
        [column_name],
        context_columns,
        "expect_column_values_to_be_unique_with_context",
        default_clause=none,
    ) %}

    select {{ select_clause }}
    from
        (
            select
                *,
                count(*) over (partition by {{ column_name }}) as elementary_n_records
            from {{ model }}
            {#- NULLs partition together, so they would report as duplicates of
                each other. dbt's own `unique` filters them out too. -#}
            where
                {{ column_name }} is not null
                {%- if row_condition %} and ({{ row_condition }}) {%- endif %}
        ) validation
    where elementary_n_records > 1
{% endtest %}
