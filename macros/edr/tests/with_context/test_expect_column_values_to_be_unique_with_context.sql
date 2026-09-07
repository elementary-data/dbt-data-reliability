{% test expect_column_values_to_be_unique_with_context(
    model, column_name, context_columns=none
) %}
    {#- default_clause=none: `*` would leak elementary_n_records into the sample. -#}
    {%- set select_clause = elementary.get_context_select_clause(
        model=model,
        tested_columns=[column_name],
        context_columns=context_columns,
        test_name="expect_column_values_to_be_unique_with_context",
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
            where {{ column_name }} is not null
        ) validation
    where elementary_n_records > 1
{% endtest %}
