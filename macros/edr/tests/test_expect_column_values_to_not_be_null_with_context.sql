{% test expect_column_values_to_not_be_null_with_context(
    model, column_name, row_condition=none, context_columns=none
) %}
    {%- set select_clause = elementary.get_context_select_clause(
        model,
        [column_name],
        context_columns,
        "expect_column_values_to_not_be_null_with_context",
    ) %}

    select {{ select_clause }}
    from {{ model }}
    where
        {{ column_name }} is null
        {%- if row_condition %} and {{ row_condition }} {%- endif %}
{% endtest %}
