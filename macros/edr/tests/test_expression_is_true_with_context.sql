{% test expression_is_true_with_context(
    model, expression, column_name=none, context_columns=none
) %}
    {#- `column_name` is optional so the same test covers table-level expressions. -#}
    {%- set select_clause = elementary.get_context_select_clause(
        model,
        [column_name],
        context_columns,
        "expression_is_true_with_context",
    ) %}

    select {{ select_clause }}
    from {{ model }}
    where
        {%- if column_name is none %} not ({{ expression }})
        {%- else %} not ({{ column_name }} {{ expression }})
        {%- endif %}
{% endtest %}
