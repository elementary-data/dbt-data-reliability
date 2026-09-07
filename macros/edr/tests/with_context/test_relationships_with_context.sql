{% test relationships_with_context(
    model, column_name, to, field, context_columns=none
) %}
    {%- set select_clause = elementary.get_context_select_clause(
        model=model,
        tested_columns=[column_name],
        context_columns=context_columns,
        test_name="relationships_with_context",
        default_clause="child.*",
        prefix="child.",
    ) %}

    select {{ select_clause }}
    from {{ model }} as child
    left join {{ to }} as parent on child.{{ column_name }} = parent.{{ field }}
    where child.{{ column_name }} is not null and parent.{{ field }} is null
{% endtest %}
