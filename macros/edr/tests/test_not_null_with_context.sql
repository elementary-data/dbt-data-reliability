{% test not_null_with_context(model, column_name, context_columns=none) %}
    {%- set select_clause = elementary.get_context_select_clause(
        model, [column_name], context_columns, "not_null_with_context"
    ) %}

    select {{ select_clause }}
    from {{ model }}
    where {{ column_name }} is null
{% endtest %}
