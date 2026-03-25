{% test expect_column_values_to_not_be_null_with_context(
    model, column_name, row_condition=none, context_columns=none
) %}
    {%- if context_columns is not none and context_columns is iterable and context_columns is not string %}
        {%- set existing_column_names = (
            adapter.get_columns_in_relation(model)
            | map(attribute="name")
            | map("lower")
            | list
        ) %}
        {%- set select_cols = [column_name] %}
        {%- for col in context_columns %}
            {%- if col | lower == column_name | lower %}
            {# already included, skip #}
            {%- elif col | lower not in existing_column_names %}
                {%- do log(
                    "WARNING [expect_column_values_to_not_be_null_with_context]: column '"
                    ~ col
                    ~ "' does not exist in model '"
                    ~ model.name
                    ~ "' and will be skipped.",
                    info=true,
                ) %}
            {%- else %} {%- do select_cols.append(col) %}
            {%- endif %}
        {%- endfor %}
        {%- set select_clause = select_cols | join(", ") %}
    {%- else %} {%- set select_clause = "*" %}
    {%- endif %}

    select {{ select_clause }}
    from {{ model }}
    where
        {{ column_name }} is null
        {%- if row_condition %} and {{ row_condition }} {%- endif %}
{% endtest %}
