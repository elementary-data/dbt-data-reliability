{% test expect_column_values_to_be_unique_with_context(
    model, column_name, row_condition=none, context_columns=none
) %}
    {%- set all_columns = (
        adapter.get_columns_in_relation(model) | map(attribute="name") | list
    ) %}
    {%- set all_columns_lower = all_columns | map("lower") | list %}

    {%- if context_columns is not none and context_columns is iterable and context_columns is not string %}
        {%- set select_cols = [column_name] %}
        {%- for col in context_columns %}
            {%- if col | lower == column_name | lower %}
            {# already included, skip #}
            {%- elif col | lower not in all_columns_lower %}
                {%- do log(
                    "WARNING [expect_column_values_to_be_unique_with_context]: column '"
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
    {%- else %} {%- set select_clause = all_columns | join(", ") %}
    {%- endif %}

    select {{ select_clause }}
    from
        (
            select *, count(*) over (partition by {{ column_name }}) as n_records
            from {{ model }}
            {%- if row_condition %} where {{ row_condition }} {%- endif %}
        ) validation
    where n_records > 1
{% endtest %}
