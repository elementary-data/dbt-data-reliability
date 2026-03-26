{% test not_null_with_context(model, column_name, context_columns=none) %}
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
                    "WARNING [not_null_with_context]: column '"
                    ~ col
                    ~ "' does not exist in model '"
                    ~ model.name
                    ~ "' and will be skipped.",
                    info=true,
                ) %}
            {%- else %} {%- do select_cols.append(col) %}
            {%- endif %}
        {%- endfor %}
        select {{ select_cols | join(", ") }}
        from {{ model }}
        where {{ column_name }} is null
    {%- else %} select * from {{ model }} where {{ column_name }} is null
    {%- endif %}
{% endtest %}
