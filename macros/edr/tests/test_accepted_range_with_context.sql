{% test accepted_range_with_context(
    model,
    column_name,
    min_value=none,
    max_value=none,
    inclusive=true,
    context_columns=none
) %}
    {%- if min_value is none and max_value is none %}
        {{
            exceptions.raise_compiler_error(
                "accepted_range_with_context: at least one of min_value or max_value must be provided."
            )
        }}
    {%- endif %}

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
                    "WARNING [accepted_range_with_context]: column '"
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
        1 = 2
        {%- if min_value is not none %}
            or not {{ column_name }} >{{- "=" if inclusive }} {{ min_value }}
        {%- endif %}
        {%- if max_value is not none %}
            or not {{ column_name }} <{{- "=" if inclusive }} {{ max_value }}
        {%- endif %}
{% endtest %}
