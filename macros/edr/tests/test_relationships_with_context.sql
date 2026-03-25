{% test relationships_with_context(
    model, column_name, to, field, context_columns=none
) %}
    {%- if context_columns is not none and context_columns is iterable and context_columns is not string %}
        {%- set existing_column_names = (
            adapter.get_columns_in_relation(model)
            | map(attribute="name")
            | map("lower")
            | list
        ) %}
        {%- set select_cols = ["child." ~ column_name] %}
        {%- for col in context_columns %}
            {%- if col | lower == column_name | lower %}
            {# already included, skip #}
            {%- elif col | lower not in existing_column_names %}
                {%- do log(
                    "WARNING [relationships_with_context]: column '"
                    ~ col
                    ~ "' does not exist in model '"
                    ~ model.name
                    ~ "' and will be skipped.",
                    info=true,
                ) %}
            {%- else %} {%- do select_cols.append("child." ~ col) %}
            {%- endif %}
        {%- endfor %}
        {%- set select_clause = select_cols | join(", ") %}
    {%- else %} {%- set select_clause = "child.*" %}
    {%- endif %}

    select {{ select_clause }}
    from {{ model }} as child
    left join {{ to }} as parent on child.{{ column_name }} = parent.{{ field }}
    where child.{{ column_name }} is not null and parent.{{ field }} is null
{% endtest %}
