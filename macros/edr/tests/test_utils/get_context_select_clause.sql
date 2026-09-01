{#
    Builds the select list for a `_with_context` test.

    The test materialization samples failing rows by wrapping the test query
    (see `query_test_result_rows`), so whatever a test selects is exactly what
    Elementary stores as its sample. This macro resolves `context_columns`
    against the tested relation so every `_with_context` test shares one
    validation and warning behavior.

    Args:
        model: the relation under test.
        tested_columns: columns the test itself needs, always selected first.
            `none` entries are ignored, so table-level tests can pass [].
        context_columns: user-requested extra columns. Anything that is not a
            list (including `none`) means "no context requested".
        test_name: used in the skipped-column warning.
        default_clause: what to select when no context is requested. Pass
            `none` to list every column explicitly, which callers need when a
            bare `*` would leak a helper column such as `elementary_n_records`.
        prefix: prepended to every column, for tests that alias the relation.
#}
{% macro get_context_select_clause(
    model,
    tested_columns,
    context_columns,
    test_name,
    default_clause="*",
    prefix=""
) %}
    {#- At parse time dbt stubs out column introspection: get_columns_in_relation
        is decorated `@available.parse_list`, which substitutes a function
        returning []. Every context column would therefore look missing and warn,
        once per context column per test on every full parse. The parsed SQL is
        only used to collect refs, so skip the resolution entirely. -#}
    {%- if not execute %}
        {%- do return(default_clause if default_clause is not none else "*") %}
    {%- endif %}

    {%- set has_context = (
        context_columns is not none
        and context_columns is iterable
        and context_columns is not string
    ) %}

    {%- if not has_context and default_clause is not none %}
        {%- do return(default_clause) %}
    {%- endif %}

    {%- set existing_columns = (
        adapter.get_columns_in_relation(model) | map(attribute="name") | list
    ) %}
    {%- set existing_columns_lower = existing_columns | map("lower") | list %}

    {%- set all_columns_clause = [] %}
    {%- for col in existing_columns %}
        {%- do all_columns_clause.append(prefix ~ col) %}
    {%- endfor %}
    {%- set all_columns_clause = all_columns_clause | join(", ") %}

    {#- Only the `default_clause is none` callers can reach a return of this value,
        and an empty one would emit `select from (...)`. A relation dbt cannot
        introspect (an ephemeral model, whose `__dbt__cte__` name does not exist in
        the warehouse) is the way to get here, so name that. -#}
    {%- if not all_columns_clause and default_clause is none %}
        {{
            exceptions.raise_compiler_error(
                test_name
                ~ ": could not resolve any columns for '"
                ~ model
                ~ "'. This test cannot run against a relation dbt is unable to introspect, such as an ephemeral model."
            )
        }}
    {%- endif %}

    {%- if not has_context %} {%- do return(all_columns_clause) %} {%- endif %}

    {%- set select_cols = [] %}
    {%- set selected_lower = [] %}

    {%- for col in tested_columns %}
        {%- if col is not none and col | lower not in selected_lower %}
            {%- do selected_lower.append(col | lower) %}
            {%- do select_cols.append(prefix ~ col) %}
        {%- endif %}
    {%- endfor %}

    {%- for col in context_columns %}
        {%- if col | lower in selected_lower %}
        {# already selected, skip #}
        {%- elif col | lower not in existing_columns_lower %}
            {%- do log(
                "WARNING ["
                ~ test_name
                ~ "]: column '"
                ~ col
                ~ "' does not exist in model '"
                ~ model.name
                ~ "' and will be skipped.",
                info=true,
            ) %}
        {%- else %}
            {%- do selected_lower.append(col | lower) %}
            {%- do select_cols.append(prefix ~ col) %}
        {%- endif %}
    {%- endfor %}

    {#- Every requested column was skipped, so fall back rather than emit an empty select list. -#}
    {%- if not select_cols %}
        {%- do return(
            default_clause
            if default_clause is not none
            else all_columns_clause
        ) %}
    {%- endif %}

    {%- do return(select_cols | join(", ")) %}
{% endmacro %}
