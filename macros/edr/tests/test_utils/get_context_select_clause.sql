{#
    Builds the select list for a `_with_context` test.

    The test materialization samples failing rows by wrapping the test query
    (see `query_test_result_rows`), so whatever a test selects is exactly what
    Elementary stores as its sample.

    Args:
        model: the relation under test.
        tested_columns: columns the test itself needs, always selected first.
            `none` entries are ignored, so table-level tests can pass [].
        context_columns: user-requested extra columns. A bare string is taken as
            a single column; empty or `none` means "no context requested".
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
    {#- `get_columns_in_relation` is stubbed to [] at parse time, so resolving
        here would warn that every context column is missing on every parse. -#}
    {%- if not execute %}
        {%- do return(default_clause if default_clause is not none else "*") %}
    {%- endif %}

    {%- set context_columns = (
        [context_columns]
        if context_columns is string and context_columns
        else context_columns
    ) %}
    {%- set has_context = (
        context_columns
        and context_columns is iterable
        and context_columns is not string
    ) %}

    {%- if not has_context and default_clause is not none %}
        {%- do return(default_clause) %}
    {%- endif %}

    {#- `model` is a subquery string, not a relation, when the test carries a
        `where` config, and that cannot be introspected. -#}
    {%- set relation = elementary.get_model_relation_for_test(
        model, elementary.get_test_model()
    ) %}

    {#- Lowercased name -> the warehouse's own casing, quoted. Unquoted, a
        mixed-case or reserved name (`myCol` on Snowflake, `order` on Postgres)
        emits invalid SQL, so user-supplied names go through this too. -#}
    {%- set resolved = {} %}
    {%- for col in (adapter.get_columns_in_relation(relation) if relation else []) %}
        {%- do resolved.update({col.name | lower: prefix ~ adapter.quote(col.name)}) %}
    {%- endfor %}
    {%- set all_columns_clause = resolved.values() | join(", ") %}

    {%- if not has_context %}
        {#- Reachable only for the `default_clause is none` callers, where an
            empty clause would emit `select from (...)`. -#}
        {%- if not all_columns_clause %}
            {{
                exceptions.raise_compiler_error(
                    test_name
                    ~ ": could not resolve any columns for '"
                    ~ model
                    ~ "'. This test cannot run against a relation dbt is unable to introspect, such as an ephemeral model."
                )
            }}
        {%- endif %}
        {%- do return(all_columns_clause) %}
    {%- endif %}

    {%- set select_cols = [] %}

    {%- for col in tested_columns %}
        {%- if col is not none %}
            {%- set rendered = resolved.get(col | lower, prefix ~ col) %}
            {%- if rendered not in select_cols %}
                {%- do select_cols.append(rendered) %}
            {%- endif %}
        {%- endif %}
    {%- endfor %}

    {%- for col in context_columns %}
        {%- if col | lower not in resolved %}
            {%- do elementary.edr_log_warning(
                test_name
                ~ ": column '"
                ~ col
                ~ "' does not exist in model '"
                ~ (relation.name if relation else model)
                ~ "' and will be skipped."
            ) %}
        {%- elif resolved[col | lower] not in select_cols %}
            {%- do select_cols.append(resolved[col | lower]) %}
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
