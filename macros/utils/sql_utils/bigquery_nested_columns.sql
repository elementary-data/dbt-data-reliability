{# ---------------------------------------------------------------------- #}
{# BigQuery STRUCT nested-field helpers.                                   #}
{# #}
{# All of these are no-ops outside BigQuery and for anything that is not a #}
{# plain dotted identifier path, so callers can apply them unconditionally #}
{# without changing behaviour on other adapters.                          #}
{# ---------------------------------------------------------------------- #}
{% macro bq_is_nested_identifier(name) %}
    {#- True only on BigQuery and only when `name` is a plain dotted identifier
        path (e.g. user.address.city) — i.e. an actual nested STRUCT reference.
        Returns false for plain identifiers, SQL expressions (dimensions are
        documented as accepting arbitrary expressions, which must pass through
        untouched) and non-BigQuery adapters. -#}
    {%- if target.type != "bigquery" or name is not string -%}
        {{ return(false) }}
    {%- endif -%}
    {{ return(modules.re.match("^\\w+(\\.\\w+)+$", name) is not none) }}
{% endmacro %}

{% macro bq_segment_quote(name) %}
    {#- Segment-quote a nested identifier path for BigQuery:
        user.address.city -> `user`.`address`.`city`.
        `BigQueryColumn.quoted` cannot be used here — it wraps the whole string
        in a single pair of backticks, which BigQuery reads as one column
        literally named "user.address.city".
        Anything that is not a nested identifier path is returned unchanged. -#}
    {%- if elementary.bq_is_nested_identifier(name) -%}
        {%- set parts = [] -%}
        {%- for seg in name.split(".") -%}
            {%- do parts.append("`" ~ seg ~ "`") -%}
        {%- endfor -%}
        {{ parts | join(".") }}
    {%- else -%} {{ name }}
    {%- endif -%}
{% endmacro %}

{% macro bq_safe_alias(name) %}
    {#- Convert a dotted identifier path into a dot-free SQL identifier.
        Projecting `select user.address.city from t` into a CTE without an alias
        names the resulting column `city`, losing the path, so nested columns
        must be aliased on the way in. Only call this for names that satisfy
        `bq_is_nested_identifier` — on arbitrary SQL expressions it produces
        nonsense. -#}
    {{- name | replace(".", "__") -}}
{% endmacro %}

{% macro bq_alias_safe_dimension(dimension) %}
    {#- Alias-safe form of a dimension: dot-free for nested BigQuery struct
        paths, unchanged for plain identifiers and SQL expressions. -#}
    {%- if elementary.bq_is_nested_identifier(dimension) -%}
        {{- elementary.bq_safe_alias(dimension) -}}
    {%- else -%} {{- dimension -}}
    {%- endif -%}
{% endmacro %}

{% macro bq_flatten_nested_columns(column_objects) %}
    {#- Expand BigQuery STRUCT columns into their monitorable leaves, keeping the
        top-level STRUCT alongside them so that `column_name=user` keeps working.
        Leaves under a REPEATED ancestor are excluded — reaching them requires
        UNNEST. Returns `column_objects` unchanged on non-BigQuery adapters. -#}
    {%- if target.type != "bigquery" -%} {{ return(column_objects) }} {%- endif -%}
    {%- set expanded = [] -%}
    {%- for column_obj in column_objects -%}
        {%- do expanded.append(column_obj) -%}
        {%- if column_obj.fields | length > 0 -%}
            {#- `BigQueryColumn.flatten()` discards ancestor modes, so a NULLABLE
                leaf under a REPEATED ancestor still satisfies
                `leaf.mode != 'REPEATED'`. Build the set of safe leaf names via an
                ancestor-aware walker and filter `flatten()` against it. -#}
            {%- set safe_names = elementary.bq_safe_leaf_names(column_obj) -%}
            {%- for leaf in column_obj.flatten() -%}
                {%- if leaf.name in safe_names -%}
                    {%- do expanded.append(leaf) -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(expanded) }}
{% endmacro %}

{% macro bq_safe_leaf_names(column_obj) %}
    {#- Walk a BigQuery STRUCT tree and collect dotted leaf names that are safe to
        monitor without UNNEST — i.e. no REPEATED ancestor anywhere in the path,
        and the leaf itself is not REPEATED. `BigQueryColumn.flatten()` returns
        leaf columns with the leaf's own mode but discards ancestor modes, so this
        walker is the source of truth for "which leaves can we project directly?".
        Names are built the same way `flatten()` builds them (from `.column`), so
        the two are directly comparable. -#}
    {%- set safe_names = [] -%}
    {%- if column_obj.mode != "REPEATED" and column_obj.fields is defined and column_obj.fields | length > 0 -%}
        {%- for child in column_obj.fields -%}
            {%- do elementary._bq_walk_collect(
                child, [column_obj.column], false, safe_names
            ) -%}
        {%- endfor -%}
    {%- endif -%}
    {{ return(safe_names) }}
{% endmacro %}

{% macro _bq_walk_collect(field, prefix, has_repeated_ancestor, safe_names) %}
    {#- Recursive helper for `bq_safe_leaf_names`. `field` is a `BigQueryColumn`
        (`BigQueryColumn.fields` wraps its subfields via `wrap_subfields`), so it
        exposes `.name`, `.mode` and `.fields`. Propagates whether any ancestor
        was REPEATED and appends safe leaf names to `safe_names`. -#}
    {%- set new_prefix = prefix + [field.name] -%}
    {%- if field.fields | length == 0 -%}
        {%- if not has_repeated_ancestor and field.mode != "REPEATED" -%}
            {%- do safe_names.append(new_prefix | join(".")) -%}
        {%- endif -%}
    {%- else -%}
        {%- set new_has_repeated = has_repeated_ancestor or (
            field.mode == "REPEATED"
        ) -%}
        {%- for child in field.fields -%}
            {%- do elementary._bq_walk_collect(
                child, new_prefix, new_has_repeated, safe_names
            ) -%}
        {%- endfor -%}
    {%- endif -%}
{% endmacro %}
