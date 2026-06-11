{% macro column_monitoring_query(
    monitored_table,
    monitored_table_relation,
    min_bucket_start,
    max_bucket_end,
    days_back,
    column_obj,
    column_metrics,
    metric_properties,
    dimensions
) %}
    {%- set full_table_name_str = elementary.edr_quote(
        elementary.relation_to_full_name(monitored_table_relation)
    ) %}
    {%- set timestamp_column = metric_properties.timestamp_column %}
    {% set prefixed_dimensions = [] %}
    {% for dimension_column in dimensions %}
        {% if elementary.bq_is_nested_identifier(dimension_column) %}
            {% do prefixed_dimensions.append(
                "dimension_" ~ elementary.bq_safe_alias(dimension_column)
            ) %}
        {% else %}
            {% do prefixed_dimensions.append("dimension_" ~ dimension_column) %}
        {% endif %}
    {% endfor %}

    {% set metric_types = [] %}
    {% set metric_name_to_type = {} %}
    {% for metric in column_metrics %}
        {% do metric_types.append(metric.type) %}
        {% do metric_name_to_type.update({metric.name: metric.type}) %}
    {% endfor %}

    with
        monitored_table as (
            select *
            from {{ monitored_table }}
            {% if metric_properties.where_expression %}
                where {{ metric_properties.where_expression }}
            {% endif %}
        ),

        {% if timestamp_column -%}
            buckets as (
                select edr_bucket_start, edr_bucket_end
                from
                    (
                        {{
                            elementary.complete_buckets_cte(
                                metric_properties, min_bucket_start, max_bucket_end
                            )
                        }}
                    ) results
                where
                    edr_bucket_start
                    >= {{ elementary.edr_cast_as_timestamp(min_bucket_start) }}
                    and edr_bucket_end
                    <= {{ elementary.edr_cast_as_timestamp(max_bucket_end) }}
            ),
            filtered_monitored_table as (
                select
                    {{ column_obj.quoted }}{% if column_obj.is_nested %} as {{ adapter.quote(column_obj.safe_alias) }}{% endif %},
                    {%- if dimensions -%}
                        {{
                            elementary.select_dimensions_columns(
                                dimensions, "dimension"
                            )
                        }},
                    {%- endif -%}
                    {{
                        elementary.get_start_bucket_in_data(
                            timestamp_column,
                            min_bucket_start,
                            metric_properties.time_bucket,
                        )
                    }} as start_bucket_in_data
                from monitored_table
                where
                    {{ elementary.edr_cast_as_timestamp(timestamp_column) }}
                    >= (select min(edr_bucket_start) from buckets)
                    and {{ elementary.edr_cast_as_timestamp(timestamp_column) }}
                    < (select max(edr_bucket_end) from buckets)
            ),
        {%- else %}
            filtered_monitored_table as (
                select
                    {{ column_obj.quoted }}{% if column_obj.is_nested %} as {{ adapter.quote(column_obj.safe_alias) }}{% endif %},
                    {%- if dimensions -%}
                        {{
                            elementary.select_dimensions_columns(
                                dimensions, "dimension"
                            )
                        }},
                    {%- endif -%}
                    {{ elementary.null_timestamp() }} as start_bucket_in_data
                from monitored_table
            ),
        {% endif %}

        column_metrics as (

            {%- if column_metrics %}
                {%- set column = adapter.quote(column_obj.safe_alias) if column_obj.is_nested else column_obj.quoted -%}
                select
                    {%- if timestamp_column %}
                        edr_bucket_start as bucket_start, edr_bucket_end as bucket_end,
                    {%- else %}
                        {{ elementary.null_timestamp() }} as bucket_start,
                        {{
                            elementary.edr_cast_as_timestamp(
                                elementary.edr_quote(
                                    elementary.run_started_at_as_string()
                                )
                            )
                        }} as bucket_end,
                    {%- endif %}
                    {% if dimensions | length > 0 %}
                        {{ elementary.select_dimensions_columns(prefixed_dimensions) }},
                    {% endif %}
                    {%- if "null_count" in metric_types -%}
                        {{ elementary.null_count(column) }}
                    {%- else -%} null
                    {% endif %} as null_count,
                    {%- if "null_percent" in metric_types -%}
                        {{ elementary.null_percent(column) }}
                    {%- else -%} null
                    {% endif %} as null_percent,
                    {%- if "not_null_percent" in metric_types -%}
                        {{ elementary.not_null_percent(column) }}
                    {%- else -%} null
                    {% endif %} as not_null_percent,
                    {%- if "max" in metric_types -%} {{ elementary.max(column) }}
                    {%- else -%} null
                    {% endif %} as {{ elementary.escape_reserved_keywords("max") }},
                    {%- if "min" in metric_types -%} {{ elementary.min(column) }}
                    {%- else -%} null
                    {% endif %} as {{ elementary.escape_reserved_keywords("min") }},
                    {%- if "average" in metric_types -%}
                        {{ elementary.average(column) }}
                    {%- else -%} null
                    {% endif %} as average,
                    {%- if "zero_count" in metric_types -%}
                        {{ elementary.zero_count(column) }}
                    {%- else -%} null
                    {% endif %} as zero_count,
                    {%- if "zero_percent" in metric_types -%}
                        {{ elementary.zero_percent(column) }}
                    {%- else -%} null
                    {% endif %} as zero_percent,
                    {%- if "not_zero_percent" in metric_types -%}
                        {{ elementary.not_zero_percent(column) }}
                    {%- else -%} null
                    {% endif %} as not_zero_percent,
                    {%- if "standard_deviation" in metric_types -%}
                        {{ elementary.standard_deviation(column) }}
                    {%- else -%} null
                    {% endif %} as standard_deviation,
                    {%- if "variance" in metric_types -%}
                        {{ elementary.variance(column) }}
                    {%- else -%} null
                    {% endif %} as variance,
                    {%- if "max_length" in metric_types -%}
                        {{ elementary.max_length(column) }}
                    {%- else -%} null
                    {% endif %} as max_length,
                    {%- if "min_length" in metric_types -%}
                        {{ elementary.min_length(column) }}
                    {%- else -%} null
                    {% endif %} as min_length,
                    {%- if "average_length" in metric_types -%}
                        {{ elementary.average_length(column) }}
                    {%- else -%} null
                    {% endif %} as average_length,
                    {%- if "missing_count" in metric_types -%}
                        {{ elementary.missing_count(column) }}
                    {%- else -%} null
                    {% endif %} as missing_count,
                    {%- if "missing_percent" in metric_types -%}
                        {{ elementary.missing_percent(column) }}
                    {%- else -%} null
                    {% endif %} as missing_percent,
                    {%- if "count_true" in metric_types -%}
                        {{ elementary.count_true(column) }}
                    {%- else -%} null
                    {% endif %} as count_true,
                    {%- if "count_false" in metric_types -%}
                        {{ elementary.count_false(column) }}
                    {%- else -%} null
                    {% endif %} as count_false,
                    {%- if "not_missing_percent" in metric_types -%}
                        {{ elementary.not_missing_percent(column) }}
                    {%- else -%} null
                    {% endif %} as not_missing_percent,
                    {%- if "sum" in metric_types -%} {{ elementary.sum(column) }}
                    {%- else -%} null
                    {% endif %} as {{ elementary.escape_reserved_keywords("sum") }}
                from filtered_monitored_table
                {%- if timestamp_column %}
                    left join buckets on (edr_bucket_start = start_bucket_in_data)
                {%- endif %}
                    {{
                        elementary.column_monitoring_group_by(
                            timestamp_column, dimensions, prefixed_dimensions
                        )
                    }}
            {%- else %}{{ elementary.empty_column_monitors_cte() }}
            {%- endif %}

        ),

        column_metrics_unpivot as (
            {%- if column_metrics %}
                {% for metric_name, metric_type in metric_name_to_type.items() %}
                    select
                        {{ elementary.const_as_string(column_obj.name) }}
                        as edr_column_name,
                        bucket_start,
                        bucket_end,
                        {% if timestamp_column %}
                            {{
                                elementary.timediff(
                                    "hour", "bucket_start", "bucket_end"
                                )
                            }} as bucket_duration_hours,
                        {% else %} {{ elementary.null_int() }} as bucket_duration_hours,
                        {% endif %}
                        {% if dimensions | length > 0 %}
                            {{
                                elementary.const_as_string(
                                    elementary.join_list(dimensions, separator="; ")
                                )
                            }} as dimension,
                            {{
                                elementary.list_concat_with_separator(
                                    prefixed_dimensions, separator="; "
                                )
                            }} as dimension_value,
                        {% else %}
                            {{ elementary.null_string() }} as dimension,
                            {{ elementary.null_string() }} as dimension_value,
                        {% endif %}
                        {{
                            elementary.edr_cast_as_float(
                                elementary.escape_reserved_keywords(metric_type)
                            )
                        }} as metric_value,
                        {{
                            elementary.edr_cast_as_string(
                                elementary.edr_quote(metric_name)
                            )
                        }} as metric_name,
                        {{
                            elementary.edr_cast_as_string(
                                elementary.edr_quote(metric_type)
                            )
                        }} as metric_type
                    from column_metrics
                    where
                        {{ elementary.escape_reserved_keywords(metric_type) }}
                        is not null
                    {% if not loop.last %}
                        union all
                    {% endif %}
                {%- endfor %}
            {%- else %}
                {{
                    elementary.empty_table(
                        [
                            ("edr_column_name", "string"),
                            ("bucket_start", "timestamp"),
                            ("bucket_end", "timestamp"),
                            ("bucket_duration_hours", "int"),
                            ("dimension", "string"),
                            ("dimension_value", "string"),
                            ("metric_name", "string"),
                            ("metric_type", "string"),
                            ("metric_value", "float"),
                        ]
                    )
                }}
            {%- endif %}
        ),

        metrics_final as (
            select
                {{ elementary.edr_cast_as_string(full_table_name_str) }}
                as full_table_name,
                edr_column_name as column_name,
                metric_name,
                metric_type,
                {{ elementary.edr_cast_as_float("metric_value") }} as metric_value,
                {{ elementary.null_string() }} as source_value,
                bucket_start,
                bucket_end,
                bucket_duration_hours,
                dimension,
                dimension_value,
                {{ elementary.dict_to_quoted_json(metric_properties) }}
                as metric_properties
            from column_metrics_unpivot
        )

    select
        {{
            elementary.generate_surrogate_key(
                [
                    "full_table_name",
                    "column_name",
                    "metric_name",
                    "metric_type",
                    "dimension",
                    "dimension_value",
                    "bucket_end",
                    "metric_properties",
                ]
            )
        }} as id,
        full_table_name,
        column_name,
        metric_name,
        metric_type,
        metric_value,
        source_value,
        bucket_start,
        bucket_end,
        bucket_duration_hours,
        {{ elementary.edr_current_timestamp_in_utc() }} as updated_at,
        dimension,
        dimension_value,
        metric_properties
    from metrics_final

{% endmacro %}

{% macro column_monitoring_group_by(
    timestamp_column, dimensions, prefixed_dimensions
) %}
    {% if timestamp_column %}
        group by
            edr_bucket_start,
            edr_bucket_end
            {% if dimensions | length > 0 %}
                , {{ elementary.select_dimensions_columns(prefixed_dimensions) }}
            {% endif %}
    {% elif dimensions | length > 0 %}
        group by {{ elementary.select_dimensions_columns(prefixed_dimensions) }}
    {% endif %}
{% endmacro %}

{# Segment-quotes nested dimensions on BigQuery and sanitises the alias suffix.
   Backward compatible for non-nested columns and non-BQ adapters. #}
{% macro select_dimensions_columns(dimension_columns, as_prefix="") %}
    {% set select_statements %}
    {%- for column in dimension_columns -%}
      {%- if as_prefix -%}
        {%- set _is_nested_bq = elementary.bq_is_nested_identifier(column) -%}
        {%- set _source = elementary.bq_segment_quote(column) if _is_nested_bq else column -%}
        {%- set _alias_suffix = elementary.bq_safe_alias(column) if _is_nested_bq else column -%}
        {{ _source }}{{ " as " ~ as_prefix ~ "_" ~ _alias_suffix }}
      {%- else -%}
        {{ column }}
      {%- endif -%}
      {%- if not loop.last -%}{{ ", " }}{%- endif -%}
    {%- endfor -%}
    {% endset %}
    {{ return(select_statements) }}
{% endmacro %}


{# ---------------------------------------------------------------------- #}
{# BigQuery STRUCT nested-field helpers.                                  #}
{# ---------------------------------------------------------------------- #}

{# True only on BigQuery and only when `name` is a plain dotted identifier path
   (e.g. user.address.city) — i.e. an actual nested STRUCT reference. Returns
   false for plain identifiers, SQL expressions (dimensions are documented as
   accepting arbitrary expressions, which must pass through untouched) and
   non-BigQuery adapters. #}
{% macro bq_is_nested_identifier(name) %}
    {%- if target.type != 'bigquery' or name is not string -%}
        {{ return(false) }}
    {%- endif -%}
    {{ return(modules.re.match('^\\w+(\\.\\w+)+$', name) is not none) }}
{% endmacro %}

{# Segment-quote a nested identifier path for BigQuery:
   user.address.city -> `user`.`address`.`city`.
   Anything that is not a nested identifier path (plain identifiers, SQL
   expressions, non-BigQuery adapters) is returned unchanged, preserving
   existing behaviour at all callsites. #}
{% macro bq_segment_quote(name) %}
    {%- if elementary.bq_is_nested_identifier(name) -%}
        {%- set parts = [] -%}
        {%- for seg in name.split('.') -%}
            {%- do parts.append('`' ~ seg ~ '`') -%}
        {%- endfor -%}
        {{ parts | join('.') }}
    {%- else -%}
        {{ name }}
    {%- endif -%}
{% endmacro %}

{# Convert a (possibly dotted) identifier into a dot-free alias safe to use
   as a SQL identifier. No-op for names without dots. #}
{% macro bq_safe_alias(name) %}
    {{- name | replace('.', '__') -}}
{% endmacro %}

{# Wrap a Column / BigQueryColumn with a dict carrying the SQL identifier
   representation (.quoted, segment-quoted for nested), a CTE-projection-safe
   alias (.safe_alias, dot-free) and an .is_nested flag. For non-nested columns
   and non-BigQuery adapters the wrapper mirrors the original Column's values
   (safe_alias falls back to .quoted so identifier quoting is never lost), so
   downstream consumers see no behavioural difference. #}
{% macro wrap_column_for_struct_support(column_obj) %}
    {%- set name = column_obj.name -%}
    {%- set is_nested = elementary.bq_is_nested_identifier(name) -%}
    {%- if is_nested -%}
        {%- set quoted_segments = [] -%}
        {%- for seg in name.split('.') -%}
            {%- do quoted_segments.append('`' ~ seg ~ '`') -%}
        {%- endfor -%}
        {%- set quoted = quoted_segments | join('.') -%}
        {%- set safe_alias = name | replace('.', '__') -%}
    {%- else -%}
        {%- set quoted = column_obj.quoted -%}
        {%- set safe_alias = column_obj.quoted -%}
    {%- endif -%}
    {# `fields` only exists on BigQueryColumn; guard so non-BigQuery
       adapters (Snowflake, Postgres, Redshift, ...) don't trip on the
       attribute access. #}
    {%- set fields = column_obj.fields if column_obj.fields is defined else [] -%}
    {{ return({
        'name': name,
        'column': column_obj.column,
        'quoted': quoted,
        'safe_alias': safe_alias,
        'is_nested': is_nested,
        'dtype': column_obj.dtype,
        'data_type': column_obj.data_type,
        'fields': fields,
    }) }}
{% endmacro %}

{# Walk a BigQuery STRUCT tree and collect dotted leaf names that are safe to
   monitor without UNNEST — i.e. no REPEATED ancestor anywhere in the path,
   and the leaf itself is not REPEATED. `BigQueryColumn.flatten()` returns leaf
   columns with the leaf's own mode but discards ancestor modes, so this walker
   is the source of truth for "which leaves can we project directly?". #}
{% macro bq_safe_leaf_names(column_obj) %}
    {%- set safe_names = [] -%}
    {%- if column_obj.mode != 'REPEATED'
            and column_obj.fields is defined
            and column_obj.fields | length > 0 -%}
        {%- for child in column_obj.fields -%}
            {%- do elementary._bq_walk_collect(
                child, [column_obj.column], false, safe_names
            ) -%}
        {%- endfor -%}
    {%- endif -%}
    {{ return(safe_names) }}
{% endmacro %}

{# Recursive helper: walks a google.cloud.bigquery.SchemaField subtree,
   propagating whether any ancestor was REPEATED. Append safe leaf names to
   `safe_names`. #}
{% macro _bq_walk_collect(field, prefix, has_repeated_ancestor, safe_names) %}
    {%- set new_prefix = prefix + [field.name] -%}
    {%- if field.fields | length == 0 -%}
        {%- if not has_repeated_ancestor and field.mode != 'REPEATED' -%}
            {%- do safe_names.append(new_prefix | join('.')) -%}
        {%- endif -%}
    {%- else -%}
        {%- set new_has_repeated = has_repeated_ancestor or (field.mode == 'REPEATED') -%}
        {%- for child in field.fields -%}
            {%- do elementary._bq_walk_collect(
                child, new_prefix, new_has_repeated, safe_names
            ) -%}
        {%- endfor -%}
    {%- endif -%}
{% endmacro %}
