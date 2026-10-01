{% macro dump_table(
    model_unique_id,
    output_path,
    exclude_deprecated_columns=true,
    timestamp_column=none,
    since=none,
    days_back=7,
    dedup=false,
    until=none,
    table_filter=none
) %}
    {% set node = graph.nodes.get(model_unique_id) %}
    {% if not node %}
        {% do print("Node '{}' does not exist.".format(model_unique_id)) %}
        {% do return([]) %}
    {% endif %}

    {% set relation = api.Relation.create(
        database=node.database, schema=node.schema, identifier=node.alias
    ) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% set column_names = columns | map(attribute="name") | map("lower") | list %}
    {% if not column_names %}
        {% do print("Relation '{}' does not exist.".format(node.relation_name)) %}
        {% do return([]) %}
    {% endif %}

    {% if exclude_deprecated_columns %}
        {% set deprecated_column_names = (
            node.meta.get("deprecated_columns", [])
            | map(attribute="name")
            | map("lower")
            | list
        ) %}
        {% set column_names = (
            column_names | reject("in", deprecated_column_names) | list
        ) %}
    {% endif %}

    {% if timestamp_column %}
        {# Only string columns (e.g. run results timing) need the nanosecond-tolerant cast.
           Native timestamp columns get a plain cast so BigQuery can still prune partitions. #}
        {% set timestamp_column_is_string = namespace(value=false) %}
        {% for column in columns %}
            {% if column.name | lower == timestamp_column | lower %}
                {% set timestamp_column_is_string.value = (
                    elementary.normalize_data_type(
                        elementary.get_column_data_type(column)
                    )
                    == "string"
                ) %}
            {% endif %}
        {% endfor %}
        {% set timestamp_column_expr = (
            elementary.edr_cast_metadata_timestamp(timestamp_column)
            if timestamp_column_is_string.value
            else elementary.edr_cast_as_timestamp(timestamp_column)
        ) %}
    {% endif %}

    {% set dedup_by_column = node.meta.dedup_by_column or "unique_id" %}
    {% set order_by_dedup_column = "generated_at" %}
    {% set query %}
        {% if dedup and (dedup_by_column in column_names) and (order_by_dedup_column in column_names) %}
            {{ elementary.dedup_by_column_query(dedup_by_column, order_by_dedup_column, column_names, relation, timestamp_column=timestamp_column) }}
        {% else %}
            select {{ elementary.select_columns(column_names, timestamp_column) }}
            from {{ relation }}
        {% endif %}
        {% if timestamp_column %}
            {% if since %}
                where {{ timestamp_column_expr }} > {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(since)) }}
                {% if until %}
                  and {{ timestamp_column_expr }} <= {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(until)) }}
                {% endif %}
            {% else %}
                where {{ elementary.edr_datediff(timestamp_column_expr, elementary.edr_current_timestamp(), 'day') }} < {{ days_back }}
            {% endif %}
        {% endif %}
        {% if table_filter %}
            {% if timestamp_column %}
                and {{ render(table_filter) }}
            {% else %}
                where {{ render(table_filter) }}
            {% endif %}
        {% endif %}
    {% endset %}
    {% set results = elementary.run_query(query) %}
    {% do results.to_csv(output_path) %}
    {% do return(results.column_names) %}
{% endmacro %}


{% macro dedup_by_column_query(
    dedup_by_column,
    order_by_dedup_column,
    column_names,
    relation,
    timestamp_column=none
) %}
    with
        indexed_relation as (
            select
                {{ elementary.escape_select(column_names) }},
                row_number() over (
                    partition by {{ dedup_by_column }}
                    order by {{ order_by_dedup_column }} desc
                ) as row_index
            from {{ relation }}
        ),

        deduped_relation as (
            select {{ elementary.escape_select(column_names) }}
            from indexed_relation
            where row_index = 1
        )

    select {{ elementary.select_columns(column_names, timestamp_column) }}
    from deduped_relation
{% endmacro %}
