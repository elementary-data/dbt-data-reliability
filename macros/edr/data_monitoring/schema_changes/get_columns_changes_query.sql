{% macro get_columns_changes_from_last_run_query(
    full_table_name, temp_columns_snapshot_relation
) %}
    {%- set schema_columns_snapshot_relation = elementary.get_elementary_relation(
        "schema_columns_snapshot"
    ) %}
    {%- set previous_schema_time_query -%}
        (select max(detected_at) from {{ schema_columns_snapshot_relation }} where lower(full_table_name) = lower('{{ full_table_name }}'))
    {%- endset %}

    {% set cur %}
        {# This is the current snapshot of the columns. #}
        select full_table_name, column_name, data_type, is_new, detected_at
        from {{ temp_columns_snapshot_relation }}
    {% endset %}

    {% set pre %}
        {# This is the previous snapshot of the columns. #}
        select full_table_name, column_name, data_type, detected_at
        from {{ schema_columns_snapshot_relation }}
        where lower(full_table_name) = lower('{{ full_table_name }}')
            and detected_at = {{ previous_schema_time_query }}
    {% endset %}

    {{ elementary.get_columns_changes_query_generic(full_table_name, cur, pre) }}
{% endmacro %}

{% macro get_column_changes_from_baseline_query(
    model_relation,
    full_table_name,
    model_baseline_relation,
    include_added=False
) %}
    {% set cur %}
        {{
            adapter.dispatch(
                "get_column_changes_from_baseline_cur", "elementary"
            )(
                model_relation,
                full_table_name,
                model_baseline_relation,
            )
        }}
    {% endset %}

    {% set pre %}
        select
            {{ elementary.const_as_string(full_table_name) }} as full_table_name,
            column_name,
            data_type,
            {{ elementary.datetime_now_utc_as_timestamp_column() }} as detected_at
        from {{ model_baseline_relation }}
    {% endset %}

    {{
        elementary.get_columns_changes_query_generic(
            full_table_name, cur, pre, include_added=include_added
        )
    }}
{% endmacro %}


{% macro get_columns_changes_query_generic(
    full_table_name, cur, pre, include_added=True
) %}
    {%- set test_execution_id = elementary.get_test_execution_id() %}
    {%- set test_unique_id = elementary.get_test_unique_id() %}

    with
        cur as ({{ cur }}),

        pre as ({{ pre }}),

        type_changes as (

            {# Finding the columns that have changed type. #}
            select
                cur.full_table_name,
                'type_changed' as change,
                cur.column_name,
                cur.data_type as data_type,
                pre.data_type as pre_data_type,
                pre.detected_at
            from cur
            inner join
                pre
                on (
                    lower(cur.full_table_name) = lower(pre.full_table_name)
                    and lower(cur.column_name) = lower(pre.column_name)
                )
            where
                pre.data_type is not null
                and lower(cur.data_type) != lower(pre.data_type)

        ),

        {% if include_added %}
            columns_added as (

                {# This is the columns that have been added. #}
                select
                    full_table_name,
                    'column_added' as change,
                    column_name,
                    data_type,
                    {{ elementary.null_string() }} as pre_data_type,
                    detected_at as detected_at
                from cur
                where is_new = {{ elementary.edr_boolean_literal(true) }}

            ),
        {% endif %}

        columns_removed as (

            {# This is finding the columns that have been removed. #}
            select
                pre.full_table_name,
                'column_removed' as change,
                pre.column_name as column_name,
                {{ elementary.null_string() }} as data_type,
                pre.data_type as pre_data_type,
                pre.detected_at as detected_at
            from pre
            left join
                cur
                on (
                    lower(cur.full_table_name) = lower(pre.full_table_name)
                    and lower(cur.column_name) = lower(pre.column_name)
                )
            where cur.full_table_name is null and cur.column_name is null

        ),

        columns_removed_filter_deleted_tables as (

            {# This is filtering out the columns of tables that have been deleted. #}
            select
                removed.full_table_name,
                removed.change,
                removed.column_name,
                removed.data_type,
                removed.pre_data_type,
                removed.detected_at
            from columns_removed as removed
            join cur on (lower(removed.full_table_name) = lower(cur.full_table_name))

        ),

        all_column_changes as (

            {# Combining the results of the three queries into one table. #}
            select *
            from type_changes
            union all
            select *
            from columns_removed_filter_deleted_tables
            {% if include_added %}
                union all
                select *
                from columns_added
            {% endif %}
        ),

        column_changes_test_results as (

            {# This is the query that is creating the test results table, by formatting a description and adding id + detection time #}
            select
                {{
                    elementary.generate_surrogate_key(
                        [
                            "full_table_name",
                            "column_name",
                            "change",
                            "detected_at",
                        ]
                    )
                }} as data_issue_id,
                {{ elementary.datetime_now_utc_as_timestamp_column() }} as detected_at,
                {{ elementary.full_name_split("database_name") }},
                {{ elementary.full_name_split("schema_name") }},
                {{ elementary.full_name_split("table_name") }},
                column_name,
                'schema_change' as test_type,
                change as test_sub_type,
                {{ elementary.schema_change_description_column() }}
            from all_column_changes
            {% if elementary.is_tsql() %}
                {#- T-SQL does not support positional GROUP BY references.
                    Group by the 6 source columns from all_column_changes;
                    all 9 output columns are deterministic functions of these. -#}
                group by
                    full_table_name,
                    change,
                    column_name,
                    data_type,
                    pre_data_type,
                    detected_at
                {% else %} {{ dbt_utils.group_by(9) }}
                {% endif %}

        )

    {# Creating a unique id for each row in the table, and adding execution id #}
    select
        {{
            elementary.generate_surrogate_key(
                ["data_issue_id", elementary.const_as_string(test_execution_id)]
            )
        }} as id,
        {{ elementary.const_as_string(test_execution_id) }} as test_execution_id,
        {{ elementary.const_as_string(test_unique_id) }} as test_unique_id,
        *
    from column_changes_test_results

{%- endmacro %}

{% macro default__get_column_changes_from_baseline_cur(
    model_relation, full_table_name, model_baseline_relation
) %}
    with
        baseline as (
            select lower(column_name) as column_name, data_type
            from {{ model_baseline_relation }}
        )

    select
        columns_snapshot.full_table_name,
        lower(columns_snapshot.column_name) as column_name,
        columns_snapshot.data_type,
        (baseline.column_name is null) as is_new,
        {{ elementary.datetime_now_utc_as_timestamp_column() }} as detected_at
    from
        (
            {{ elementary.get_columns_snapshot_query(model_relation, full_table_name) }}
        ) columns_snapshot
    left join
        baseline on (lower(columns_snapshot.column_name) = lower(baseline.column_name))
    where lower(columns_snapshot.full_table_name) = lower('{{ full_table_name }}')
{% endmacro %}

{% macro schema_change_description_column() %}
    case
        when change = 'column_added'
        then
            {{
                elementary.edr_concat(
                    ["'The column \"'", "column_name", "'\" was added'"]
                )
            }}
        when change = 'column_removed'
        then
            {{
                elementary.edr_concat(
                    ["'The column \"'", "column_name", "'\" was removed'"]
                )
            }}
        when change = 'type_changed'
        then
            {{
                elementary.edr_concat(
                    [
                        "'The type of \"'",
                        "column_name",
                        "'\" was changed from '",
                        "pre_data_type",
                        "' to '",
                        "data_type",
                    ]
                )
            }}
        else null
    end as test_results_description
{% endmacro %}

{% macro fabric__get_column_changes_from_baseline_cur(
    model_relation, full_table_name, model_baseline_relation
) %}
    {#- Fabric / T-SQL does not allow CTEs inside subqueries or derived tables.
        get_columns_snapshot_query returns a CTE-based query, so we materialise
        its result into a temp table first, then reference it with a plain SELECT.
        We pass into_relation so the INTO clause is placed inside the CTE's final
        SELECT (the only valid position in T-SQL). -#}
    {% set tmp_snapshot = api.Relation.create(
        database=model_relation.database,
        schema=model_relation.schema,
        identifier=model_relation.identifier ~ "__snap_tmp",
        type="table",
    ) %}
    {% do run_query("drop table if exists " ~ tmp_snapshot) %}
    {% do run_query(
        elementary.get_columns_snapshot_query(
            model_relation, full_table_name, into_relation=tmp_snapshot
        )
    ) %}

    select
        cs.full_table_name,
        lower(cs.column_name) as column_name,
        cs.data_type,
        case
            when bl.column_name is null
            then {{ elementary.edr_boolean_literal(true) }}
            else {{ elementary.edr_boolean_literal(false) }}
        end as is_new,
        {{ elementary.datetime_now_utc_as_timestamp_column() }} as detected_at
    from {{ tmp_snapshot }} cs
    left join
        (
            select lower(column_name) as column_name, data_type
            from {{ model_baseline_relation }}
        ) bl
        on (lower(cs.column_name) = lower(bl.column_name))
    where lower(cs.full_table_name) = lower('{{ full_table_name }}')
{% endmacro %}
