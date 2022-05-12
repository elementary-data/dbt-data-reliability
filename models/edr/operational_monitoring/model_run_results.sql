with dbt_run_results as (
    {% set dbt_run_results = source('elementary_dbt_artifacts', 'dbt_run_results') %}
    {% if elementary.relation_exists(dbt_run_results) %}
        select * from {{ dbt_run_results }}
    {% else %}
        {{ elementary.get_dbt_run_results_empty_table_query() }}
    {% endif %}
),

models_metadata as (
    {% set dbt_models = source('elementary_dbt_artifacts', 'dbt_models') %}
    {% if elementary.relation_exists(dbt_models) %}
        select * from {{ dbt_models }}
    {% else %}
        {{ elementary.get_dbt_models_empty_table_query() }}
    {% endif %}
),

model_run_results as (
    select *
    from dbt_run_results where resource_type = 'model'
),

model_run_results_with_metadata as (
    select mr.*,
           alias,
           checksum,
           materialization,
           tags,
           meta,
           owner,
           database_name,
           schema_name,
           depends_on_macros,
           depends_on_nodes,
           description,
           package_name
    from model_run_results mr left join models_metadata mm on mr.unique_id = mm.unique_id
)

select * from model_run_results_with_metadata
