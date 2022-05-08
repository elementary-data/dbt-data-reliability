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
    select dr.*,
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
    from dbt_run_results dr join models_metadata mm on dr.unique_id = mm.unique_id
    where dr.generated_at >= {{ elementary.const_as_string((run_started_at - modules.datetime.timedelta(elementary.get_config_var('dbt_monitoring_days_back'))).strftime("%Y-%m-%d 00:00:00")) }}
        and dr.resource_type = 'model'
)

select * from model_run_results
