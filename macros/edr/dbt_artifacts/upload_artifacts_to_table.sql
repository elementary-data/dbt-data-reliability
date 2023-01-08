{% macro upload_artifacts_to_table(table_relation, artifacts, flatten_artifact_callback, append=False, should_commit=False) %}
    {% set flatten_artifact_dicts = [] %}
    {% for artifact in artifacts %}
        {% set flatten_artifact_dict = flatten_artifact_callback(artifact) %}
        {% if flatten_artifact_dict is not none and elementary.dict_has_nonempty_values(flatten_artifact_dict) %}
            {% do flatten_artifact_dicts.append(flatten_artifact_dict) %}
        {% endif %}
    {% endfor %}

    {% if append %}
        {# In append mode, just insert, and no need to be atomic #}
        {% do elementary.insert_rows(table_relation, flatten_artifact_dicts, should_commit, elementary.get_config_var('dbt_artifacts_chunk_size')) %}
    {% else %}
        {% do elementary.overwrite_artifacts_table(table_relation, flatten_artifact_dicts, should_commit) %}
    {% endif %}
{% endmacro %}

{% macro overwrite_artifacts_table(table_relation, flatten_artifact_dicts, should_commit) %}
    {% do return(adapter.dispatch('overwrite_artifacts_table', 'elementary')(table_relation, flatten_artifact_dicts, should_commit)) %}
{% endmacro %}

{% macro default__overwrite_artifacts_table(table_relation, flatten_artifact_dicts, should_commit) %}
    {# First upload everything to a temp table #}
    {% set temp_table_suffix = modules.datetime.datetime.utcnow().strftime('__tmp_%y%m%d%H%M%S%f') %}
    {% set temp_table_relation = dbt.make_temp_relation(table_relation, temp_table_suffix) %}
    {% do elementary.create_table_like(temp_table_relation, table_relation, temporary=True) %}
    {% do elementary.insert_rows(temp_table_relation, flatten_artifact_dicts, should_commit, elementary.get_config_var('dbt_artifacts_chunk_size')) %}

    {# Now atomically replace the data #}
    {% do elementary.replace_data_with_table_contents(table_relation, temp_table_relation) %}
{% endmacro %}

{% macro spark__overwrite_artifacts_table(table_relation, flatten_artifact_dicts, should_commit) %}
    {# Databricks does not support temp tables well enough, and does not have transactions, so we provide
       a more straightforward (non-atomic) implementation here #}

    {% do dbt.truncate_relation(table_relation) %}
    {% do elementary.insert_rows(table_relation, flatten_artifact_dicts, should_commit, elementary.get_config_var('dbt_artifacts_chunk_size')) %}
{% endmacro %}
