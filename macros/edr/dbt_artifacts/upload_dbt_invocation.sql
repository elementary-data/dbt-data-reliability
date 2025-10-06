{% macro upload_dbt_invocation() %}
  {% set relation = elementary.get_elementary_relation('dbt_invocations') %}
  {% if not execute or not relation %}
    {{ return('') }}
  {% endif %}

  {% do elementary.file_log("Uploading dbt invocation.") %}
  {% set now_str = elementary.datetime_now_utc_as_string() %}
  {% set orchestrator = elementary.get_orchestrator() %}
  {% set job_id = elementary.get_var("job_id", ["JOB_ID", "DBT_JOB_ID", "DBT_CLOUD_JOB_ID"]) %}
  {% set job_run_id = elementary.get_var("job_run_id", ["JOB_RUN_ID","DBT_JOB_RUN_ID", "DBT_CLOUD_RUN_ID", "GITHUB_RUN_ID"]) %}
  {% set dbt_invocation = {
      'invocation_id': invocation_id,
      'run_started_at': elementary.run_started_at_as_string(),
      'run_completed_at': now_str,
      'generated_at': now_str,
      'command': flags.WHICH,
      'dbt_version': dbt_version,
      'elementary_version': elementary.get_elementary_package_version(),
      'full_refresh': flags.FULL_REFRESH,
      'invocation_vars': elementary.get_invocation_vars(),
      'vars': elementary.get_all_vars(),
      'target_name': target.name,
      'target_database': elementary.target_database(),
      'target_schema': target.schema,
      'target_profile_name': target.profile_name,
      'threads': target.threads,
      'selected': elementary.get_invocation_select_filter(),
      'yaml_selector': elementary.get_invocation_yaml_selector(),
      'project_name': elementary.get_project_name(),
      'job_id': job_id,
      'job_run_id': job_run_id,
      'job_name': elementary.get_var("job_name", ["JOB_NAME", "DBT_JOB_NAME"]),
      'env': elementary.get_first_env_var(["DBT_ENV"]),
      'env_id': elementary.get_first_env_var(["DBT_ENV_ID"]),
      'project_id': elementary.get_first_env_var(["DBT_PROJECT_ID", "DBT_CLOUD_PROJECT_ID", "GITHUB_REPOSITORY"]),
      'cause_category': elementary.get_first_env_var(["DBT_CAUSE_CATEGORY", "DBT_CLOUD_RUN_REASON_CATEGORY", "GITHUB_EVENT_NAME"]),
      'cause': elementary.get_first_env_var(["DBT_CAUSE", "DBT_CLOUD_RUN_REASON"]),
      'pull_request_id': elementary.get_first_env_var(["DBT_PULL_REQUEST_ID", "DBT_CLOUD_PR_ID", "GITHUB_HEAD_REF"]),
      'git_sha': elementary.get_first_env_var(["DBT_GIT_SHA", "DBT_CLOUD_GIT_SHA", "GITHUB_SHA"]),
      'orchestrator': orchestrator,
      'dbt_user': elementary.get_first_env_var(["DBT_USER"]),
      'job_url': elementary.get_job_url(orchestrator, job_id),
      'job_run_url': elementary.get_job_run_url(orchestrator, job_id, job_run_id),
      'account_id': elementary.get_var("account_id", ["DBT_ACCOUNT_ID"]),
      'target_adapter_specific_fields': elementary.get_target_adapter_specific_fields()
  } %}
  {% do elementary.insert_rows(relation, [dbt_invocation], should_commit=true, chunk_size=elementary.get_config_var('dbt_artifacts_chunk_size')) %}
  {% do elementary.file_log("Uploaded dbt invocation successfully.") %}
{% endmacro %}


{% macro get_target_adapter_specific_fields() %}
    {{ return(adapter.dispatch('get_target_adapter_specific_fields', 'elementary')()) }}
{% endmacro %}

{% macro default__get_target_adapter_specific_fields() %}
    {{ return('') }}
{% endmacro %}

{% macro bigquery__get_target_adapter_specific_fields() %}
    {{ return({"execution_project": target.execution_project}) }}
{% endmacro %}

{% macro databricks__get_target_adapter_specific_fields() %}
    {{ return({"http_path": target.http_path}) }}
{% endmacro %}

{% macro snowflake__get_target_adapter_specific_fields() %}
    {{ return({"warehouse": target.warehouse, "user": target.user, "role": target.role}) }}
{% endmacro %}

{% macro postgres__get_target_adapter_specific_fields() %}
    {{ return({"user": target.user}) }}
{% endmacro %}


{% macro get_project_name() %}
    {% set project_name = elementary.get_config_var("project_name") %}
    {% if project_name %}
        {{ return(project_name) }}
    {% endif %}

    {% set config = elementary.get_runtime_config() %}
    {% do return(config.project_name) %}
{% endmacro %}

{%- macro get_invocation_select_filter() -%}
    {% set config = elementary.get_runtime_config() %}
    {%- if invocation_args_dict -%}
        {%- if invocation_args_dict.select -%}
            {{- return(invocation_args_dict.select) -}}
        {%- elif invocation_args_dict.SELECT -%}
            {{- return(invocation_args_dict.SELECT) -}}
        {%- endif -%}
    {%- elif config.args and config.args.select -%}
        {{- return(config.args.select) -}}
    {%- else -%}
        {{- return([]) -}}
    {%- endif -%}
{%- endmacro -%}

{% macro get_invocation_yaml_selector() %}
    {% set config = elementary.get_runtime_config() %}
    {% if invocation_args_dict and invocation_args_dict.selector %}
        {% do return(invocation_args_dict.selector) %}
    {% elif invocation_args_dict and invocation_args_dict.selector_name %}
        {% do return(invocation_args_dict.selector_name) %}
    {% elif invocation_args_dict and invocation_args_dict.INVOCATION_COMMAND %}
        {% set match = modules.re.search(
            "--selector(?:\s+|=)(\S+)",
            invocation_args_dict.INVOCATION_COMMAND
        ) %}
        {% if match %}
            {% do return(match.group(1)) %}
        {% else %}
            {% do return(none) %}
        {% endif %}
    {% elif config.args and config.args.selector_name %}
        {% do return(config.args.selector_name) %}
    {% else %}
        {% do return(none) %}
    {% endif %}
{% endmacro %}

{% macro get_invocation_vars() %}
    {% set config = elementary.get_runtime_config() %}
    {% set invocation_vars = {} %}
    {% if invocation_args_dict and invocation_args_dict.vars %}
        {% if invocation_args_dict.vars is mapping %}
            {% set invocation_vars = invocation_args_dict.vars %}
        {% else %}
            {% set invocation_vars = fromyaml(invocation_args_dict.vars) %}
        {% endif %}
    {% elif invocation_args_dict and invocation_args_dict.VARS %}
        {% set invocation_vars = invocation_args_dict.VARS %}
    {% elif config.cli_vars %}
        {% set invocation_vars = config.cli_vars %}
    {% endif %}
    {{ return(elementary.to_primitive(invocation_vars)) }}
{% endmacro %}

{%- macro get_all_vars() -%}
    {% set all_vars = {} %}
    {% set config = elementary.get_runtime_config() %}
    {%- if config.vars -%}
        {% do all_vars.update(elementary.dbt_object_to_dict(config.vars)) %}
    {%- endif -%}
    {% do all_vars.update(elementary.get_invocation_vars()) %}
    {{- return(all_vars) -}}
{%- endmacro -%}

{% macro get_orchestrator() %}
  {% set var_value = elementary.get_var("orchestrator", ["ORCHESTRATOR", "DBT_ORCHESTRATOR"])%}
  {% if var_value %}
    {% do return(var_value) %}
  {% endif %}
  {% set orchestrator_env_map = {
    "airflow": ["AIRFLOW_HOME"],
    "dbt_cloud": ["DBT_CLOUD_PROJECT_ID"],
    "github_actions": ["GITHUB_ACTIONS"],
  } %}
  {% for orchestrator, env_vars in orchestrator_env_map.items() %}
    {% if elementary.get_first_env_var(env_vars) %}
      {% do return(orchestrator) %}
    {% endif %}
  {% endfor %}
  {% do return(none) %}
{% endmacro %}

{% macro get_job_url(orchestrator, job_id) %}
  {% set var_value = elementary.get_var("job_url", ["JOB_URL", "DBT_JOB_URL"]) %}
  {% if var_value %}
    {% do return(var_value) %}
  {% endif %}
  {% if orchestrator == 'airflow' %}
    {% set server_url = elementary.get_var('airflow_url', ["AIRFLOW_URL"]) %}
    {% set airflow_job_url = server_url ~ "/dags/" ~ job_id ~ "/grid" %}
    {% do return(airflow_job_url) %}
  {% elif orchestrator == 'dbt_cloud' %}
    {% set account_id = elementary.get_var('account_id', ['DBT_CLOUD_ACCOUNT_ID']) %}
    {% set dbt_cloud_project_id = elementary.get_first_env_var(['DBT_CLOUD_PROJECT_ID']) %}
    {% set dbt_cloud_job_id = elementary.get_first_env_var(['DBT_CLOUD_JOB_ID']) %}
    {% set dbt_cloud_domain = elementary.get_first_env_var(['DBT_CLOUD_DOMAIN']) or "cloud.getdbt.com" %}

    {% set dbt_cloud_job_url = "https://" ~ dbt_cloud_domain ~ "/deploy/" ~ account_id ~ "/projects/" ~ dbt_cloud_project_id ~ "/jobs/" ~ dbt_cloud_job_id %}
    {% do return(dbt_cloud_job_url) %}
  {% elif orchestrator == 'github_actions' %}
    {% set server_url = elementary.get_first_env_var(["GITHUB_SERVER_URL"]) %}
    {% set repository = elementary.get_first_env_var(["GITHUB_REPOSITORY"]) %}
    {% set run_id = elementary.get_first_env_var(["GITHUB_RUN_ID"]) %}

    {% set github_job_url = server_url ~ "/" ~ repository ~ "/actions/runs/" ~ run_id %}
    {% do return(github_job_url) %}
  {% endif %}
  {% do return(none) %}
{% endmacro %}

{% macro get_job_run_url(orchestrator, job_id, job_run_id) %}
  {% set var_value = elementary.get_var("job_run_url", ["JOB_RUN_URL", "DBT_JOB_RUN_URL"]) %}
  {% if var_value %}
    {% do return(var_value) %}
  {% endif %}
  {% if orchestrator == 'airflow' %}
    {% set server_url = elementary.get_var('airflow_url', ["AIRFLOW_URL"]) %}
    {% set airflow_job_url = server_url ~ "/dags/" ~ job_id ~ "/grid?dag_run_id=" ~ job_run_id %}
    {% do return(airflow_job_url) %}
  {% elif orchestrator == 'dbt_cloud' %}
    {% set account_id = elementary.get_var('account_id', ['DBT_CLOUD_ACCOUNT_ID']) %}
    {% set dbt_cloud_project_id = elementary.get_first_env_var(['DBT_CLOUD_PROJECT_ID']) %}
    {% set dbt_cloud_run_id = elementary.get_first_env_var(['DBT_CLOUD_RUN_ID']) %}
    {% set dbt_cloud_domain = elementary.get_first_env_var(['DBT_CLOUD_DOMAIN']) or "cloud.getdbt.com" %}

    {% set dbt_cloud_job_url = "https://" ~ dbt_cloud_domain ~ "/deploy/" ~ account_id ~ "/projects/" ~ dbt_cloud_project_id ~ "/runs/" ~ dbt_cloud_run_id %}
    {% do return(dbt_cloud_job_url) %}
  {% elif orchestrator == 'github_actions' %}
    {% set server_url = elementary.get_first_env_var(["GITHUB_SERVER_URL"]) %}
    {% set repository = elementary.get_first_env_var(["GITHUB_REPOSITORY"]) %}
    {% set run_id = elementary.get_first_env_var(["GITHUB_RUN_ID"]) %}

    {% set github_job_url = server_url ~ "/" ~ repository ~ "/actions/runs/" ~ run_id %}
    {% do return(github_job_url) %}
  {% endif %}
  {% do return(none) %}
{% endmacro %}

{% macro get_dbt_invocations_empty_table_query() %}
    {{ return(elementary.empty_table([
      ('invocation_id', 'long_string'),
      ('job_id', 'long_string'),
      ('job_name', 'long_string'),
      ('job_run_id', 'long_string'),
      ('run_started_at', 'string'),
      ('run_completed_at', 'string'),
      ('generated_at', 'string'),
      ('created_at', 'timestamp'),
      ('command', 'string'),
      ('dbt_version', 'string'),
      ('elementary_version', 'string'),
      ('full_refresh', 'boolean'),
      ('invocation_vars', 'long_string'),
      ('vars', 'long_string'),
      ('target_name', 'string'),
      ('target_database', 'string'),
      ('target_schema', 'string'),
      ('target_profile_name', 'string'),
      ('threads', 'int'),
      ('selected', 'long_string'),
      ('yaml_selector', 'long_string'),
      ('project_id', 'string'),
      ('project_name', 'string'),
      ('env', 'string'),
      ('env_id', 'string'),
      ('cause_category', 'string'),
      ('cause', 'long_string'),
      ('pull_request_id', 'string'),
      ('git_sha', 'string'),
      ('orchestrator', 'string'),
      ('dbt_user', 'string'),
      ('job_url', 'string'),
      ('job_run_url', 'string'),
      ('account_id', 'string'),
      ('target_adapter_specific_fields', 'long_string')
    ])) }}
{% endmacro %}
