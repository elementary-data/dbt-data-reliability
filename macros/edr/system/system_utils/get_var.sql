{% macro get_var(config_var_name, env_vars_names) %}
  {% do return(elementary.get_config_var(config_var_name) or elementary.get_first_env_var(env_vars_names)) %}
{% endmacro %}
