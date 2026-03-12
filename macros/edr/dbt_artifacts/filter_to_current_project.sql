{%- macro filter_to_current_project_if_needed(entities) -%}
    {%- if elementary.get_config_var("upload_only_current_project_artifacts") -%}
        {% set project_name = elementary.get_project_name() %}
        {% do return(
            entities | selectattr("package_name", "==", project_name) | list
        ) %}
    {%- else -%} {% do return(entities | list) %}
    {%- endif -%}
{%- endmacro -%}
