{% macro ensure_materialize_override() %}
    {% if elementary.get_config_var("mute_ensure_materialization_override") %}
        {% do return(none) %}
    {% endif %}

    {% set runtime_config = elementary.get_runtime_config() %}
    {% if runtime_config.args.require_explicit_package_overrides_for_builtin_materializations is false %}
        {% do elementary.file_log("Materialization override is enabled.") %}
        {% do return(none) %}
    {% endif %}

    {% set major, minor, revision = dbt_version.split(".") %}
    {% set major = major | int %}
    {% set minor = minor | int %}
    {% if major > 1 or major == 1 and minor >= 8 %}
        {%- set msg %}
IMPORTANT - Starting from dbt 1.8, users must explicitly allow packages to override materializations. 
Elementary requires this ability to support collection of samples and failed row count for dbt tests.
Please add the following flag to dbt_project.yml to allow it:

flags:
  require_explicit_package_overrides_for_builtin_materializations: false

Notes - 
* This is a temporary measure that will result in a deprecation warning, please ignore it for now. Elementary is working with the dbt-core team on a more permanent solution.
* This message can be muted by setting the 'mute_ensure_materialization_override' var to true.
        {% endset %}
        {% do log(msg, info=true) %}
    {% endif %}
{% endmacro %}
