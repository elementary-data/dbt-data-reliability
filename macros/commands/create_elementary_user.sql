{% macro create_elementary_user(user="elementary", password=none, role="ELEMENTARY_ROLE", public_key=none) %}
  {% set auth_method = elementary.get_auth_method() %}
  {% set parameter_values = {
    "user": user,
    "role": role
  } %}

  {% if auth_method == "password" %}
    {% if password is none %}
      {% do parameter_values.update({"password": elementary.generate_password()}) %}
    {% else %}
      {% do parameter_values.update({"password": password}) %}
    {% endif %}
  {% elif auth_method == "keypair" %}
    {%- if public_key is none or public_key == "" -%}
      {%- do exceptions.raise_compiler_error("ERROR: A public key must be provided to generate a Snowflake user!") -%}
    {%- endif -%}
    {% do parameter_values.update({"public_key": public_key}) %}
  {% endif %}

  {# Unify the parameters above with auto-generated profile parameters, to get everything
     we need to create the user. #}
  {% set profile_parameters = elementary.generate_elementary_profile_args(overwrite_values=parameter_values) %}  
  {% for parameter in profile_parameters %}
    {% do parameter_values.update({parameter["name"]: parameter["value"]}) %}
  {% endfor %}

  {% set profile_creation_query = elementary.get_user_creation_query(parameter_values) %}
  {% do print("\nPlease run the following query in your database to create the Elementary user:\n" ~ profile_creation_query) %}
  {% do print("\nAfter that, use the following parameters when configuring your environment\n") %}
  {% for parameter in profile_parameters %}
    {% if parameter["name"] not in ["threads", "public_key"] %}
      {% do print(parameter["name"]|capitalize|replace('_', ' ') ~ ": " ~ parameter["value"]) %}
    {% endif %}
  {% endfor %}
{% endmacro %}


{% macro generate_password() %}
  {# Heavily inspired by https://gist.github.com/benwalio/8598cf9f642271ffdaf3daa82c1802cb #}
  {% set characters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'] %}
  {% set ns = namespace(placeholder="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", password="") %}
  {% for x in ns.placeholder %}
    {% set ns.password = [ns.password, (x | replace('x', characters | random ))] | join %}
  {% endfor %}
  {% do return(ns.password) %}
{% endmacro %}


{% macro get_auth_method() %}
  {% do return(adapter.dispatch("get_auth_method", "elementary")()) %}
{% endmacro %}

{% macro default__get_auth_method() %}
  {% do return("password") %}
{% endmacro %}

{% macro snowflake__get_auth_method() %}
  {% do return("keypair") %}
{% endmacro %}
