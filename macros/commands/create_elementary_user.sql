{% macro create_elementary_user(user="elementary", password=none, role="ELEMENTARY_ROLE") %}
  {% if password is none %}
    {% set password = elementary.generate_password() %}
  {% endif %}
  {% set profile_parameters = elementary.generate_elementary_profile_args(overwrite_values={
    "user": user,
    "password": password,
    "role": role
  }) %}
  {% set profile_parameters_dict = {} %}
  {% for parameter in profile_parameters %}
    {% set profile_parameters_dict = profile_parameters_dict.update({parameter["name"]: parameter["value"]}) %}
  {% endfor %}
  {% set profile_creation_query = elementary.get_profile_creation_query(profile_parameters_dict) %}
  {% do print("\nPlease run the following query in your database to create the Elementary user:\n" ~ profile_creation_query) %}
  {% do print("\nAfter that, use the following parameters when configuring your environment\n") %}
  {% for parameter in profile_parameters %}
    {% if parameter["name"] != "threads" %}  {# threads is not a parameter for the environment #}
      {% do print(parameter["name"]|capitalize ~ ": " ~ parameter["value"]) %}
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
