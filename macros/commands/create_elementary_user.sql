{% macro create_elementary_user(user="elementary", password=none, role="ELEMENTARY_ROLE") %}
  {% if password is none %}
    {% set password = elementary.generate_password() %}
  {% endif %}
  {% set profile_parameters = elementary.generate_elementary_profile_parameters(overwrite_values={
    "user": user,
    "password": password,
    "role": role
  }) %}
  {% set profile_parameters_dict = {} %}
  {% for parameter in profile_parameters %}
    {% set name, value = parameter[0], parameter[1] %}
    {% set profile_parameters_dict = profile_parameters_dict.update({name: value}) %}
  {% endfor %}
  {% set profile_creation_query = elementary.get_profile_creation_query(profile_parameters_dict) %}
  {% do print("\nPlease run the following query in your database to create the Elementary user:\n" ~ profile_creation_query) %}
  {% do print("\nAfter that, use the following parameters when configuring your environment\n") %}
  {% for parameter in profile_parameters %}
    {% set name, value = parameter[0], parameter[1] %}
    {% if name != "threads" %}  {# threads is not a parameter for the environment #}
      {% do print(name|capitalize ~ ": " ~ value) %}
    {% endif %}
  {% endfor %}
{% endmacro %}


{% macro generate_password() %}
  {# Heavily inspired by https://gist.github.com/benwalio/8598cf9f642271ffdaf3daa82c1802cb #}
  {% set ns = namespace(placeholder="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", password="") %}
  {% for x in ns.placeholder %}
    {% set ns.password = [ns.password, (x | replace('x', [0,1,2,3,4,5,6,7,8,9,'a','b','c','d','e','f','A','B','C','D','E','F'] | random ))] | join %}
  {% endfor %}
  {% do return(ns.password) %}
{% endmacro %}
