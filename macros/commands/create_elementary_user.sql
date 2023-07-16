{% macro create_elementary_user(username=none, password=none, role=none) %}
  {% if username is none %}
    {% set username = "elementary" %}
  {% endif %}
  {% if password is none %}
    {% set password = elementary.generate_password() %}
    {% do log("Generated password: " ~ password, info=True) %}
  {% endif %}
  {% set query = elementary.create_user(username, password)|trim ~ "\n" ~ elementary.grant_elementary_access(username, role)|trim %}
  {% do print(query) %}
  {% call statement("create_elementary_user", fetch_result=True) %}
    {{ query }}
  {% endcall %}
  {% do adapter.commit() %}
  {% do print(load_result("create_elementary_user")) %}
{% endmacro %}


{% macro generate_password() %}
  {# Heavily inspired by https://gist.github.com/benwalio/8598cf9f642271ffdaf3daa82c1802cb #}
  {% set ns = namespace(placeholder="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", password="") %}
  {% for x in ns.placeholder %}
    {% set ns.password = [ns.password, (x | replace('x', [0,1,2,3,4,5,6,7,8,9,'a','b','c','d','e','f','A','B','C','D','E','F'] | random ))] | join %}
  {% endfor %}
  {% do return(ns.password) %}
{% endmacro %}
