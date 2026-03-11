{#
  Parses a CI schema name and returns the embedded timestamp as a datetime,
  or none if the name doesn't match the expected CI naming pattern.

  Schema naming convention:
      <prefix><YY><MM><DD>_<HH><MI><SS>_<branch>_<hash>[_<suffix>]
#}
{% macro parse_timestamp_from_ci_schema_name(schema_name, prefixes) %}
    {% set schema_lower = schema_name.lower() %}
    {% for prefix in prefixes %}
        {% if schema_lower.startswith(prefix.lower()) %}
            {% set remainder = schema_lower[prefix | length :] %}
            {% set match = modules.re.match(
                "^(?P<yy>\\d{2})(?P<mm>\\d{2})(?P<dd>\\d{2})_(?P<HH>\\d{2})(?P<MI>\\d{2})(?P<SS>\\d{2})_.+",
                remainder,
            ) %}
            {% if match %}
                {% set yy = match.group("yy") | int %}
                {% set mm = match.group("mm") | int %}
                {% set dd = match.group("dd") | int %}
                {% set HH = match.group("HH") | int %}
                {% set MI = match.group("MI") | int %}
                {% set SS = match.group("SS") | int %}
                {% if 1 <= mm <= 12 and 1 <= dd <= 31 and 0 <= HH <= 23 and 0 <= MI <= 59 and 0 <= SS <= 59 %}
                    {% do return(
                        modules.datetime.datetime(
                            2000 + yy, mm, dd, HH, MI, SS
                        )
                    ) %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {% do return(none) %}
{% endmacro %}
