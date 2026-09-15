{% macro get_elementary_cache() %}
    {# The on-run-start seed lives on `graph`, but Manifest.build_flat_graph() rebinds
       flat_graph to a fresh dict that has no "elementary" key. On the classic path the
       rebuild happens before on-run-start so the seed survives; the v2/Fusion parser
       rebuilds afterwards, dropping it and leaving every accessor below returning none.
       Re-seed on access so chained call sites cannot crash. #}
    {% do return(
        graph.setdefault("elementary", elementary.elementary_graph_defaults())
    ) %}
{% endmacro %}

{% macro set_cache(entry, val) %}
    {% do elementary.get_elementary_cache().update({entry: val}) %}
{% endmacro %}

{% macro get_cache(entry, default=none) %}
    {% do return(elementary.get_elementary_cache().get(entry, default)) %}
{% endmacro %}

{% macro setdefault_cache(entry, default=none) %}
    {% do return(elementary.get_elementary_cache().setdefault(entry, default)) %}
{% endmacro %}
