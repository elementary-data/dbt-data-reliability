{% macro get_alerts_config_dict(node_dict) %}
    {%- if node_dict.get('resource_type') == 'test' %}
        {%- set test_node = node_dict %}
        {%- set parent_model_unique_id = elementary.get_primary_test_model_unique_id_from_test_node(test_node) %}
        {%- if parent_model_unique_id %}
            {%- set model_node = elementary.get_node(parent_model_unique_id) %}
        {%- endif %}
    {%- else %}
        {%- set model_node = node_dict %}
    {%- endif %}

    {%- set alerts_config_backcomp = elementary.get_alerts_config_backcomp(model_node, test_node) %}

    {%- set alert_suppression_interval = elementary.get_config_argument(argument_name='alert_suppression_interval', value=none, model_node=model_node, test_node=test_node) %}
    {%- set alert_channel = elementary.get_config_argument(argument_name='alert_channel', value=none, model_node=model_node, test_node=test_node) %}
    {%- set alert_fields = elementary.get_config_argument(argument_name='alert_fields', value=none, model_node=model_node, test_node=test_node) %}
    {%- set slack_group_alerts_by = elementary.get_config_argument(argument_name='slack_group_alerts_by', value=none, model_node=model_node, test_node=test_node) %}
    {%- set subscribers = elementary.get_alert_subscribers(model_node, test_node, alerts_config_backcomp) %}

    {%- if alert_suppression_interval or alert_channel or alert_fields or slack_group_alerts_by or subscribers %}
        {% set alerts_config =
          {'alert_suppression_interval': alert_suppression_interval,
           'channel': alert_channel,
           'alert_fields': alert_fields,
           'slack_group_alerts_by': slack_group_alerts_by,
           'subscribers': subscribers
            } %}
        {%- set alerts_config = elementary.empty_dict_keys_to_none(alerts_config) -%}
        {%- set alerts_config = elementary.merge_dict2_to_dict1(alerts_config, alerts_config_backcomp) %}
        {{ return(alerts_config) }}
    {%- elif alerts_config_backcomp %}
        {{ return(alerts_config_backcomp) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}



{% macro get_alert_subscribers(model_node_dict, test_node_dict, alerts_config_backcomp) %}
    {%- if alerts_config_backcomp %}
        {%- set backcomp_subscribers = elementary.safe_get_with_default(alerts_config_backcomp, 'subscribers', []) %}
    {%- endif %}
    {%- set model_subscribers = elementary.get_config_argument('subscribers', value=none, model_node=model_node_dict, test_node=none) %}
    {%- set test_subscribers = elementary.get_config_argument('subscribers', value=none, model_node=none, test_node=test_node_dict) %}
    {%- set subscribers = elementary.union_lists(model_subscribers, backcomp_subscribers) %}
    {%- set subscribers = elementary.union_lists(subscribers, test_subscribers) %}
    {%- if subscribers | length > 0 %}
        {{ return(subscribers) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}

{% macro get_alerts_config_backcomp(model_node, test_node) %}
    {%- set model_alerts_config = elementary.get_config_argument('alerts_config', value=none, model_node=model_node, test_node=none) %}
    {%- set test_alerts_config = elementary.get_config_argument('alerts_config', value=none, model_node=none, test_node=test_node) %}

    {%- if model_alerts_config or test_alerts_config %}
        {%- set alerts_config = {} %}
        {%- if model_alerts_config %}
            {%- do alerts_config.update(model_alerts_config) -%}
            {%- set model_subscribers = elementary.safe_get_with_default(model_alerts_config, 'subscribers', []) %}
        {%- endif %}
        {%- if test_alerts_config %}
            {%- do alerts_config.update(test_alerts_config) -%}
            {%- set test_subscribers = elementary.safe_get_with_default(test_alerts_config, 'subscribers', []) %}
        {%- endif %}
        {# We want to union subscribers, not override #}
        {%- set subscribers = elementary.union_lists(model_subscribers, test_subscribers) %}
        {%- if subscribers %}
            {%- do alerts_config.update({'subscribers': subscribers}) -%}
        {%- endif %}
        {{ return(alerts_config) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}