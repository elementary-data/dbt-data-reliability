{# table monitors #}

{% test freshness_anomaly(model) %}
    {{ elementary.anomaly_test('freshness') }}
{% endtest %}

{% test row_count_anomaly(model) %}
    {{ elementary.anomaly_test('row_count') }}
{% endtest %}


{# column any type monitors #}

{% test null_count_anomaly(model) %}
    {{ elementary.anomaly_test('null_count') }}
{% endtest %}

{% test null_percent_anomaly(model) %}
    {{ elementary.anomaly_test('null_percent') }}
{% endtest %}


{# column string monitors #}

{% test string_min_length_anomaly(model) %}
    {{ elementary.anomaly_test('min_length') }}
{% endtest %}

{% test string_max_length_anomaly(model) %}
    {{ elementary.anomaly_test('max_length') }}
{% endtest %}

{% test string_average_length_anomaly(model) %}
    {{ elementary.anomaly_test('average_length') }}
{% endtest %}

{% test string_missing_count_anomaly(model) %}
    {{ elementary.anomaly_test('missing_count') }}
{% endtest %}

{% test string_missing_percent_anomaly(model) %}
    {{ elementary.anomaly_test('missing_percent') }}
{% endtest %}


{# column numeric monitors #}

{% test numeric_max_anomaly(model) %}
    {{ elementary.anomaly_test('max') }}
{% endtest %}

{% test numeric_min_anomaly(model) %}
    {{ elementary.anomaly_test('min') }}
{% endtest %}

{% test numeric_average_anomaly(model) %}
    {{ elementary.anomaly_test('average') }}
{% endtest %}

{% test numeric_zero_count_anomaly(model) %}
    {{ elementary.anomaly_test('zero_count') }}
{% endtest %}

{% test numeric_zero_percent_anomaly(model) %}
    {{ elementary.anomaly_test('zero_percent') }}
{% endtest %}

{% test numeric_standard_deviation_anomaly(model) %}
    {{ elementary.anomaly_test('standard_deviation') }}
{% endtest %}

{% test numeric_variance_anomaly(model) %}
    {{ elementary.anomaly_test('variance') }}
{% endtest %}