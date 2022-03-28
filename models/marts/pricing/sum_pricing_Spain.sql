/* input parameters */
{% set helloprint_models = var('pricing_helloprint_models') %}
{% set competitors = var('pricing_competitors') %}

{% set spain_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Spain' %}
    {% set spain_competitors.value = v %}
{% endfor %}

{{ generate_sum_pricing('Spain', spain_competitors.value) }}