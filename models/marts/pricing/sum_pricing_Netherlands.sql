/* input parameters */
{% set helloprint_models = var('pricing_helloprint_models') %}
{% set competitors = var('pricing_competitors') %}

{% set netherlands_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Netherlands' %}
    {% set netherlands_competitors.value = v %}
{% endfor %}

{{ generate_sum_pricing('Netherlands', netherlands_competitors.value) }}