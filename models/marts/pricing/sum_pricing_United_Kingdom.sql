/* input parameters */
{% set helloprint_models = var('pricing_helloprint_models') %}
{% set competitors = var('pricing_competitors') %}

{% set united_kingdom_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Spain' %}
    {% set united_kingdom_competitors.value = v %}
{% endfor %}

{{ generate_sum_pricing('United_Kingdom', united_kingdom_competitors.value) }}