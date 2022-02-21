/* input parameters */
{% set competitors_raw = var('pricing_competitors_raw') %}
{% set netherlands_competitors_raw = namespace(value=['']) %}
{% for k, v in competitors_raw.items() if k == 'Netherlands' %}
    {% set netherlands_competitors_raw.value = v %}
{% endfor %}

{% set competitors = var('pricing_competitors') %}
{% set netherlands_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Netherlands' %}
    {% set netherlands_competitors.value = v %}
{% endfor %}

{% set products = var('pricing_products') %}

{{ generate_stg_pricing_monitoring('Netherlands', netherlands_competitors_raw.value, netherlands_competitors.value, products) }} 
