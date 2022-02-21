/* input parameters */
{% set competitors_raw = var('pricing_competitors_raw') %}
{% set united_kingdom_competitors_raw = namespace(value=['']) %}
{% for k, v in competitors_raw.items() if k == 'United_Kingdom' %}
    {% set united_kingdom_competitors_raw.value = v %}
{% endfor %}

{% set competitors = var('pricing_competitors') %}
{% set united_kingdom_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'United_Kingdom' %}
    {% set united_kingdom_competitors.value = v %}
{% endfor %}

{% set products = var('pricing_products') %}

{{ generate_stg_pricing_monitoring('United_Kingdom', united_kingdom_competitors_raw.value, united_kingdom_competitors.value, products) }} 
