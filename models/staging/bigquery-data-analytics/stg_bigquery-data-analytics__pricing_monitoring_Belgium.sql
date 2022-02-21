/* input parameters */
{% set competitors_raw = var('pricing_competitors_raw') %}
{% set belgium_competitors_raw = namespace(value=['']) %}
{% for k, v in competitors_raw.items() if k == 'Belgium' %}
    {% set belgium_competitors_raw.value = v %}
{% endfor %}

{% set competitors = var('pricing_competitors') %}
{% set belgium_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Belgium' %}
    {% set belgium_competitors.value = v %}
{% endfor %}

{% set products = var('pricing_products') %}

{{ generate_stg_pricing_monitoring('Belgium', belgium_competitors_raw.value, belgium_competitors.value, products) }} 
