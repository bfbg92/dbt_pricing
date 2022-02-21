/* input parameters */
{% set competitors_raw = var('pricing_competitors_raw') %}
{% set spain_competitors_raw = namespace(value=['']) %}
{% for k, v in competitors_raw.items() if k == 'Spain' %}
    {% set spain_competitors_raw.value = v %}
{% endfor %}

{% set competitors = var('pricing_competitors') %}
{% set spain_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Spain' %}
    {% set spain_competitors.value = v %}
{% endfor %}

{% set products = var('pricing_products') %}

{{ generate_stg_pricing_monitoring('Spain', spain_competitors_raw.value, spain_competitors.value, products) }} 
