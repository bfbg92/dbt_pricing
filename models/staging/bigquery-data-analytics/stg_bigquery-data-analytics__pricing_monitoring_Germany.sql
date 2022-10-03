/* input parameters */
{% set competitors_raw = var('pricing_competitors_raw') %}
{% set germany_competitors_raw = namespace(value=['']) %}
{% for k, v in competitors_raw.items() if k == 'Germany' %}
    {% set germany_competitors_raw.value = v %}
{% endfor %}

{% set competitors = var('pricing_competitors') %}
{% set germany_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Germany' %}
    {% set germany_competitors.value = v %}
{% endfor %}

{% set products = var('pricing_products') %}

{{ generate_stg_pricing_monitoring('Germany', germany_competitors_raw.value, germany_competitors.value, products) }} 
