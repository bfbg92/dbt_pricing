/* input parameters */
{% set competitors_raw = var('pricing_competitors_raw') %}
{% set france_competitors_raw = namespace(value=['']) %}
{% for k, v in competitors_raw.items() if k == 'France' %}
    {% set france_competitors_raw.value = v %}
{% endfor %}

{% set competitors = var('pricing_competitors') %}
{% set france_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'France' %}
    {% set france_competitors.value = v %}
{% endfor %}

{% set products = var('pricing_products') %}

{{ generate_stg_pricing_monitoring('France', france_competitors_raw.value, france_competitors.value, products) }} 
