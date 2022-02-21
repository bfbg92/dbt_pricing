/* input parameters */
{% set competitors = var('pricing_competitors') %}
{% set products = var('pricing_products') %}

{% set france_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'France' %}
    {% set france_competitors.value = v %}
{% endfor %}

{{ generate_stg_pricing_monitoring('France', france_competitors.value, products) }} 
