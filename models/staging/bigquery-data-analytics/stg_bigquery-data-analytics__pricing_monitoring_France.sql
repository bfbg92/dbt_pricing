/* input parameters */
{% set competitors = var('pricing_competitors') %}
{% set france_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'France' %}
    {% set france_competitors.value = v %}
{% endfor %}

{% set pricing_competitors_mapped = var('pricing_competitors_mapped') %}
{% set france_competitors_mapped = namespace(value=['']) %}
{% for k, v in pricing_competitors_mapped.items() if k == 'France' %}
    {% set france_competitors_mapped.value = v %}
{% endfor %}

{% set products = var('pricing_products') %}

{{ generate_stg_pricing_monitoring('France', france_competitors.value, france_competitors_mapped.value, products) }} 
