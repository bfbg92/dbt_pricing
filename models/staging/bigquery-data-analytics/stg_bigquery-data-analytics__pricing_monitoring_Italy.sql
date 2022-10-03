/* input parameters */
{% set competitors_raw = var('pricing_competitors_raw') %}
{% set italy_competitors_raw = namespace(value=['']) %}
{% for k, v in competitors_raw.items() if k == 'Italy' %}
    {% set italy_competitors_raw.value = v %}
{% endfor %}

{% set competitors = var('pricing_competitors') %}
{% set italy_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Italy' %}
    {% set italy_competitors.value = v %}
{% endfor %}

{% set products = var('pricing_products') %}

{{ generate_stg_pricing_monitoring('Italy', italy_competitors_raw.value, italy_competitors.value, products) }} 
