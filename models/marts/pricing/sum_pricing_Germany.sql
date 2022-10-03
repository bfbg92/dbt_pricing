/* input parameters */
{% set helloprint_models = var('pricing_helloprint_models') %}
{% set competitors = var('pricing_competitors') %}

{% set germany_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Germany' %}
    {% set germany_competitors.value = v %}
{% endfor %}

{{ generate_sum_pricing('Germany', germany_competitors.value) }}