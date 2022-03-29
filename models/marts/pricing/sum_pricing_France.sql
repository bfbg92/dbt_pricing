/* input parameters */
{% set helloprint_models = var('pricing_helloprint_models') %}
{% set competitors = var('pricing_competitors') %}

{% set france_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'France' %}
    {% set france_competitors.value = v %}
{% endfor %}

{{ generate_sum_pricing('France', france_competitors.value) }}