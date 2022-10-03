/* input parameters */
{% set helloprint_models = var('pricing_helloprint_models') %}
{% set competitors = var('pricing_competitors') %}

{% set italy_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Italy' %}
    {% set italy_competitors.value = v %}
{% endfor %}

{{ generate_sum_pricing('Italy', italy_competitors.value) }}