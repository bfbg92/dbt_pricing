/* input parameters */
{% set competitors = var('pricing_competitors') %}

{% set germany_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Germany' %}
    {% set germany_competitors.value = v %}
{% endfor %}

{{ generate_dim_sku_turnaround_type('Germany') }} 
