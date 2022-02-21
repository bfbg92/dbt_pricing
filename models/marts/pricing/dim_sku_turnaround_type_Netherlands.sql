/* input parameters */
{% set competitors = var('pricing_competitors') %}

{% set netherlands_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Netherlands' %}
    {% set netherlands_competitors.value = v %}
{% endfor %}

{{ generate_dim_sku_turnaround_type('Netherlands') }} 
