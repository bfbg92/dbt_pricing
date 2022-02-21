/* input parameters */
{% set competitors = var('pricing_competitors') %}

{% set belgium_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Belgium' %}
    {% set belgium_competitors.value = v %}
{% endfor %}

{{ generate_dim_sku_turnaround_type('Belgium') }} 
