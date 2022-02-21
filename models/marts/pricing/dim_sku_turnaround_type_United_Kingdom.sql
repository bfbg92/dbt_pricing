/* input parameters */
{% set competitors = var('pricing_competitors') %}

{% set united_kingdom_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'United_Kingdom' %}
    {% set united_kingdom_competitors.value = v %}
{% endfor %}

{{ generate_dim_sku_turnaround_type('United_Kingdom') }} 
