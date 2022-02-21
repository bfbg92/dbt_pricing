/* input parameters */
{% set competitors = var('pricing_competitors') %}

{% set france_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'France' %}
    {% set france_competitors.value = v %}
{% endfor %}

{{ generate_dim_sku_turnaround_type('France') }} 
