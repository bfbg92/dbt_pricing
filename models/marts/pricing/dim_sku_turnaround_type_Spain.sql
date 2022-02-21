/* input parameters */
{% set competitors = var('pricing_competitors') %}

{% set spain_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Spain' %}
    {% set spain_competitors.value = v %}
{% endfor %}

{{ generate_dim_sku_turnaround_type('Spain') }} 
