/* input parameters */
{% set competitors = var('pricing_competitors') %}

{% set italy_competitors = namespace(value=['']) %}
{% for k, v in competitors.items() if k == 'Italy' %}
    {% set italy_competitors.value = v %}
{% endfor %}

{{ generate_dim_sku_turnaround_type('Italy') }} 
