{% macro map_pricing_competitor(competitor)%}

{% set competitor_dict = var('pricing_competitor_map_dict') %}
{% set result = competitor %}

{% for k, v in competitor_dict.items() %}
  {% for i in v %}
    {% if i == competitor %}
    {% set result = k %}
    {% endif %}
  {% endfor %}
{% endfor %}

{{ log("result: " ~ result, True) }}
{{ return(result) }}
    
{% endmacro %}