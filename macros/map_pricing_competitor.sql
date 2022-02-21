{% macro map_comp(competitor)%}

{% set competitor_dict = var('pricing_competitor_map_dict') %}
{% set result = namespace(value=competitor) %}

 {% for k, v in competitor_dict.items() %}
   {% for i in v if i == competitor %}
     {% set result.value = k %}
   {% endfor %}
 {% endfor %}

{{ return(result.value) }}    
{% endmacro %}