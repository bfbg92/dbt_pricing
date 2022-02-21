{% macro map_comp(competitor)%}

{% set competitor_dict = var('pricing_competitor_map_dict') %}
<<<<<<< HEAD
{% set result = namespace(value=competitor) %}

 {% for k, v in competitor_dict.items() %}
   {% for i in v if i == competitor %}
     {% set result.value = k %}
   {% endfor %}
 {% endfor %}

{{ return(result.value) }}    
=======
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
    
>>>>>>> efc6d2c682ef4e856033ec7ad226f707a3fee896
{% endmacro %}