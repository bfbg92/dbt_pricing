{% macro map_comp(competitor)%}
{% set competitor_dict = var('pricing_competitor_map_dict') %}
CASE
{% for k, v in competitor_dict.items() -%}
  {% for i in v -%}
  WHEN '{{ i }}' = '{{ competitor }}' THEN '{{ k }}'
  {% endfor -%}
{% endfor -%}
ELSE '{{ competitor }}'
END
{% endmacro %}