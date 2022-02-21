{% macro map_case(competitor, dict)%}

CASE
{% for k, v in dict.items() %}
  {%- for i in v -%}
  WHEN '{{ i }}' = {{ competitor }} THEN '{{ k }}'
  {% endfor -%}
{% endfor -%}
ELSE {{ competitor }}
END
{% endmacro %}