{% macro generate_stg_order_items_2month(country) %}

  SELECT
    sku_product_identifier,
    sku_material,
    sku_size,
    sku_printing_option,
    sku_finishing,
    total_revenue,
    order_items
  FROM {{ source('bigquery-data-analytics_report', 'order_items_2month') }}
  WHERE country  = '{{ country }}'

{% endmacro %}
