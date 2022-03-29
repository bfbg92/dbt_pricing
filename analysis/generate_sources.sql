-- src_bigquery-data-analytics.yml

-- bigquery-data-analytics / silver_raw / pricing_monitoring
{{ codegen.generate_source(
    database_name='helloprint-data-analytics-live',
    schema_name='silver_raw',
    table_pattern='pricing_monitoring',
    generate_columns='True',
    include_descriptions='True'
) }}

-- bigquery-data-analytics / report / order_items_2month
{{ codegen.generate_source(
    database_name='helloprint-data-analytics-live',
    schema_name='report',
    table_pattern='order_items_2month',
    generate_columns='True',
    include_descriptions='True'
) }}
