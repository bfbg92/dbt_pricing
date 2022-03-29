-- stg_bigquery-data-analytics.yml

-- staging / bigquery-data-analytics
{{ codegen.generate_model_yaml(model_name='stg_bigquery-data-analytics__pricing_monitoring_Belgium') }}
{{ codegen.generate_model_yaml(model_name='stg_bigquery-data-analytics__pricing_monitoring_France') }}
{{ codegen.generate_model_yaml(model_name='stg_bigquery-data-analytics__pricing_monitoring_Netherlands') }}
{{ codegen.generate_model_yaml(model_name='stg_bigquery-data-analytics__pricing_monitoring_Spain') }}
{{ codegen.generate_model_yaml(model_name='stg_bigquery-data-analytics__pricing_monitoring_United_Kingdom') }}

{{ codegen.generate_model_yaml(model_name='stg_bigquery-data-analytics__order_items_2month_Belgium') }}
{{ codegen.generate_model_yaml(model_name='stg_bigquery-data-analytics__order_items_2month_France') }}
{{ codegen.generate_model_yaml(model_name='stg_bigquery-data-analytics__order_items_2month_Netherlands') }}
{{ codegen.generate_model_yaml(model_name='stg_bigquery-data-analytics__order_items_2month_Spain') }}
{{ codegen.generate_model_yaml(model_name='stg_bigquery-data-analytics__order_items_2month_United_Kingdom') }}

-- pricing.yml

-- marts / pricing / dim tables
{{ codegen.generate_model_yaml(model_name='dim_sku_turnaround_type_Belgium') }}
{{ codegen.generate_model_yaml(model_name='dim_sku_turnaround_type_France') }}
{{ codegen.generate_model_yaml(model_name='dim_sku_turnaround_type_Netherlands') }}
{{ codegen.generate_model_yaml(model_name='dim_sku_turnaround_type_Spain') }}
{{ codegen.generate_model_yaml(model_name='dim_sku_turnaround_type_United_Kingdom') }}

-- marts / pricing / fct tables
{{ codegen.generate_model_yaml(model_name='fct_pricing_Belgium') }}
{{ codegen.generate_model_yaml(model_name='fct_pricing_France') }}
{{ codegen.generate_model_yaml(model_name='fct_pricing_Netherlands') }}
{{ codegen.generate_model_yaml(model_name='fct_pricing_Spain') }}
{{ codegen.generate_model_yaml(model_name='fct_pricing_United_Kingdom') }}

-- marts / pricing / summary tables
{{ codegen.generate_model_yaml(model_name='sum_pricing_Belgium') }}
{{ codegen.generate_model_yaml(model_name='sum_pricing_France') }}
{{ codegen.generate_model_yaml(model_name='sum_pricing_Netherlands') }}
{{ codegen.generate_model_yaml(model_name='sum_pricing_Spain') }}
{{ codegen.generate_model_yaml(model_name='sum_pricing_United_Kingdom') }}