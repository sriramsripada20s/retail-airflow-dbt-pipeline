-- dim_product.sql
with products as (
    select distinct
        stock_code,
        description,
        unit_price
    from {{ ref('int_retail__invoice_lines') }}
    where stock_code is not null
      and unit_price > 0
)

select
    {{ dbt_utils.generate_surrogate_key(['stock_code', 'description', 'unit_price']) }} as product_key,
    stock_code,
    description,
    unit_price as price
from products