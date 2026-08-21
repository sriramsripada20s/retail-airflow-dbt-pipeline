-- dim_product.sql
-- stock_code isn't unique on its own — the same code can appear with
-- different descriptions/prices over time, so the grain includes all three.
with products as (
    select distinct
        stock_code,
        description,
        unit_price
    from {{ ref('stg_retail__invoices') }}
    where stock_code is not null
      and unit_price > 0
)

select
    {{ dbt_utils.generate_surrogate_key(['stock_code', 'description', 'unit_price']) }} as product_key,
    stock_code,
    description,
    unit_price as price
from products