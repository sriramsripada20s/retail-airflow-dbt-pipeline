
  
    

create or replace transient table RETAIL.TRANSFORM.dim_product
    
    
    
    as (-- dim_product.sql
with products as (
    select distinct
        stock_code,
        description,
        unit_price
    from RETAIL.TRANSFORM.int_retail__invoice_lines
    where stock_code is not null
      and unit_price > 0
)

select
    md5(cast(coalesce(cast(stock_code as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(description as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(unit_price as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as product_key,
    stock_code,
    description,
    unit_price as price
from products
    )
;


  