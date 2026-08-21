-- fct_invoices.sql
-- Builds the core transaction fact table linking line-item metrics (quantity, revenue)
-- to temporal, product, and customer dimensions.

-- Step 1: Load staging/intermediate invoice line-item records
with lines as (
    select * from {{ ref('int_retail__invoice_lines') }}
),

-- Step 2: Reference dimension tables to fetch surrogate keys
customer_dim as (
    select * from {{ ref('dim_customer') }}
),

product_dim as (
    select * from {{ ref('dim_product') }}
),

datetime_dim as (
    select * from {{ ref('dim_datetime') }}
)

-- Step 3: Join dimensions to associate transaction facts with dimension surrogate keys
select
    lines.invoice_no,
    datetime_dim.datetime_id,
    product_dim.product_key,
    
    -- Anonymous/guest purchases (where customer_id IS NULL in source) map to 
    -- the '-1' sentinel row in dim_customer rather than returning NULL or getting dropped.
    coalesce(customer_dim.customer_key, '-1') as customer_key,
    
    lines.quantity,
    lines.line_total as total
from lines

-- Join 1: Exact timestamp match to retrieve the datetime dimension key
inner join datetime_dim
    on lines.invoice_datetime = datetime_dim.datetime_id

-- Join 2: Multi-column match on stock code, description, and price to capture specific product versions
inner join product_dim
    on lines.stock_code = product_dim.stock_code
    and lines.description = product_dim.description
    and lines.unit_price = product_dim.price

-- Join 3: Left join allows unassigned/guest customer lines to pass through cleanly without dropping revenue
left join customer_dim
    on lines.customer_id = customer_dim.customer_id
    and lines.country = customer_dim.country