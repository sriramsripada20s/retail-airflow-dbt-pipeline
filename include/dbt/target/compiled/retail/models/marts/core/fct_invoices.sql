-- fct_invoices.sql
with lines as (
    select * from RETAIL.TRANSFORM.int_retail__invoice_lines
),

customer_dim as (
    select * from RETAIL.TRANSFORM.dim_customer
),

product_dim as (
    select * from RETAIL.TRANSFORM.dim_product
),

datetime_dim as (
    select * from RETAIL.TRANSFORM.dim_datetime
)

select
    lines.invoice_no,
    datetime_dim.datetime_id,
    product_dim.product_key,
    customer_dim.customer_key,
    lines.quantity,
    lines.line_total as total
from lines
inner join datetime_dim
    on lines.invoice_datetime = datetime_dim.datetime_id
inner join product_dim
    on lines.stock_code = product_dim.stock_code
    and lines.description = product_dim.description
    and lines.unit_price = product_dim.price
inner join customer_dim
    on lines.customer_id = customer_dim.customer_id
    and lines.country = customer_dim.country