-- report_product_invoices.sql
select
    p.product_key,
    p.stock_code,
    p.description,
    sum(fi.quantity) as total_quantity_sold
from {{ ref('fct_invoices') }} fi
join {{ ref('dim_product') }} p on fi.product_key = p.product_key
group by p.product_key, p.stock_code, p.description
order by total_quantity_sold desc
