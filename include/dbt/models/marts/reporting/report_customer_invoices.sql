-- report_customer_invoices.sql
select
    c.country,
    c.iso,
    count(fi.invoice_no) as total_invoices,
    sum(fi.total) as total_revenue
from {{ ref('fct_invoices') }} fi
join {{ ref('dim_customer') }} c on fi.customer_key = c.customer_key
group by c.country, c.iso
order by total_revenue desc
