-- report_year_invoices.sql
select
    dt.year,
    dt.month,
    count(distinct fi.invoice_no) as num_invoices,
    sum(fi.total) as total_revenue
from {{ ref('fct_invoices') }} fi
join {{ ref('dim_datetime') }} dt on fi.datetime_id = dt.datetime_id
group by dt.year, dt.month
order by dt.year, dt.month
