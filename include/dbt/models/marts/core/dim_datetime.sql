-- dim_datetime.sql
with dates as (
    select distinct invoice_datetime
    from {{ ref('stg_retail__invoices') }}
    where invoice_datetime is not null
)

select
    invoice_datetime            as datetime_id,
    invoice_datetime            as datetime,
    year(invoice_datetime)      as year,
    month(invoice_datetime)     as month,
    day(invoice_datetime)       as day,
    hour(invoice_datetime)      as hour,
    minute(invoice_datetime)    as minute,
    dayofweek(invoice_datetime) as weekday
from dates