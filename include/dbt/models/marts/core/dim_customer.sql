-- dim_customer.sql
with customers as (
    select distinct
        customer_id,
        country
    from {{ ref('stg_retail__invoices') }}
    where customer_id is not null
),

country as (
    select * from {{ ref('stg_retail__country') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customers.customer_id', 'customers.country']) }} as customer_key,
    customers.customer_id,
    customers.country,
    country.iso
from customers
left join country
    on upper(customers.country) = upper(country.country_name)