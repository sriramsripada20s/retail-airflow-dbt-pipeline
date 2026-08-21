-- dim_customer.sql
with customers as (
    select distinct
        customer_id,
        country
    from RETAIL.TRANSFORM.int_retail__invoice_lines
    where customer_id is not null
),

country as (
    select * from RETAIL.TRANSFORM.stg_retail__country
)

select
    md5(cast(coalesce(cast(customers.customer_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(customers.country as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as customer_key,
    customers.customer_id,
    customers.country,
    country.iso
from customers
left join country
    on upper(customers.country) = upper(country.country_name)